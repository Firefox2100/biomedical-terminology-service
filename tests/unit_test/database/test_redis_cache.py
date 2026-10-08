import asyncio
import json
import sys
import time
import types

import pytest

from bioterms.database.cache.redis_cache import CACHE_PAYLOAD_VERSION, RedisCache
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, SimilarityMethod
from bioterms.model.vocabulary_status import VocabularyStatus


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.expirations = {}
        self.deleted = []

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value, ex=None, nx=False):
        if nx and key in self.values:
            return None

        self.values[key] = value
        if ex is not None:
            self.expirations[key] = ex

        return True

    async def setex(self, key, ttl, value):
        self.values[key] = value
        self.expirations[key] = ttl

    async def delete(self, key):
        self.deleted.append(key)
        self.values.pop(key, None)

    async def flushdb(self):
        self.values.clear()
        self.expirations.clear()


def make_status() -> VocabularyStatus:
    return VocabularyStatus(
        prefix=ConceptPrefix.HPO,
        name='Human Phenotype Ontology',
        fileDownloaded=True,
        loaded=True,
        conceptCount=10,
        relationshipCount=20,
        vectorCount=30,
        annotations=[ConceptPrefix.MONDO],
        similarityMethods=[SimilarityMethod.RELEVANCE],
    )


@pytest.mark.asyncio
async def test_save_uses_payload_soft_ttl_and_redis_hard_ttl():
    redis = FakeRedis()
    cache = RedisCache(redis)
    status = make_status()

    await cache.save_vocabulary_status(status, ttl=10)

    key = 'vocab_status:hpo'
    payload = json.loads(redis.values[key])

    assert payload['version'] == CACHE_PAYLOAD_VERSION
    assert payload['stale_at'] > time.time()
    assert VocabularyStatus.model_validate_json(payload['value']) == status
    assert redis.expirations[key] == 10 * CONFIG.cache_hard_ttl_multiplier


@pytest.mark.asyncio
async def test_stale_value_is_served_and_rebuild_is_single_flight():
    redis = FakeRedis()
    cache = RedisCache(redis)
    status = make_status()
    calls = []

    fake_cache_module = types.ModuleType('bioterms.task.cache')

    class FakeTask:
        @staticmethod
        def delay(rotate_dataset_version):
            calls.append(rotate_dataset_version)

    fake_cache_module.rebuild_cache_task = FakeTask()
    original_cache_module = sys.modules.get('bioterms.task.cache')
    sys.modules['bioterms.task.cache'] = fake_cache_module

    redis.values['vocab_status:hpo'] = json.dumps({
        'version': CACHE_PAYLOAD_VERSION,
        'stale_at': time.time() - 1,
        'value': status.model_dump_json(),
    })

    try:
        first_result = await cache.get_vocabulary_status(ConceptPrefix.HPO)
        second_result = await cache.get_vocabulary_status(ConceptPrefix.HPO)

        for _ in range(20):
            if calls:
                break
            await asyncio.sleep(0.01)
    finally:
        if original_cache_module is None:
            sys.modules.pop('bioterms.task.cache', None)
        else:
            sys.modules['bioterms.task.cache'] = original_cache_module

    assert first_result == status
    assert second_result == status
    assert calls == [False]
    assert 'lock:cache_rebuild' in redis.values


@pytest.mark.asyncio
async def test_purge_preserves_dataset_version():
    redis = FakeRedis()
    cache = RedisCache(redis)
    redis.values.update({
        'version:dataset': '2026-01-02T03:04:05+00:00',
        'vocab_status:hpo': 'cached',
    })

    await cache.purge()

    assert redis.values == {'version:dataset': '2026-01-02T03:04:05+00:00'}


@pytest.mark.asyncio
@pytest.mark.parametrize('raw', ['not json', json.dumps(['legacy', 'list']), json.dumps({'version': -1, 'value': 'x'})])
async def test_unversioned_or_corrupt_payloads_are_returned_as_stale(raw):
    redis = FakeRedis()
    redis.values['assets:site_map'] = raw
    cache = RedisCache(redis)

    assert await cache._load_stale_while_revalidate('assets:site_map') == (raw, True)


@pytest.mark.asyncio
async def test_payload_without_string_value_is_evicted():
    redis = FakeRedis()
    redis.values['vocab_status:hpo'] = json.dumps({'version': CACHE_PAYLOAD_VERSION, 'value': 42})
    cache = RedisCache(redis)

    assert await cache.get_vocabulary_status(ConceptPrefix.HPO) is None
    assert redis.deleted == ['vocab_status:hpo']


@pytest.mark.asyncio
async def test_invalid_cached_model_is_evicted():
    redis = FakeRedis()
    redis.values['vocab_status:hpo'] = json.dumps({
        'version': CACHE_PAYLOAD_VERSION, 'stale_at': None, 'value': '{"prefix": "not-a-prefix"}',
    })
    cache = RedisCache(redis)

    assert await cache.get_vocabulary_status(ConceptPrefix.HPO) is None
    assert 'vocab_status:hpo' in redis.deleted


@pytest.mark.asyncio
async def test_fresh_status_round_trips_without_rebuild(monkeypatch):
    cache = RedisCache(FakeRedis())
    monkeypatch.setattr(cache, '_trigger_rebuild_if_needed', lambda: pytest.fail('fresh value triggered rebuild'))

    await cache.save_vocabulary_status(make_status())

    assert await cache.get_vocabulary_status(ConceptPrefix.HPO) == make_status()
    assert await cache.get_vocabulary_status(ConceptPrefix.MONDO) is None


@pytest.mark.asyncio
async def test_site_map_round_trip_and_stale_rebuild(monkeypatch):
    redis = FakeRedis()
    cache = RedisCache(redis)
    rebuilds = []

    async def rebuild():
        rebuilds.append(True)

    monkeypatch.setattr(cache, '_trigger_rebuild_if_needed', rebuild)

    assert await cache.get_site_map() is None
    await cache.save_site_map('<urlset/>')
    assert await cache.get_site_map() == '<urlset/>'
    assert rebuilds == []

    payload = json.loads(redis.values['assets:site_map'])
    payload['stale_at'] = time.time() - 1
    redis.values['assets:site_map'] = json.dumps(payload)
    assert await cache.get_site_map() == '<urlset/>'
    assert rebuilds == [True]


@pytest.mark.asyncio
async def test_dataset_last_modified_is_initialised_on_first_read():
    redis = FakeRedis()
    cache = RedisCache(redis)

    first = await cache.get_dataset_last_modified()
    again = await cache.get_dataset_last_modified()

    assert first == again
    assert first.tzinfo is not None
