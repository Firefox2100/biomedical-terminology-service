import asyncio
import json
import os
import sys
import time
import types

import pytest

os.environ.setdefault('BTS_SERVER_HMAC_KEY', 'test-hmac-key')

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
        def delay():
            calls.append('delay')

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
    assert calls == ['delay']
    assert 'lock:cache_rebuild' in redis.values
