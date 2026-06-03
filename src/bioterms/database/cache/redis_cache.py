import asyncio
import json
import time
from datetime import datetime, timezone
from typing import TypeVar
import redis.asyncio as redis
from pydantic import BaseModel
from pydantic import ValidationError

from bioterms.etc.consts import CONFIG, LOGGER
from bioterms.etc.enums import ConceptPrefix
from bioterms.model.vocabulary_status import VocabularyStatus
from bioterms.model.annotation_status import AnnotationStatus
from bioterms.model.similarity_status import SimilarityStatus
from .cache import Cache


CacheModel = TypeVar('CacheModel', bound=BaseModel)
CACHE_PAYLOAD_VERSION = 1
REFRESH_LOCK_KEY = 'lock:cache_rebuild'


class RedisCache(Cache):
    """
    A Redis-based cache implementation.
    """

    _db: redis.Redis | None = None

    def __init__(self,
                 client: redis.Redis | None = None,
                 ):
        """
        Initialise the RedisCache with an optional Redis client.
        :param client: An instance of redis.Redis to override the class variable.
        """
        if client is not None:
            self._db = client

    @property
    def db(self) -> redis.Redis:
        """
        Get the Redis client instance.
        :return: An instance of redis.Redis.
        """
        if self._db is None:
            raise ValueError('Redis client has not been set.')

        return self._db

    @classmethod
    def set_client(cls,
                   client: redis.Redis,
                   ):
        """
        Set the Redis client for the RedisCache class.
        :param client: An instance of redis.Redis to be used by the class.
        """
        cls._db = client

    @staticmethod
    def _pack_cache_value(value: str,
                          ttl: int,
                          ) -> str:
        """
        Wrap a cached value with an application-level stale timestamp.
        """
        return json.dumps({
            'version': CACHE_PAYLOAD_VERSION,
            'stale_at': time.time() + ttl if ttl > 0 else None,
            'value': value,
        })

    @staticmethod
    def _hard_ttl(ttl: int) -> int | None:
        """
        Convert the soft TTL into the Redis hard-expiration TTL.
        """
        if ttl <= 0:
            return None

        return max(ttl, ttl * CONFIG.cache_hard_ttl_multiplier)

    async def _save_stale_while_revalidate(self,
                                           key: str,
                                           value: str,
                                           ttl: int,
                                           ):
        """
        Save a value with soft expiration in the payload and hard expiration in Redis.
        """
        packed_value = self._pack_cache_value(value, ttl)
        hard_ttl = self._hard_ttl(ttl)

        if hard_ttl is not None:
            await self.db.setex(key, hard_ttl, packed_value)
        else:
            await self.db.set(key, packed_value)

    async def _load_stale_while_revalidate(self,
                                           key: str,
                                           ) -> tuple[str | None, bool]:
        """
        Load a cached value and report whether it has passed its soft expiration.
        """
        raw_value = await self.db.get(key)

        if raw_value is None:
            return None, False

        try:
            payload = json.loads(raw_value)
        except json.JSONDecodeError:
            return raw_value, True

        if not isinstance(payload, dict) or payload.get('version') != CACHE_PAYLOAD_VERSION:
            return raw_value, True

        value = payload.get('value')
        stale_at = payload.get('stale_at')

        if not isinstance(value, str):
            await self.db.delete(key)
            return None, False

        return value, stale_at is not None and time.time() >= stale_at

    async def _get_model(self,
                         key: str,
                         model_class: type[CacheModel],
                         ) -> CacheModel | None:
        """
        Load and validate a model, scheduling a cache rebuild when the value is stale.
        """
        value, is_stale = await self._load_stale_while_revalidate(key)

        if value is None:
            return None

        try:
            model = model_class.model_validate_json(value)
        except ValidationError:
            await self.db.delete(key)
            return None

        if is_stale:
            await self._trigger_rebuild_if_needed()

        return model

    async def _trigger_rebuild_if_needed(self):
        """
        Trigger a background cache rebuild once across all concurrent stale readers.
        """
        acquired = await self.db.set(
            REFRESH_LOCK_KEY,
            datetime.now(timezone.utc).isoformat(),
            ex=CONFIG.cache_rebuild_lock_ttl,
            nx=True,
        )

        if not acquired:
            return

        async def queue_rebuild():
            try:
                from bioterms.task.cache import rebuild_cache_task

                await asyncio.to_thread(rebuild_cache_task.delay)
                LOGGER.info('Scheduled stale cache rebuild task.')
            except Exception as e:     # pylint: disable=broad-exception-caught
                await self.db.delete(REFRESH_LOCK_KEY)
                LOGGER.error('Failed to schedule stale cache rebuild: %s', str(e), exc_info=True)

        asyncio.create_task(queue_rebuild())

    async def save_vocabulary_status(self,
                                     status: VocabularyStatus,
                                     ttl: int = 86400,
                                     ):
        """
        Store the vocabulary status in the cache.
        :param status: The vocabulary status to store.
        :param ttl: Time to live in seconds. Defaults to 86400 seconds (1 day). If set to 0,
            the status will be stored indefinitely, and must be manually invalidated.
        """
        key = f'vocab_status:{status.prefix.value}'
        value = status.model_dump_json()

        await self._save_stale_while_revalidate(key, value, ttl)

    async def get_vocabulary_status(self,
                                    prefix: ConceptPrefix,
                                    ) -> VocabularyStatus | None:
        """
        Retrieve the vocabulary status from the cache.
        :param prefix: The prefix of the vocabulary to retrieve.
        :return: The vocabulary status if it exists and is not expired, otherwise None
        """
        key = f'vocab_status:{prefix.value}'
        return await self._get_model(key, VocabularyStatus)

    async def save_annotation_status(self,
                                     status: AnnotationStatus,
                                     ttl: int = 86400,
                                     ):
        """
        Store the annotation status in the cache.
        :param status: The annotation status to store.
        """
        key = f'anno_status:{status.prefix_source.value}:{status.prefix_target.value}'

        value = status.model_dump_json()

        await self._save_stale_while_revalidate(key, value, ttl)

    async def get_annotation_status(self,
                                    prefix_1: ConceptPrefix,
                                    prefix_2: ConceptPrefix,
                                    ) -> AnnotationStatus | None:
        """
        Retrieve the annotation status from the cache.
        :param prefix_1: The first prefix of the annotation to retrieve.
        :param prefix_2: The second prefix of the annotation to retrieve.
        :return: The annotation status if it exists, otherwise None
        """
        key = f'anno_status:{prefix_1.value}:{prefix_2.value}'
        return await self._get_model(key, AnnotationStatus)

    async def save_similarity_status(self,
                                     status: SimilarityStatus,
                                     ttl: int = 86400,
                                     ):
        """
        Store the similarity status in the cache.
        :param status: The similarity status to store.
        """
        key = f'sim_status:{status.prefix.value}'
        value = status.model_dump_json()

        await self._save_stale_while_revalidate(key, value, ttl)

    async def get_similarity_status(self,
                                    prefix: ConceptPrefix,
                                    ) -> SimilarityStatus | None:
        """
        Retrieve the similarity status from the cache.
        :param prefix: The prefix of the vocabulary to retrieve.
        :return: The similarity status if it exists, otherwise None
        """
        key = f'sim_status:{prefix.value}'
        return await self._get_model(key, SimilarityStatus)

    async def save_site_map(self,
                            site_map_str: str,
                            ttl: int = 86400,
                            ):
        """
        Store the site map string in the cache.
        :param site_map_str: The site map string to store.
        :param ttl: Time to live in seconds. Defaults to 86400 seconds (1 day). If set to 0,
            the site map will be stored indefinitely, and must be manually invalidated.
        """
        key = 'assets:site_map'

        await self._save_stale_while_revalidate(key, site_map_str, ttl)

    async def get_site_map(self) -> str | None:
        """
        Retrieve the site map string from the cache.
        :return: The site map string if it exists and is not expired, otherwise None
        """
        key = 'assets:site_map'
        value, is_stale = await self._load_stale_while_revalidate(key)

        if value is None:
            return None

        if is_stale:
            await self._trigger_rebuild_if_needed()

        return value

    async def rotate_dataset_version(self):
        """
        Rotate the dataset version in the cache. This is called when database is updated, and controls
        whether the cached results in proxy are still valid or not.
        """
        await self.db.set('version:dataset', datetime.now(timezone.utc).isoformat())

    async def get_dataset_last_modified(self) -> datetime:
        """
        Get the last modified timestamp of the dataset from the cache.
        :return: The last modified timestamp of the dataset.
        """
        key = 'version:dataset'
        value = await self.db.get(key)

        if value is None:
            await self.rotate_dataset_version()

        value = await self.db.get(key)

        return datetime.fromisoformat(value)

    async def purge(self):
        """
        Purge all cached data.
        """
        await self.db.flushdb()

    async def close(self) -> None:
        """
        Close the cache driver connection.
        """
        await self.db.close()
