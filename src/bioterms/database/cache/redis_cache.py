import redis.asyncio as redis
from pydantic import ValidationError

from bioterms.etc.enums import ConceptPrefix
from bioterms.model.vocabulary_status import VocabularyStatus
from bioterms.model.annotation_status import AnnotationStatus
from bioterms.model.similarity_status import SimilarityStatus
from .cache import Cache


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

    async def save_vocabulary_status(self,
                                     status: VocabularyStatus,
                                     ttl: int = 3600,
                                     ):
        """
        Store the vocabulary status in the cache.
        :param status: The vocabulary status to store.
        :param ttl: Time to live in seconds. Defaults to 3600 seconds (1 hour). If set to 0,
            the status will be stored indefinitely, and must be manually invalidated.
        """
        key = f'vocab_status:{status.prefix.value}'
        value = status.model_dump_json()

        if ttl > 0:
            await self.db.setex(key, ttl, value)
        else:
            await self.db.set(key, value)

    async def get_vocabulary_status(self,
                                    prefix: ConceptPrefix,
                                    ) -> VocabularyStatus | None:
        """
        Retrieve the vocabulary status from the cache.
        :param prefix: The prefix of the vocabulary to retrieve.
        :return: The vocabulary status if it exists and is not expired, otherwise None
        """
        key = f'vocab_status:{prefix.value}'
        value = await self.db.get(key)

        if value is not None:
            try:
                return VocabularyStatus.model_validate_json(value)
            except ValidationError:
                await self.db.delete(key)

        return None

    async def save_annotation_status(self,
                                     status: AnnotationStatus,
                                     ):
        """
        Store the annotation status in the cache.
        :param status: The annotation status to store.
        """
        key = f'anno_status:{status.prefix_source.value}:{status.prefix_target.value}'

        value = status.model_dump_json()

        await self.db.set(key, value)

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
        value = await self.db.get(key)

        if value is not None:
            try:
                return AnnotationStatus.model_validate_json(value)
            except ValidationError:
                await self.db.delete(key)

        return None

    async def save_similarity_status(self,
                                     status: SimilarityStatus,
                                     ):
        """
        Store the similarity status in the cache.
        :param status: The similarity status to store.
        """
        key = f'sim_status:{status.prefix.value}'
        value = status.model_dump_json()

        await self.db.set(key, value)

    async def get_similarity_status(self,
                                    prefix: ConceptPrefix,
                                    ) -> SimilarityStatus | None:
        """
        Retrieve the similarity status from the cache.
        :param prefix: The prefix of the vocabulary to retrieve.
        :return: The similarity status if it exists, otherwise None
        """
        key = f'sim_status:{prefix.value}'
        value = await self.db.get(key)

        if value is not None:
            try:
                return SimilarityStatus.model_validate_json(value)
            except ValidationError:
                await self.db.delete(key)

        return None

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

        if ttl > 0:
            await self.db.setex(key, ttl, site_map_str)
        else:
            await self.db.set(key, site_map_str)

    async def get_site_map(self) -> str | None:
        """
        Retrieve the site map string from the cache.
        :return: The site map string if it exists and is not expired, otherwise None
        """
        key = 'assets:site_map'
        value = await self.db.get(key)

        if value is not None:
            return value

        return None

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
