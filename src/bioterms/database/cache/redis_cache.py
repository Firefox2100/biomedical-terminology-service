import redis.asyncio as redis

from bioterms.etc.enums import ConceptPrefix
from bioterms.model.vocabulary_status import VocabularyStatus
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
            return VocabularyStatus.model_validate_json(value)

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
