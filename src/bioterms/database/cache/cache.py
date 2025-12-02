from abc import ABC, abstractmethod

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import CacheDriverType, ConceptPrefix
from bioterms.model.vocabulary_status import VocabularyStatus


class Cache(ABC):
    """
    A class for hot data caching and multiprocess state persistence.
    """

    @abstractmethod
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

    @abstractmethod
    async def get_vocabulary_status(self,
                                    prefix: ConceptPrefix,
                                    ) -> VocabularyStatus | None:
        """
        Retrieve the vocabulary status from the cache.
        :param prefix: The prefix of the vocabulary to retrieve.
        :return: The vocabulary status if it exists and is not expired, otherwise None
        """

    @abstractmethod
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

    @abstractmethod
    async def get_site_map(self) -> str | None:
        """
        Retrieve the site map string from the cache.
        :return: The site map string if it exists and is not expired, otherwise None
        """

    @abstractmethod
    async def purge(self):
        """
        Purge all cached data.
        """

    @abstractmethod
    async def close(self) -> None:
        """
        Close the cache driver connection.
        """


_active_cache: Cache | None = None


def get_active_cache() -> Cache:
    """
    Return the active cache set by configuration
    :return: Cache instance based on the configuration.
    """
    global _active_cache

    if _active_cache is not None:
        return _active_cache

    if CONFIG.cache_driver == CacheDriverType.REDIS:
        import redis.asyncio as redis
        from .redis_cache import RedisCache

        redis_client = redis.Redis(
            host=CONFIG.redis_host,
            port=CONFIG.redis_port,
            db=CONFIG.redis_db,
            decode_responses=True,
        )
        RedisCache.set_client(redis_client)
        _active_cache = RedisCache()

        return _active_cache

    raise ValueError(f'Unsupported cache driver: {CONFIG.cache_driver}')
