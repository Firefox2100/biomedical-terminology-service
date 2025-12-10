from pytheus.backends import load_backend
from pytheus.backends.redis import MultiProcessRedisBackend

from .consts import CONFIG
from .enums import CacheDriverType

if CONFIG.enable_metrics:
    if CONFIG.cache_driver == CacheDriverType.REDIS:
        # Enable Redis backend for multiprocess support
        load_backend(
            backend_class=MultiProcessRedisBackend,
            backend_config={
                'host': CONFIG.redis_host,
                'port': CONFIG.redis_port,
                'db': 15,
            },
        )
    else:
        # Use the default backend (in-memory)
        load_backend()

