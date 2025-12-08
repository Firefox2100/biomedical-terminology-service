from bioterms.database import get_active_cache
from .app import celery_app
from .utils import run_async


@celery_app.task(name='cache.purge')
def purge_cache_task() -> None:
    """
    Celery task to purge the cache.
    """
    cache = get_active_cache()
    run_async(cache.purge())


@celery_app.task(name='cache.rebuild')
def rebuild_cache_task() -> None:
    """
    Celery task to rebuild the cache.
    """
    from bioterms.app import rebuild_cache

    run_async(rebuild_cache())
