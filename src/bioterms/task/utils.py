import asyncio
import threading
from celery.signals import worker_process_init

from bioterms.database import get_active_cache, get_active_vector_db, get_active_doc_db, get_active_graph_db


_loop = asyncio.new_event_loop()
_loop_started = threading.Event()


async def init_databases():
    """
    Initialise all database connections in a specified eventloop.
    """
    _ = get_active_cache()
    _ = get_active_vector_db()
    _ = await get_active_doc_db()
    _ = get_active_graph_db()


def _loop_thread() -> None:
    asyncio.set_event_loop(_loop)
    _loop_started.set()
    _loop.run_forever()


@worker_process_init.connect
def _init_worker_async_runtime(**_kwargs):
    threading.Thread(target=_loop_thread, daemon=True).start()
    _loop_started.wait()

    # Immediately initialise database connections to ensure they bind to the correct event loop.
    run_async(init_databases())


def run_async(coro):
    """
    Run an asynchronous coroutine in a synchronous context.
    :param coro: The coroutine to run.
    :return: The result of the coroutine.
    """
    fut = asyncio.run_coroutine_threadsafe(coro, _loop)
    return fut.result()
