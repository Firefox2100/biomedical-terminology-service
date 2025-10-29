import asyncio
import functools
import inspect
from rich.console import Console


def run_async(func):
    if inspect.iscoroutinefunction(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return asyncio.run(func(*args, **kwargs))

        return wrapper

    return func


CONSOLE = Console()
