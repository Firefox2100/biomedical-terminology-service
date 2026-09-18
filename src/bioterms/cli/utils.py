import asyncio
import functools
import inspect
import time
from rich.console import Console

from bioterms.etc.consts import LOGGER
from bioterms.etc.utils import report_exception, verbose_print


CONSOLE = Console()


def verbose_cli(message: str) -> None:
    """Show a CLI execution detail when verbose output is enabled."""
    verbose_print(f'CLI: {message}')


def verbose_targets(action: str, targets) -> None:
    values = [getattr(target, 'value', str(target)) for target in targets]
    verbose_cli(f'{action}: selected {len(values):,} target(s): {", ".join(values)}')


def run_async(func):
    if inspect.iscoroutinefunction(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            started = time.perf_counter()
            LOGGER.info('CLI command started: %s', func.__name__)
            try:
                return asyncio.run(func(*args, **kwargs))
            except Exception as exc:
                LOGGER.exception('CLI command failed: %s', func.__name__)
                report_exception(exc)
                raise
            finally:
                LOGGER.info(
                    'CLI command finished: %s (%.3fs)',
                    func.__name__,
                    time.perf_counter() - started,
                )

        return wrapper

    return func


def observe_cli_exception(action: str, exc: Exception) -> None:
    """Record a handled CLI failure in logs and error reporting."""
    LOGGER.error('CLI operation failed: %s: %s', action, exc, exc_info=True)
    report_exception(exc)
