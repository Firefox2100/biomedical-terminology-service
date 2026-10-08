import asyncio
import functools
import inspect
import logging
import os
import time
from rich.logging import RichHandler

from bioterms.etc.consts import LOGGER
from bioterms.etc.console import CONSOLE
from bioterms.etc.utils import report_exception, verbose_print


def configure_cli_output() -> None:
    """Coordinate application logs and third-party output with Rich progress bars."""
    formatter = logging.Formatter(
        fmt='[%(asctime)s] [%(process)d] [%(levelname)s]: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S %z',
    )
    handler = RichHandler(
        console=CONSOLE,
        show_time=False,
        show_level=False,
        show_path=False,
        markup=False,
    )
    handler.setLevel(LOGGER.level)
    handler.setFormatter(formatter)
    for existing_handler in LOGGER.handlers[:]:
        if isinstance(existing_handler, logging.StreamHandler) \
                and not isinstance(existing_handler, logging.FileHandler):
            LOGGER.removeHandler(existing_handler)
    LOGGER.addHandler(handler)
    LOGGER.propagate = False

    # Third-party warnings (for example Neo4j server notifications) use the root
    # logger. Route those through Rich too, while retaining any configured file sinks.
    root_logger = logging.getLogger()
    for existing_handler in root_logger.handlers[:]:
        if isinstance(existing_handler, logging.StreamHandler) \
                and not isinstance(existing_handler, logging.FileHandler):
            root_logger.removeHandler(existing_handler)
    third_party_handler = RichHandler(
        console=CONSOLE,
        show_time=False,
        show_level=False,
        show_path=False,
        markup=False,
    )
    third_party_handler.setLevel(logging.WARNING)
    third_party_handler.setFormatter(formatter)
    root_logger.addHandler(third_party_handler)

    # The CLI already reports operation-level progress. Nested tqdm displays from
    # model loaders otherwise fight with Rich's live display.
    os.environ['HF_HUB_DISABLE_PROGRESS_BARS'] = '1'
    try:
        from huggingface_hub.utils import disable_progress_bars  # pylint: disable=import-outside-toplevel
        disable_progress_bars()
    except ImportError:
        pass
    try:
        from transformers.utils.logging import disable_progress_bar  # pylint: disable=import-outside-toplevel
        disable_progress_bar()
    except ImportError:
        pass


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
            LOGGER.debug('CLI command started: %s', func.__name__)
            try:
                return asyncio.run(func(*args, **kwargs))
            except Exception as exc:
                LOGGER.exception('CLI command failed: %s', func.__name__)
                report_exception(exc)
                raise
            finally:
                LOGGER.debug(
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
