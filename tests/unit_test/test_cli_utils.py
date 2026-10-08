import logging

from rich.logging import RichHandler

from bioterms.cli import utils
from bioterms.etc.consts import LOGGER


def test_configure_cli_output_uses_shared_rich_console(monkeypatch):
    original_handlers = list(LOGGER.handlers)
    original_propagate = LOGGER.propagate
    root_logger = logging.getLogger()
    original_root_handlers = list(root_logger.handlers)
    monkeypatch.setattr(utils.os, 'environ', {})
    try:
        utils.configure_cli_output()

        assert len(LOGGER.handlers) == 1
        handler = LOGGER.handlers[0]
        assert isinstance(handler, RichHandler)
        assert handler.console is utils.CONSOLE
        assert utils.os.environ['HF_HUB_DISABLE_PROGRESS_BARS'] == '1'
        assert handler.level == LOGGER.level
        assert isinstance(handler.formatter, logging.Formatter)
        assert LOGGER.propagate is False
        assert any(
            isinstance(root_handler, RichHandler)
            and root_handler.console is utils.CONSOLE
            and root_handler.level == logging.WARNING
            for root_handler in root_logger.handlers
        )
    finally:
        LOGGER.handlers.clear()
        LOGGER.handlers.extend(original_handlers)
        LOGGER.propagate = original_propagate
        root_logger.handlers.clear()
        root_logger.handlers.extend(original_root_handlers)
