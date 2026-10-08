"""Shared terminal console used by CLI output and progress displays."""

from rich.console import Console


# Rich can safely print log records above an active progress display only when both
# use the same Console instance.  Keeping this in a dependency-light module avoids
# coupling the general utilities module to the CLI package.
CONSOLE = Console()
