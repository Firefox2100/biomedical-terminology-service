import traceback
from typing import Annotated, Optional
from rich.table import Table
import typer

from bioterms.etc.enums import ConceptPrefix
from bioterms.database import get_active_cache
from .utils import CONSOLE, run_async


app = typer.Typer(help='Manage data cache.')


@app.command(name='purge', help='Purge all cached data.')
@run_async
async def purge_cache():
    try:
        cache = get_active_cache()
        await cache.purge()
        CONSOLE.print('[green]Successfully purged all cached data.[/green]')
    except Exception as e:
        CONSOLE.print(f'[red]Failed to purge cached data: {e}[/red]')
        traceback.print_exc()
