import traceback
import typer

from bioterms.database import get_active_cache
from bioterms.app import rebuild_cache
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


@app.command(name='rebuild', help='Rebuild the cache.')
@run_async
async def rebuild_cache_command():
    try:
        await rebuild_cache()
        CONSOLE.print('[green]Successfully rebuilt the cache.[/green]')
    except Exception as e:
        CONSOLE.print(f'[red]Failed to rebuild the cache: {e}[/red]')
        traceback.print_exc()
