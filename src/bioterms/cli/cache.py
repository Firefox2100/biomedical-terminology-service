import typer

from .utils import CONSOLE, observe_cli_exception, run_async, verbose_cli


app = typer.Typer(help='Manage data cache.')


@app.command(name='purge', help='Purge all cached data.')
@run_async
async def purge_cache():
    try:
        from bioterms.database import get_active_cache

        cache = get_active_cache()
        verbose_cli(f'purging all entries from {type(cache).__name__}')
        await cache.purge()
        CONSOLE.print('[green]Successfully purged all cached data.[/green]')
    except Exception as e:
        observe_cli_exception('purge cache', e)
        CONSOLE.print(f'[red]Failed to purge cached data: {e}[/red]')


@app.command(name='rebuild', help='Rebuild the cache.')
@run_async
async def rebuild_cache_command():
    try:
        from bioterms.app import rebuild_cache

        verbose_cli('rebuilding configured application cache')
        await rebuild_cache()
        CONSOLE.print('[green]Successfully rebuilt the cache.[/green]')
    except Exception as e:
        observe_cli_exception('rebuild cache', e)
        CONSOLE.print(f'[red]Failed to rebuild the cache: {e}[/red]')
