from typing import Annotated
from rich.table import Table
import typer

from bioterms.etc.enums import ConceptPrefix
from bioterms.vocabulary import download_vocabulary, load_vocabulary
from .utils import CONSOLE, run_async


app = typer.Typer(help='Manage biomedical vocabularies.')


@app.command(name='download', help='Download a given vocabulary.')
@run_async
async def download_command(vocabulary: Annotated[ConceptPrefix, typer.Argument(help='The vocabulary to download.')],
                           redownload: Annotated[
                               bool,
                               typer.Option(
                                   '--redownload',
                                   '-r',
                                   help='Redownload the vocabulary even if it exists.')
                           ] = False
                           ):
    try:
        await download_vocabulary(vocabulary, redownload)
        CONSOLE.print(f'[green]Successfully downloaded vocabulary {vocabulary.value}.[/green]')
    except Exception as e:
        CONSOLE.print(f'[red]Failed to download vocabulary {vocabulary.value}: {e}[/red]')
