import traceback
from typing import Annotated, Optional
from rich.table import Table
import typer

from bioterms.etc.enums import ConceptPrefix
from bioterms.vocabulary import download_vocabulary, load_vocabulary, delete_vocabulary, embed_vocabulary, \
    get_vocabulary_status
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
        traceback.print_exc()


@app.command(name='load', help='Load a vocabulary into database.')
@run_async
async def load_command(vocabulary: Annotated[ConceptPrefix, typer.Argument(help='The vocabulary to load.')],
                       overwrite: Annotated[
                           bool,
                           typer.Option(
                              '--overwrite',
                              '-o',
                              help='Overwrite existing data in the database.')
                       ] = False
                       ):
    try:
        await load_vocabulary(vocabulary, drop_existing=overwrite)
        CONSOLE.print(f'[green]Successfully loaded vocabulary {vocabulary.value} into the database.[/green]')
    except Exception as e:
        CONSOLE.print(f'[red]Failed to load vocabulary {vocabulary.value} into the database: {e}[/red]')
        traceback.print_exc()


@app.command(name='embed', help='Embed a vocabulary into vector database.')
@run_async
async def embed_command(vocabulary: Annotated[ConceptPrefix, typer.Argument(help='The vocabulary to embed.')],
                        overwrite: Annotated[
                            bool,
                            typer.Option(
                                '--overwrite',
                                '-o',
                                help='Overwrite existing embeddings in the vector database.')
                        ] = False
                        ):
    try:
        await embed_vocabulary(vocabulary, drop_existing=overwrite)
        CONSOLE.print(f'[green]Successfully embedded vocabulary {vocabulary.value} into the vector database.[/green]')
    except Exception as e:
        CONSOLE.print(f'[red]Failed to embed vocabulary {vocabulary.value} into the vector database: {e}[/red]')
        traceback.print_exc()


@app.command(name='delete', help='Delete a vocabulary from database.')
@run_async
async def delete_command(vocabulary: Annotated[ConceptPrefix, typer.Argument(help='The vocabulary to delete.')],
                         ):
    try:
        await delete_vocabulary(vocabulary)
        CONSOLE.print(f'[green]Successfully deleted vocabulary {vocabulary.value} from the database.[/green]')
    except Exception as e:
        CONSOLE.print(f'[red]Failed to delete vocabulary {vocabulary.value} from the database: {e}[/red]')
        traceback.print_exc()


@app.command(name='status', help='Get the status of a vocabulary.')
@run_async
async def status_command(vocabulary: Annotated[
                             Optional[ConceptPrefix],
                             typer.Argument(help='The vocabulary to check. Omit to check all vocabularies.')
                         ] = None):
    if vocabulary:
        vocabularies = [vocabulary]
    else:
        vocabularies = list(ConceptPrefix)

    statuses = []
    for vocab in vocabularies:
        status = await get_vocabulary_status(vocab)
        statuses.append(status)

    table = Table(
        'Prefix',
        'Name',
        'Loaded in DB',
        'Concept Count',
        'Relationship Count',
        'Annotations',
    )
    for status in statuses:
        table.add_row(
            status.prefix.value,
            status.name,
            'Yes' if status.loaded else 'No',
            str(status.concept_count),
            str(status.relationship_count),
            ', '.join([anno.value for anno in status.annotations]),
        )

    CONSOLE.print(table)
