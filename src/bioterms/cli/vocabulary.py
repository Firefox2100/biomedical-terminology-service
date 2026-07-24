import traceback
from typing import Annotated, Optional
from rich.table import Table
import typer

from bioterms.etc.enums import ConceptPrefix
from bioterms.vocabulary import download_vocabulary, load_vocabulary, delete_vocabulary, embed_vocabulary, \
    restore_vocabulary_embeddings, get_vocabulary_status
from .utils import CONSOLE, run_async


app = typer.Typer(help='Manage biomedical vocabularies.')


@app.command(name='download', help='Download a given vocabulary.')
@run_async
async def download_command(vocabulary: Annotated[
                               Optional[ConceptPrefix],
                               typer.Argument(help='The vocabulary to download.')
                           ] = None,
                           download_all: Annotated[
                               bool,
                               typer.Option(
                                   '--all',
                                   '-a',
                                   help='Download all available vocabularies.'
                               )
                           ] = False,
                           redownload: Annotated[
                               bool,
                               typer.Option(
                                   '--redownload',
                                   '-r',
                                   help='Redownload the vocabulary even if it exists.')
                           ] = False
                           ):
    try:
        if download_all:
            target_vocabularies = list(ConceptPrefix)
        elif vocabulary:
            target_vocabularies = [vocabulary]
        else:
            CONSOLE.print('[red]Either specify a vocabulary to download or use the --all flag.[/red]')
            return
        for vocabulary in target_vocabularies:
            await download_vocabulary(vocabulary, redownload)
            CONSOLE.print(f'[green]Successfully downloaded vocabulary {vocabulary.value}.[/green]')
    except Exception as e:
        CONSOLE.print(f'[red]Failed to download vocabulary {vocabulary.value}: {e}[/red]')
        traceback.print_exc()


@app.command(name='load', help='Load a vocabulary into database.')
@run_async
async def load_command(vocabulary: Annotated[
                           Optional[ConceptPrefix],
                           typer.Argument(help='The vocabulary to load.')
                       ] = None,
                       load_all: Annotated[
                           bool,
                           typer.Option(
                                '--all',
                                '-a',
                                help='Load all available vocabularies.'
                           )
                       ] = False,
                       overwrite: Annotated[
                           bool,
                           typer.Option(
                              '--overwrite',
                              '-o',
                              help='Overwrite existing data in the database.')
                       ] = False,
                       offline: Annotated[
                           bool,
                           typer.Option(
                               '--offline',
                               '-f',
                               help='Load vocabulary in offline mode without writing to database. '
                                    'This mode allows compiling the database structure on a separate '
                                    'machine.')
                       ] = False
                       ):
    try:
        if load_all:
            target_vocabularies = list(ConceptPrefix)
        elif vocabulary:
            target_vocabularies = [vocabulary]
        else:
            CONSOLE.print('[red]Either specify a vocabulary to load or use the --all flag.[/red]')
            return
        for vocabulary in target_vocabularies:
            await load_vocabulary(vocabulary, drop_existing=overwrite, offline=offline)
            CONSOLE.print(f'[green]Successfully loaded vocabulary {vocabulary.value}.[/green]')
    except Exception as e:
        CONSOLE.print(f'[red]Failed to load vocabulary {vocabulary.value}: {e}[/red]')
        traceback.print_exc()


@app.command(name='embed', help='Embed a vocabulary into vector database.')
@run_async
async def embed_command(v: Annotated[
                            Optional[ConceptPrefix],
                            typer.Argument(help='The vocabulary to embed.')
                        ] = None,
                        embed_all: Annotated[
                            bool,
                            typer.Option(
                                '--all',
                                '-a',
                                help='Embed all available vocabularies.'
                            )
                        ] = False,
                        overwrite: Annotated[
                            bool,
                            typer.Option(
                                '--overwrite',
                                '-o',
                                help='Overwrite existing embeddings in the vector database.')
                        ] = False,
                        offline: Annotated[
                            bool,
                            typer.Option(
                                '--offline',
                                '-f',
                                help='Embed vocabulary in offline mode without writing to vector database. '
                                     'Requires offline file from the loading process first.')
                        ] = False,
                        restore: Annotated[
                            bool,
                            typer.Option(
                                '--restore',
                                '-r',
                                help='Restore embeddings from offline files created during embedding '
                                     'process on another machine.')
                        ] = False,
                        ):
    if offline and restore:
        CONSOLE.print('[red]Cannot use --offline and --restore flags together.[/red]')
        return

    try:
        if embed_all:
            target_vocabularies = list(ConceptPrefix)
        elif v:
            target_vocabularies = [v]
        else:
            CONSOLE.print('[red]Either specify a vocabulary to embed or use the --all flag.[/red]')
            return
        for v in target_vocabularies:
            if restore:
                await restore_vocabulary_embeddings(v, drop_existing=overwrite)
                CONSOLE.print(
                    f'[green]Successfully restored embeddings for vocabulary {v.value} '
                    f'into the vector database.[/green]'
                )
            else:
                await embed_vocabulary(v, drop_existing=overwrite, offline=offline)
                CONSOLE.print(
                    f'[green]Successfully embedded vocabulary {v.value}.[/green]'
                )
    except Exception as e:
        CONSOLE.print(f'[red]Failed to embed vocabulary {v.value} into the vector database: {e}[/red]')
        traceback.print_exc()


@app.command(name='delete', help='Delete a vocabulary from database.')
@run_async
async def delete_command(vocabulary: Annotated[
                             Optional[ConceptPrefix],
                             typer.Argument(help='The vocabulary to delete.')
                         ] = None,
                         delete_all: Annotated[
                             bool,
                             typer.Option(
                                 '--all',
                                 '-a',
                                 help='Delete all available vocabularies.'
                             )
                         ] = False
                         ):
    try:
        if delete_all:
            target_vocabularies = list(ConceptPrefix)
        elif vocabulary:
            target_vocabularies = [vocabulary]
        else:
            CONSOLE.print('[red]Either specify a vocabulary to delete or use the --all flag.[/red]')
            return
        for vocabulary in target_vocabularies:
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
