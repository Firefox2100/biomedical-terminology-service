import traceback
from typing import Annotated, Optional
from rich.table import Table
import typer

from bioterms.etc.enums import ConceptPrefix
from bioterms.vocabulary import get_vocabulary_config
from bioterms.annotation import download_annotation, load_annotation, delete_annotation, get_annotation_status, \
    get_annotation_config
from .utils import CONSOLE, run_async


app = typer.Typer(help='Manage biomedical vocabulary annotations.')


def _get_all_annotations():
    annotations = []
    for vocab in ConceptPrefix:
        vocab_status = get_vocabulary_config(vocab)
        for anno in vocab_status['annotations']:
            anno_config = get_annotation_config(vocab, anno)
            annotations.append((anno_config['prefix1'], anno_config['prefix2']))

    annotations = list(set(annotations))
    return annotations


@app.command(name='download', help='Download a given vocabulary annotation.')
@run_async
async def download_command(prefix_1: Annotated[
                               Optional[ConceptPrefix],
                               typer.Argument(
                                   help='The prefix of the first vocabulary.'
                               )
                           ] = None,
                           prefix_2: Annotated[
                               Optional[ConceptPrefix],
                               typer.Argument(
                                   help='The prefix of the second vocabulary.'
                               )
                           ] = None,
                           download_all: Annotated[
                                 bool,
                                 typer.Option(
                                      '--all',
                                      '-a',
                                      help='Download all available vocabulary annotations. '
                                           'Cannot be used with prefix options.'
                                 )
                           ] = False,
                           redownload: Annotated[
                               bool,
                               typer.Option(
                                   '--redownload',
                                   '-r',
                                   help='Redownload the annotation even if it exists.')
                           ] = False,
                           ):
    if download_all:
        if prefix_1 or prefix_2:
            raise typer.BadParameter('Cannot use --all option with specific prefix arguments.')
        annotations = _get_all_annotations()
    else:
        if not (prefix_1 and prefix_2):
            raise typer.BadParameter(
                'Both prefix_1 and prefix_2 must be provided unless --all is used.'
            )
        annotations = [(prefix_1, prefix_2)]
    for prefix_a, prefix_b in annotations:
        try:
            await download_annotation(
                prefix_1=prefix_a,
                prefix_2=prefix_2,
                redownload=redownload,
            )
            CONSOLE.print(
                f'[green]Successfully downloaded annotation between '
                f'{prefix_a.value} and {prefix_b.value}.[/green]'
            )
        except Exception as e:
            CONSOLE.print(
                f'[red]Failed to download annotation between '
                f'{prefix_a.value} and {prefix_b.value}: {e}[/red]'
            )
            traceback.print_exc()


@app.command(name='load', help='Load a vocabulary annotation into database.')
@run_async
async def load_command(prefix_1: Annotated[
                           Optional[ConceptPrefix],
                           typer.Argument(
                               help='The prefix of the first vocabulary.'
                           )
                       ] = None,
                       prefix_2: Annotated[
                           Optional[ConceptPrefix],
                           typer.Argument(
                               help='The prefix of the second vocabulary.'
                           )
                       ] = None,
                       load_all: Annotated[
                           bool,
                           typer.Option(
                               '--all',
                               '-a',
                               help='Load all vocabulary annotations. Cannot be used with prefix options.'
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
                               help='Write annotation output to offline dump file instead of database.',
                           )
                       ] = False,
                       ):
    if load_all:
        if prefix_1 or prefix_2:
            raise typer.BadParameter('Cannot use --all option with specific prefix arguments.')

        annotations = _get_all_annotations()
    else:
        if not (prefix_1 and prefix_2):
            raise typer.BadParameter(
                'Both prefix_1 and prefix_2 must be provided unless --all is used.'
            )
        annotations = [(prefix_1, prefix_2)]

    for prefix_a, prefix_b in annotations:
        try:
            await download_annotation(
                prefix_1=prefix_a,
                prefix_2=prefix_b,
            )

            await load_annotation(
                prefix_1=prefix_a,
                prefix_2=prefix_b,
                overwrite=overwrite,
                offline=offline,
            )
            if offline:
                CONSOLE.print(
                    f'[green]Successfully loaded annotation between '
                    f'{prefix_a.value} and {prefix_b.value} into offline dump file.[/green]'
                )
            else:
                CONSOLE.print(
                    f'[green]Successfully loaded annotation between '
                    f'{prefix_a.value} and {prefix_b.value} into the database.[/green]'
                )
        except Exception as e:
            destination = 'offline dump file' if offline else 'database'
            CONSOLE.print(
                f'[red]Failed to load annotation between '
                f'{prefix_a.value} and {prefix_b.value} into the {destination}: {e}[/red]'
            )
            traceback.print_exc()


@app.command(name='delete', help='Delete a vocabulary annotation from database.')
@run_async
async def delete_command(prefix_1: Annotated[
                             Optional[ConceptPrefix],
                             typer.Argument(
                                 help='The prefix of the first vocabulary.'
                             )
                         ] = None,
                         prefix_2: Annotated[
                             Optional[ConceptPrefix],
                             typer.Argument(
                                 help='The prefix of the second vocabulary.'
                             )
                         ] = None,
                         delete_all: Annotated[
                             bool,
                             typer.Option(
                                 '--all',
                                 '-a',
                                 help='Delete all available vocabulary annotations. '
                                      'Cannot be used with prefix options.'
                             )
                         ] = False,
                         ):
    if delete_all:
        if prefix_1 or prefix_2:
            raise typer.BadParameter('Cannot use --all option with specific prefix arguments.')
        annotations = _get_all_annotations()
    else:
        if not (prefix_1 and prefix_2):
            raise typer.BadParameter(
                'Both prefix_1 and prefix_2 must be provided unless --all is used.'
            )
        annotations = [(prefix_1, prefix_2)]

    for prefix_a, prefix_b in annotations:
        try:
            await delete_annotation(
                prefix_1=prefix_a,
                prefix_2=prefix_b,
            )
            CONSOLE.print(
                f'[green]Successfully deleted annotation between '
                f'{prefix_a.value} and {prefix_b.value} from the database.[/green]'
            )
        except Exception as e:
            CONSOLE.print(
                f'[red]Failed to delete annotation between '
                f'{prefix_a.value} and {prefix_b.value} from the database: {e}[/red]'
            )
            traceback.print_exc()


@app.command(name='status', help='Get the status of a vocabulary annotation.')
@run_async
async def status_command(prefix_1: Annotated[
                             Optional[ConceptPrefix],
                             typer.Argument(
                                 help='The prefix of the first vocabulary. Omit to check all annotations.'
                             )
                         ] = None,
                         prefix_2: Annotated[
                             Optional[ConceptPrefix],
                             typer.Argument(
                                 help='The prefix of the second vocabulary. Omit to check all annotations.'
                             )
                         ] = None,
                         ):
    if prefix_1 or prefix_2:
        if not (prefix_1 and prefix_2):
            raise typer.BadParameter(
                'Both prefix_1 and prefix_2 must be provided together to check a specific annotation.'
            )
        annotations = [(prefix_1, prefix_2)]
    else:
        annotations = _get_all_annotations()

    table = Table(
        'Source Prefix',
        'Target Prefix',
        'Name',
        'Loaded in DB',
        'Number of Mappings',
    )
    for prefix_a, prefix_b in annotations:
        status = await get_annotation_status(prefix_a, prefix_b)
        table.add_row(
            prefix_a.value,
            prefix_b.value,
            status.name,
            'Yes' if status.loaded else 'No',
            str(status.relationship_count) if status.relationship_count is not None else '0',
        )

    CONSOLE.print(table)
