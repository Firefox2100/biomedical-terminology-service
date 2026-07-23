import traceback
from pathlib import Path
from typing import Annotated, Optional
import typer

from bioterms.etc.enums import ConceptPrefix, SimilarityMethod
from bioterms.vocabulary import get_vocabulary_config
from bioterms.similarity import calculate_similarity
from .utils import CONSOLE, run_async


app = typer.Typer(help='Manage similarity computations between biomedical terms.')


@app.command(name='calculate', help='Calculate similarity between two vocabularies and store the results.')
@run_async
async def calculate_command(target_prefix: Annotated[
                                Optional[ConceptPrefix],
                                typer.Option(
                                    '--target',
                                    '-a',
                                    help='The prefix of the target vocabulary.'
                                )
                            ] = None,
                            corpus_prefix: Annotated[
                                Optional[ConceptPrefix],
                                typer.Option(
                                    '--corpus',
                                    '-c',
                                    help='The prefix of the corpus vocabulary.'
                                )
                            ] = None,
                            method: Annotated[
                                Optional[SimilarityMethod],
                                typer.Option(
                                    '--method',
                                    '-m',
                                    help='The similarity calculation method to use.'
                                         'If not provided, use all available methods.'
                                )
                            ] = None,
                            threshold: Annotated[
                                Optional[float],
                                typer.Option(
                                    '--threshold',
                                    '-t',
                                    help='The similarity threshold to apply.'
                                         'If not provided, use the default threshold for the method.'
                                )
                            ] = None,
                            offline: Annotated[
                                bool,
                                typer.Option(
                                    '--offline',
                                    is_flag=True,
                                    help='Run the calculation in offline mode. Requires the offline '
                                         'files from loading functions.'
                                )
                            ] = False,
                            annotation_file: Annotated[
                                Optional[Path],
                                typer.Option(
                                    '--annotation-file',
                                    help='Override the annotation dump used in offline mode. '
                                         'Only rows for the selected target/corpus pair are loaded.'
                                )
                            ] = None,
                            ):
    if annotation_file is not None and not offline:
        raise typer.BadParameter('--annotation-file requires --offline.')

    if target_prefix:
        targets = [target_prefix]
    else:
        targets = list(ConceptPrefix)

    for target in targets:
        target_config = get_vocabulary_config(target)

        if corpus_prefix:
            corpus = [corpus_prefix]
        else:
            corpus = target_config['annotations']

        for corp in corpus:
            if method:
                methods = [method]
            else:
                methods = target_config['similarityMethods']

            for m in methods:
                try:
                    await calculate_similarity(
                        method=m,
                        target_prefix=target,
                        corpus_prefix=corp,
                        similarity_threshold=threshold,
                        offline=offline,
                        annotation_file_path=annotation_file,
                    )
                    CONSOLE.print(
                        f'[green]Successfully calculated similarity between '
                        f'{target.value} and {corp.value} using {m.value} method.[/green]'
                    )
                except Exception as e:
                    CONSOLE.print(
                        f'[red]Failed to calculate similarity between '
                        f'{target.value} and {corp.value} using {m.value} method: {e}[/red]'
                    )
                    traceback.print_exc()
