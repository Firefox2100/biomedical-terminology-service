import traceback
from pathlib import Path
from typing import Annotated, Optional
import typer

from bioterms.etc.enums import ConceptPrefix, SimilarityMethod
from bioterms.vocabulary import get_vocabulary_config
from bioterms.similarity import calculate_similarity
from .utils import CONSOLE, run_async


app = typer.Typer(help='Manage similarity computations between biomedical terms.')


async def _run_one_similarity_calculation(target: ConceptPrefix,
                                          corp: ConceptPrefix,
                                          method: SimilarityMethod,
                                          threshold: Optional[float],
                                          offline: bool,
                                          annotation_file: Optional[Path],
                                          ):
    """
    Calculate similarity for one (target, corpus, method) combination, printing
    success/failure to the console.
    :param target: The target vocabulary prefix.
    :param corp: The corpus vocabulary prefix.
    :param method: The similarity calculation method to use.
    :param threshold: The similarity threshold to apply, or None to use the method's default.
    :param offline: Whether to run the calculation in offline mode.
    :param annotation_file: Optional annotation dump override for offline calculation.
    """
    try:
        await calculate_similarity(
            method=method,
            target_prefix=target,
            corpus_prefix=corp,
            similarity_threshold=threshold,
            offline=offline,
            annotation_file_path=annotation_file,
        )
        CONSOLE.print(
            f'[green]Successfully calculated similarity between '
            f'{target.value} and {corp.value} using {method.value} method.[/green]'
        )
    except Exception as e:
        CONSOLE.print(
            f'[red]Failed to calculate similarity between '
            f'{target.value} and {corp.value} using {method.value} method: {e}[/red]'
        )
        traceback.print_exc()


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

    targets = [target_prefix] if target_prefix else list(ConceptPrefix)

    for target in targets:
        target_config = get_vocabulary_config(target)
        corpus = [corpus_prefix] if corpus_prefix else target_config['annotations']
        methods = [method] if method else target_config['similarityMethods']

        for corp in corpus:
            for m in methods:
                await _run_one_similarity_calculation(target, corp, m, threshold, offline, annotation_file)
