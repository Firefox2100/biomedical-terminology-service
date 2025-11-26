import traceback
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
                                ConceptPrefix,
                                typer.Option(
                                    '--target',
                                    '-a',
                                    help='The prefix of the target vocabulary.'
                                )
                            ],
                            corpus_prefix: Annotated[
                                ConceptPrefix,
                                typer.Option(
                                    '--corpus',
                                    '-c',
                                    help='The prefix of the corpus vocabulary.'
                                )
                            ],
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
                            ):
    try:
        if method:
            methods = [method]
        else:
            vocab_config = get_vocabulary_config(target_prefix)
            methods = vocab_config['similarityMethods']

        for sim_method in methods:
            await calculate_similarity(
                method=sim_method,
                target_prefix=target_prefix,
                corpus_prefix=corpus_prefix,
                similarity_threshold=threshold,
            )
            CONSOLE.print(
                f'[green]Successfully calculated similarity between '
                f'{target_prefix.value} and {corpus_prefix.value} using {sim_method.value} method.[/green]'
            )
    except Exception as e:
        CONSOLE.print(
            f'[red]Failed to calculate similarity between '
            f'{target_prefix.value} and {corpus_prefix.value}: {e}[/red]'
        )
        traceback.print_exc()
