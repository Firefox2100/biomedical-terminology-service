from pathlib import Path
from typing import Annotated, Optional
import typer

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, SimilarityMethod
from bioterms.etc.utils import iter_progress
from bioterms.vocabulary import get_vocabulary_config
from bioterms.similarity import calculate_similarity, restore_similarity
from .utils import CONSOLE, observe_cli_exception, run_async, verbose_cli, verbose_targets


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
        verbose_cli(
            f'calculating {method.value} similarity for {target.value} -> {corp.value} '
            f'(threshold={threshold if threshold is not None else "default"}, offline={offline})'
        )
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
        observe_cli_exception('calculate similarity', e)
        CONSOLE.print(
            f'[red]Failed to calculate similarity between '
            f'{target.value} and {corp.value} using {method.value} method: {e}[/red]'
        )


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
    verbose_targets('calculate similarity', targets)

    jobs = []
    for target in targets:
        target_config = get_vocabulary_config(target)
        corpus = [corpus_prefix] if corpus_prefix else target_config['annotations']
        methods = [method] if method else target_config['similarityMethods']
        for corp in corpus:
            for m in methods:
                jobs.append((target, corp, m))

    verbose_cli(f'calculate similarity: planned {len(jobs):,} target/corpus/method combination(s)')
    for target, corp, selected_method in iter_progress(
            jobs, description='Calculating similarities'):
        await _run_one_similarity_calculation(
            target, corp, selected_method, threshold, offline, annotation_file,
        )


@app.command(name='restore', help='Restore similarity scores from offline dump files into the database.')
@run_async
async def restore_command(target_prefix: Annotated[
                              Optional[ConceptPrefix],
                              typer.Argument(help='The target vocabulary whose similarity dumps to restore.')
                          ] = None,
                          restore_all: Annotated[
                              bool,
                              typer.Option(
                                  '--all',
                                  '-a',
                                  help='Restore similarity dumps for every vocabulary that has any.'
                              )
                          ] = False,
                          batch_size: Annotated[
                              int,
                              typer.Option(
                                  '--batch-size',
                                  '-b',
                                  help='Number of similarity scores written to the database per request.',
                              )
                          ] = 5000,
                          offline_dir: Annotated[
                              Optional[str],
                              typer.Option(
                                  '--offline-dir',
                                  help='Directory containing the offline dump files '
                                       '(default: BTS_DATA_DIR/offline).',
                              )
                          ] = None,
                          ):
    if restore_all:
        if target_prefix:
            raise typer.BadParameter('Cannot use --all option with a target prefix argument.')
        targets = list(ConceptPrefix)
    else:
        if not target_prefix:
            raise typer.BadParameter('Either specify a target vocabulary or use the --all flag.')
        targets = [target_prefix]

    search_dir = Path(offline_dir) if offline_dir else Path(CONFIG.data_dir) / 'offline'
    verbose_targets('restore similarity', targets)
    verbose_cli(f'searching for similarity dumps in {search_dir} (batch_size={batch_size:,})')

    for target in iter_progress(targets, description='Restoring vocabulary similarities'):
        if restore_all and not list(search_dir.glob(f'{target.value}-*.similarity.dump')):
            # --all sweeps every prefix; most won't have similarity dumps, which is expected
            # (only a few vocabularies get similarity calculated at all) -- skip silently
            # rather than reporting every vocabulary without one as a failure.
            continue
        try:
            verbose_cli(f'restoring similarity scores for {target.value}')
            count = await restore_similarity(
                target_prefix=target,
                batch_size=batch_size,
                offline_dir=offline_dir,
            )
            CONSOLE.print(
                f'[green]Successfully restored {count} similarity scores for {target.value}.[/green]'
            )
        except Exception as e:
            observe_cli_exception('restore similarity', e)
            CONSOLE.print(f'[red]Failed to restore similarity scores for {target.value}: {e}[/red]')
