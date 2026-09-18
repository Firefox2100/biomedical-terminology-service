import csv
import importlib
import importlib.resources
import inspect
import json
import os
from pathlib import Path
import aiofiles
import aiofiles.os

from bioterms.etc.enums import AnnotationType, ConceptPrefix
from bioterms.etc.consts import CONFIG, LOGGER
from bioterms.etc.utils import check_files_exist
from bioterms.etc.restore import batched_write
from bioterms.database import Cache, GraphDatabase, get_active_cache, get_active_graph_db
from bioterms.model.annotation_status import AnnotationStatus
from bioterms.model.annotation import Annotation
from bioterms.vocabulary.utils import parse_annotation_curie, write_annotations_to_file


class _OfflineAnnotationGraphDB:
    """
    Lightweight adapter that captures annotation writes into offline dump files.
    """

    def __init__(self,
                 prefix_1: ConceptPrefix,
                 prefix_2: ConceptPrefix,
                 overwrite: bool,
                 ):
        self.prefix_1 = prefix_1
        self.prefix_2 = prefix_2
        self.overwrite = overwrite
        self._written = False

    async def count_terms(self,
                          prefix: ConceptPrefix,
                          ) -> int:
        # Offline annotation generation may run without online databases.
        return 1

    async def count_annotations(self,
                                prefix_1: ConceptPrefix,
                                prefix_2: ConceptPrefix,
                                ) -> int:
        if self.overwrite:
            return 0

        offline_file_path = os.path.join(
            CONFIG.data_dir,
            'offline',
            f'{self.prefix_1.value}-{self.prefix_2.value}.annotation.dump',
        )
        return 1 if os.path.exists(offline_file_path) and os.path.getsize(offline_file_path) > 0 else 0

    async def save_annotations(self,
                               annotations: list[Annotation],
                               ):
        await write_annotations_to_file(
            prefix_from=self.prefix_1,
            prefix_to=self.prefix_2,
            annotations=annotations,
            overwrite=self.overwrite and not self._written,
        )
        self._written = True


def _offline_annotation_dump_path(prefix_1: ConceptPrefix,
                                  prefix_2: ConceptPrefix,
                                  ) -> str:
    return os.path.join(CONFIG.data_dir, 'offline', f'{prefix_1.value}-{prefix_2.value}.annotation.dump')


def _get_annotation_module_name(prefix_1: ConceptPrefix,
                                prefix_2: ConceptPrefix,
                                ) -> str:
    """
    Get the annotation module name for the given pair of prefixes.
    :param prefix_1: The first prefix.
    :param prefix_2: The second prefix.
    :return: The annotation module name.
    """
    prefix_str_1 = prefix_1.value.lower()
    prefix_str_2 = prefix_2.value.lower()

    sorted_prefixes = sorted([prefix_str_1, prefix_str_2])
    annotation_module_name = f'{sorted_prefixes[0]}_{sorted_prefixes[1]}'

    return annotation_module_name


def get_annotation_module(prefix_1: ConceptPrefix,
                          prefix_2: ConceptPrefix,
                          ):
    """
    Get the annotation module for the given pair of prefixes.
    :param prefix_1: The first prefix.
    :param prefix_2: The second prefix.
    :return: The annotation module.
    """
    annotation_module_name = _get_annotation_module_name(prefix_1, prefix_2)

    try:
        annotation_module = importlib.import_module(f'bioterms.annotation.{annotation_module_name}')
    except ModuleNotFoundError:
        raise ValueError(f'No annotation available for prefixes: {prefix_1}, {prefix_2}')

    return annotation_module


def get_annotation_config(prefix_1: ConceptPrefix,
                          prefix_2: ConceptPrefix,
                          ) -> dict:
    """
    Get the annotation configuration for the given pair of prefixes.
    :param prefix_1: The first prefix.
    :param prefix_2: The second prefix.
    :return: The annotation configuration.
    """
    annotation_module = get_annotation_module(prefix_1, prefix_2)
    return {
        'name': annotation_module.ANNOTATION_NAME,
        'prefix1': annotation_module.VOCABULARY_PREFIX_1,
        'prefix2': annotation_module.VOCABULARY_PREFIX_2,
        'filePaths': annotation_module.FILE_PATHS,
    }


async def delete_annotation_files(prefix_1: ConceptPrefix,
                                  prefix_2: ConceptPrefix,
                                  ):
    """
    Delete the annotation files for the given pair of prefixes.
    :param prefix_1: The first prefix.
    :param prefix_2: The second prefix.
    :return: The annotation module.
    """
    annotation_module = get_annotation_module(prefix_1, prefix_2)

    deletion_func = getattr(annotation_module, 'delete_annotation_files', None)

    if deletion_func is None or not callable(deletion_func):
        # Fallback to default deletion method
        for file_path in annotation_module.FILE_PATHS:
            try:
                await aiofiles.os.remove(file_path)
            except Exception:
                pass
    else:
        result = deletion_func()
        if inspect.iscoroutine(result):
            await result


async def download_annotation(prefix_1: ConceptPrefix,
                              prefix_2: ConceptPrefix,
                              redownload: bool = False,
                              download_client=None,
                              ):
    """
    Download the annotation specified by the pair of prefixes.
    :param prefix_1: The first prefix.
    :param prefix_2: The second prefix.
    :param redownload: Whether to redownload the files even if they exist.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    LOGGER.info(
        'Downloading annotation %s -> %s (redownload=%s)',
        prefix_1.value, prefix_2.value, redownload,
    )
    annotation_module = get_annotation_module(prefix_1, prefix_2)

    if redownload:
        await delete_annotation_files(prefix_1, prefix_2)

    download_func = getattr(annotation_module, 'download_annotation', None)
    if download_func is None or not callable(download_func):
        raise ValueError(f'Annotation module for {prefix_1} and {prefix_2} does not '
                         f'have a download_annotation function.')

    result = download_func(download_client=download_client)
    if inspect.iscoroutine(result):
        await result
    LOGGER.info('Annotation download complete: %s -> %s', prefix_1.value, prefix_2.value)


async def delete_annotation(prefix_1: ConceptPrefix,
                            prefix_2: ConceptPrefix,
                            graph_db: GraphDatabase = None,
                            ):
    """
    Delete the annotation specified by the pair of prefixes from the database.
    :param prefix_1: The first prefix.
    :param prefix_2: The second prefix.
    :param graph_db: Optional GraphDatabase instance to use.
    """
    LOGGER.info('Deleting annotation %s -> %s', prefix_1.value, prefix_2.value)
    annotation_module = get_annotation_module(prefix_1, prefix_2)
    cache = get_active_cache()

    delete_func = getattr(annotation_module, 'delete_annotation_data', None)

    if delete_func is None or not callable(delete_func):
        # Fallback to default deletion method
        if graph_db is None:
            graph_db = get_active_graph_db()

        await graph_db.delete_annotations(
            prefix_1=annotation_module.VOCABULARY_PREFIX_1,
            prefix_2=annotation_module.VOCABULARY_PREFIX_2,
        )
    else:
        result = delete_func(
            graph_db=graph_db,
        )
        if inspect.iscoroutine(result):
            await result

    await cache.rotate_dataset_version()
    LOGGER.info('Annotation deletion complete: %s -> %s', prefix_1.value, prefix_2.value)


def _offline_dump_already_exists(annotation_module,
                                 ) -> bool:
    """
    Check whether an offline annotation dump already exists for the given annotation module.
    Used to tolerate a NotImplementedError from an annotation's live loader when running
    offline, since the annotation may have already been dumped by an earlier offline run.
    :param annotation_module: The annotation module being loaded.
    :return: True if the offline dump file exists, False otherwise.
    """
    dump_path = _offline_annotation_dump_path(
        annotation_module.VOCABULARY_PREFIX_1,
        annotation_module.VOCABULARY_PREFIX_2,
    )
    return os.path.exists(dump_path)


async def load_annotation(prefix_1: ConceptPrefix,
                          prefix_2: ConceptPrefix,
                          overwrite: bool = True,
                          offline: bool = False,
                          graph_db: GraphDatabase = None,
                          ):
    """
    Load the annotation specified by the pair of prefixes.
    :param prefix_1: The first prefix.
    :param prefix_2: The second prefix.
    :param overwrite: Whether to overwrite existing annotation data.
    :param offline: Whether to write output to offline dump file instead of graph database.
    :param graph_db: Optional GraphDatabase instance to use.
    """
    LOGGER.info(
        'Loading annotation %s -> %s (overwrite=%s, offline=%s)',
        prefix_1.value, prefix_2.value, overwrite, offline,
    )
    annotation_module = get_annotation_module(prefix_1, prefix_2)

    if not check_files_exist(annotation_module.FILE_PATHS):
        raise ValueError(f'Annotation files for {prefix_1} and {prefix_2} not found. '
                         f'Are they downloaded?')

    if overwrite and not offline:
        # Drop existing data before loading
        await delete_annotation(
            prefix_1=prefix_1,
            prefix_2=prefix_2,
            graph_db=graph_db,
        )

    load_func = getattr(annotation_module, 'load_annotation_from_file', None)
    if load_func is None or not callable(load_func):
        raise ValueError(f'Annotation module for {prefix_1} and {prefix_2} does not '
                         f'have a load_annotation_from_file function.')

    active_graph_db = graph_db
    if offline:
        active_graph_db = _OfflineAnnotationGraphDB(
            prefix_1=annotation_module.VOCABULARY_PREFIX_1,
            prefix_2=annotation_module.VOCABULARY_PREFIX_2,
            overwrite=overwrite,
        )

    try:
        result = load_func(
            graph_db=active_graph_db,
        )
    except NotImplementedError:
        if not offline or not _offline_dump_already_exists(annotation_module):
            raise
        return

    if inspect.iscoroutine(result):
        try:
            await result
        except NotImplementedError:
            if not offline or not _offline_dump_already_exists(annotation_module):
                raise
            return

    if offline:
        LOGGER.info('Annotation load complete: %s -> %s (offline)', prefix_1.value, prefix_2.value)
        return

    cache = get_active_cache()

    await cache.rotate_dataset_version()
    LOGGER.info('Annotation load complete: %s -> %s', prefix_1.value, prefix_2.value)


async def get_annotation_status(prefix_1: ConceptPrefix,
                                prefix_2: ConceptPrefix,
                                cache: Cache = None,
                                graph_db: GraphDatabase = None,
                                use_cache: bool = True,
                                ) -> AnnotationStatus:
    """
    Get the annotation status for the given pair of prefixes.
    :param prefix_1: The first prefix.
    :param prefix_2: The second prefix.
    :param cache: Optional Cache instance to use.
    :param graph_db: Optional GraphDatabase instance to use.
    :param use_cache: Whether to use cached status if available.
    :return: The AnnotationStatus instance.
    """
    annotation_module = get_annotation_module(prefix_1, prefix_2)

    if cache is None:
        cache = get_active_cache()

    prefix_1 = annotation_module.VOCABULARY_PREFIX_1
    prefix_2 = annotation_module.VOCABULARY_PREFIX_2

    if use_cache:
        cached_status = await cache.get_annotation_status(
            prefix_1=prefix_1,
            prefix_2=prefix_2,
        )

        if cached_status is not None:
            return cached_status

    if graph_db is None:
        graph_db = get_active_graph_db()

    annotation_count = await graph_db.count_annotations(
        prefix_1=prefix_1,
        prefix_2=prefix_2,
    )

    status = AnnotationStatus(
        prefixSource=prefix_1,
        prefixTarget=prefix_2,
        name=annotation_module.ANNOTATION_NAME,
        loaded=annotation_count > 0,
        relationshipCount=annotation_count,
    )

    await cache.save_annotation_status(
        status=status,
    )

    return status


def _infer_annotation_dump_prefixes(path: Path) -> tuple[str | None, str | None]:
    """
    Infer zero, one, or two fallback prefixes from an annotation dump filename.
    :param path: The annotation dump file path.
    :return: The (source, target) prefix strings inferred from the filename, or None each if
        the filename carries no prefix information (a bare `.annotation.dump`).
    """
    suffix = '.annotation.dump'
    if not path.name.endswith(suffix):
        raise ValueError(f'Annotation dump must end in {suffix}: {path}')
    stem = path.name[:-len(suffix)]
    if not stem:
        return None, None
    parts = stem.split('-')
    if len(parts) == 1:
        return parts[0], None
    if len(parts) == 2:
        return parts[0], parts[1]
    raise ValueError(
        f'Cannot infer prefixes from {path.name!r}; use --source-prefix and --target-prefix.'
    )


def _canonical_annotation_prefix(value: str | ConceptPrefix | None) -> str | ConceptPrefix | None:
    """
    Normalise a prefix to a `ConceptPrefix` or a lowercase string for external vocabularies.
    :param value: The raw prefix value (string or ConceptPrefix), or None.
    :return: The normalised prefix, or None if `value` was None/blank.
    """
    if value is None or not str(value).strip():
        return None
    value = value.value if isinstance(value, ConceptPrefix) else str(value).strip()
    try:
        return ConceptPrefix(value.lower())
    except ValueError:
        return value.lower()


async def restore_annotation(dump_path: str | os.PathLike,
                             source_prefix: str | ConceptPrefix | None = None,
                             target_prefix: str | ConceptPrefix | None = None,
                             overwrite: bool = False,
                             batch_size: int = 5000,
                             cache: Cache = None,
                             graph_db: GraphDatabase = None,
                             ) -> int:
    """
    Restore an annotation dump file (produced by `load_annotation(..., offline=True)`, i.e.
    `write_annotations_to_file`) into the live graph database.

    Each row carries source/target prefix and CURIE columns, so restoring does not require
    knowing the annotation pair up front. `source_prefix` and `target_prefix`
    (explicit, or inferred from the dump filename when omitted) are used only as a fallback for
    rows where a prefix column is empty. This goes through `GraphDatabase.save_annotations`
    (the same interface `load_annotation` itself uses when not offline) rather than talking to
    Neo4j directly, so it also works with the PostgreSQL graph driver.
    :param dump_path: Path to a `<prefix1>[-<prefix2>].annotation.dump` file.
    :param source_prefix: Fallback source prefix, overriding filename inference.
    :param target_prefix: Fallback target prefix, overriding filename inference.
    :param overwrite: Whether to drop any existing annotations for the inferred/explicit pair
        before restoring. Requires the pair to be resolvable and registered (from a known
        `bioterms.annotation.<p1>_<p2>` module) -- there is no such pair to delete otherwise.
    :param batch_size: Number of annotations written per `save_annotations` call.
    :param cache: The cache instance.
    :param graph_db: The graph database instance.
    :return: The number of annotations restored.
    """
    dump_path = Path(dump_path)
    LOGGER.info(
        'Restoring annotations from %s (overwrite=%s, batch_size=%s)',
        dump_path, overwrite, batch_size,
    )
    if not dump_path.is_file():
        raise ValueError(f'Annotation dump not found: {dump_path}')

    inferred_source, inferred_target = _infer_annotation_dump_prefixes(dump_path)
    source_fallback = _canonical_annotation_prefix(source_prefix) or _canonical_annotation_prefix(inferred_source)
    target_fallback = _canonical_annotation_prefix(target_prefix) or _canonical_annotation_prefix(inferred_target)

    if graph_db is None:
        graph_db = get_active_graph_db()

    if overwrite:
        if source_fallback is None or target_fallback is None:
            raise ValueError(
                'Cannot determine the (source, target) prefix pair to overwrite from '
                f'{dump_path.name!r}; pass --source-prefix/--target-prefix explicitly.'
            )
        await delete_annotation(prefix_1=source_fallback, prefix_2=target_fallback, graph_db=graph_db)

    def annotations():
        with dump_path.open(encoding='utf-8', newline='') as f:
            for line_number, row in enumerate(csv.reader(f), 1):
                if not row or not any(value.strip() for value in row):
                    continue
                if len(row) < 6:
                    raise ValueError(f'{dump_path}:{line_number} has {len(row)} columns; expected at least 6')

                row_source_prefix, source_id, row_target_prefix, target_id, annotation_type, properties_text = row[:6]
                source_curie = parse_annotation_curie(
                    _canonical_annotation_prefix(row_source_prefix), source_id, source_fallback,
                )
                target_curie = parse_annotation_curie(
                    _canonical_annotation_prefix(row_target_prefix), target_id, target_fallback,
                )
                source_curie_prefix, source_curie_id = source_curie.split(':', 1)
                target_curie_prefix, target_curie_id = target_curie.split(':', 1)

                try:
                    properties = json.loads(properties_text) if properties_text.strip() else None
                except json.JSONDecodeError as exc:
                    raise ValueError(f'{dump_path}:{line_number} contains invalid properties JSON') from exc

                yield Annotation(
                    prefixFrom=source_curie_prefix,
                    conceptIdFrom=source_curie_id,
                    prefixTo=target_curie_prefix,
                    conceptIdTo=target_curie_id,
                    annotationType=annotation_type or AnnotationType.ANNOTATED_WITH.value,
                    properties=properties,
                )

    async def save(batch: list[Annotation]) -> None:
        await graph_db.save_annotations(batch)

    total = await batched_write(annotations(), save, batch_size)

    if cache is None:
        cache = get_active_cache()

    await cache.rotate_dataset_version()

    LOGGER.info('Annotation restore complete: %s (%s annotations)', dump_path, total)

    return total
