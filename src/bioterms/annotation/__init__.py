import importlib
import importlib.resources
import inspect
import aiofiles
import aiofiles.os

from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.utils import check_files_exist
from bioterms.database import Cache, GraphDatabase, get_active_cache, get_active_graph_db
from bioterms.model.annotation_status import AnnotationStatus


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
    annotation_module = get_annotation_module(prefix_1, prefix_2)

    if redownload:
        await delete_annotation_files(prefix_1, prefix_2)

    download_func = getattr(annotation_module, 'download_annotation', None)
    if download_func is None or not callable(download_func):
        raise ValueError(f'Annotation module for {prefix_1} and {prefix_2} does not '
                         f'have a download_annotation function.')

    result = download_func()
    if inspect.iscoroutine(result):
        await result


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
    annotation_module = get_annotation_module(prefix_1, prefix_2)

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


async def load_annotation(prefix_1: ConceptPrefix,
                          prefix_2: ConceptPrefix,
                          overwrite: bool = True,
                          graph_db: GraphDatabase = None,
                          ):
    """
    Load the annotation specified by the pair of prefixes.
    :param prefix_1: The first prefix.
    :param prefix_2: The second prefix.
    :param overwrite: Whether to overwrite existing annotation data.
    :param graph_db: Optional GraphDatabase instance to use.
    """
    annotation_module = get_annotation_module(prefix_1, prefix_2)

    if not check_files_exist(annotation_module.FILE_PATHS):
        raise ValueError(f'Annotation files for {prefix_1} and {prefix_2} not found. '
                         f'Are they downloaded?')

    if overwrite:
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

    result = load_func(
        graph_db=graph_db,
    )
    if inspect.iscoroutine(result):
        await result


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
