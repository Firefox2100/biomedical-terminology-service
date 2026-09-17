"""
Utility functions for data management, downloading, extraction, and processing.
"""

import asyncio
import os
import io
import itertools
import zipfile
import uuid
import tempfile
import fnmatch
import zlib
import tarfile
import warnings
from collections.abc import MutableSequence, Iterable, Sized
from pathlib import Path
from itertools import islice
from concurrent.futures import Executor
from typing import Any, Iterator, AsyncIterable, AsyncIterator, Callable, Optional, TypeVar, TYPE_CHECKING
import aiofiles
import aiofiles.os
import httpx
import pandas as pd
import networkx as nx
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, \
    TimeRemainingColumn, DownloadColumn, TransferSpeedColumn

from .consts import CONFIG, DOWNLOAD_CLIENT, QUERY_CLIENT, LOGGER
from .errors import FilesNotFound

if TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer

_TRANSFORMER: Optional['SentenceTransformer'] = None
T = TypeVar('T')
R = TypeVar('R')


def _progress_columns(total_known: bool = True) -> list:
    """
    Build the standard set of rich.progress columns shared by this module's progress bars.
    :param total_known: Whether the task has a known total, to show a completion fraction
        and estimated time remaining rather than just a running count.
    """
    columns = [
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}") if total_known else TextColumn("{task.completed} batches"),
        TimeElapsedColumn(),
    ]
    if total_known:
        columns.append(TimeRemainingColumn())

    return columns


def _download_progress_columns() -> list:
    return [
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ]


def check_files_exist(files: list[str]) -> bool:
    """
    Check if all specified files exist in the data directory.
    :param files: List of file names to check.
    :return: True if all files exist, False otherwise.
    """
    for file_name in files:
        if not os.path.exists(os.path.join(CONFIG.data_dir, file_name)):
            return False

    return True


async def download_obo_owl_release(release_url: str,
                                   file_path: str,
                                   download_client: httpx.AsyncClient = None,
                                   ):
    """Download one canonical OWL product from an OBO ontology release."""
    if check_files_exist([file_path]):
        return

    ensure_data_directory()
    await download_file(
        url=release_url,
        file_path=file_path,
        download_client=download_client,
    )


def load_obo_owl_classes(file_path: str,
                         class_name_prefix: str,
                         ) -> tuple[Any, list[Any]]:
    """
    Load an OBO OWL release into an isolated owlready2 World and return only classes in
    the ontology's own identifier namespace. Release products commonly include imported
    classes from BFO, RO, CHEBI, GO, and other ontologies; those must not become concepts
    in the vocabulary being loaded.
    """
    from owlready2 import World

    absolute_path = os.path.join(CONFIG.data_dir, file_path)
    ontology = World().get_ontology(Path(absolute_path).resolve().as_uri()).load()
    classes = [
        ontology_class
        for ontology_class in ontology.classes()
        if ontology_class.name.startswith(class_name_prefix)
    ]
    return ontology, classes


def obo_entity_local_id(entity: Any,
                        id_prefix: str,
                        ) -> str | None:
    """Return the local ID for an OBO entity/CURIE/IRI when it belongs to ``id_prefix``."""
    value = getattr(entity, 'name', None) or str(entity)
    underscore_prefix = f'{id_prefix}_'
    curie_prefix = f'{id_prefix}:'

    if value.startswith(underscore_prefix):
        return value[len(underscore_prefix):]
    if value.startswith(curie_prefix):
        return value[len(curie_prefix):]

    iri_marker = f'/{underscore_prefix}'
    if iri_marker in value:
        return value.rsplit(iri_marker, 1)[1]
    return None


def obo_class_metadata(ontology_class: Any) -> dict[str, Any]:
    """Extract the common descriptive fields encoded by OBO OWL release products."""
    def first_value(*attribute_names: str) -> str | None:
        for attribute_name in attribute_names:
            values = getattr(ontology_class, attribute_name, [])
            if values:
                return str(values[0])
        return None

    synonyms = []
    for attribute_name in (
        'hasExactSynonym',
        'hasBroadSynonym',
        'hasNarrowSynonym',
        'hasRelatedSynonym',
    ):
        synonyms.extend(str(value) for value in getattr(ontology_class, attribute_name, []))

    # Preserve release order while removing duplicates and a synonym identical to the label.
    label = first_value('label')
    synonyms = list(dict.fromkeys(value for value in synonyms if value != label))

    return {
        'label': label,
        'definition': first_value('IAO_0000115', 'definition'),
        'comment': first_value('comment'),
        'deprecated': bool(getattr(ontology_class, 'deprecated', [])),
        'synonyms': synonyms or None,
    }


def ensure_data_directory():
    """
    Ensure that the data directory exists.
    """
    if not os.path.exists(CONFIG.data_dir):
        os.makedirs(CONFIG.data_dir, exist_ok=True)


def _batch_mutable_sequence(seq: MutableSequence,
                            batch_size: int,
                            consume: bool,
                            ) -> Iterator[list]:
    """
    Batch a MutableSequence (e.g. a list), showing a determinate progress bar since the
    total length is known upfront.
    :param seq: The list-like sequence to batch.
    :param batch_size: Size of each batch.
    :param consume: Whether to pop items from the sequence instead of slicing it.
    """
    if not seq:
        return

    batch_count = (len(seq) + batch_size - 1) // batch_size
    if batch_count <= 1:
        yield seq
        return

    if CONFIG.disable_progress_bar:
        if not consume:
            for i in range(0, len(seq), batch_size):
                yield seq[i: i + batch_size]
        else:
            while seq:
                yield [seq.pop() for _ in range(min(len(seq), batch_size))]
        return

    with Progress(*_progress_columns()) as progress:
        task = progress.add_task(description="Batching...", total=batch_count)

        if not consume:
            n = len(seq)
            for i in range(0, n, batch_size):
                yield seq[i: i + batch_size]
                progress.advance(task)
        else:
            while seq:
                k = min(len(seq), batch_size)
                yield [seq.pop() for _ in range(k)]
                progress.advance(task)


def _batch_general_iterable(seq: Iterable,
                            batch_size: int,
                            ) -> Iterator[list]:
    """
    Batch a general (possibly single-pass) iterable, showing an indeterminate progress bar
    since the total length is not known upfront.
    :param seq: The iterable to batch.
    :param batch_size: Size of each batch.
    """
    it = iter(seq)

    first = next(it, None)
    if first is None:
        return

    if CONFIG.disable_progress_bar:
        batch = [first]
        while True:
            batch.extend(islice(it, batch_size - len(batch)))
            yield batch
            first = next(it, None)
            if first is None:
                return
            batch = [first]

    with Progress(*_progress_columns(total_known=False), transient=False) as progress:
        task = progress.add_task(description="Batching...", total=None)

        batch = [first]
        while True:
            batch.extend(islice(it, batch_size - len(batch)))
            yield batch
            progress.advance(task)

            first = next(it, None)
            if first is None:
                break
            batch = [first]


def batch_iterable(seq: Iterable[T] | list[T],
                   batch_size: int = 10000,
                   consume: bool = False,
                   ) -> Iterator[list[T]]:
    """
    Batch the input parameters in case of large insertions.
    :param seq: The iterable, can be either a list-like object or an iterator
    :param batch_size: Size of the batch, default to 1000
    """
    if batch_size <= 0:
        raise ValueError('batch_size must be positive')

    if isinstance(seq, MutableSequence):
        yield from _batch_mutable_sequence(seq, batch_size, consume)
        return

    if not isinstance(seq, Iterable):
        raise TypeError('seq must be an iterable')

    yield from _batch_general_iterable(seq, batch_size)


def peek_first(items: Iterable[T]) -> tuple[Optional[T], Iterable[T]]:
    """
    Return the first item of `items` (or None if empty) alongside an iterable that still
    yields every item, including that first one -- so a caller can inspect the first element
    (e.g. to read a `prefix` shared by every item) without forcing the rest into memory.

    A `MutableSequence` (e.g. a list) is indexed and returned unchanged, so a downstream
    `batch_iterable` call still sees a real sequence and can show a determinate progress bar.
    Any other (single-pass) iterable, such as a generator streaming an offline dump file, is
    peeked via its iterator and re-assembled with `itertools.chain` so nothing already pulled
    off it is lost.
    :param items: The iterable to peek into.
    :return: A `(first_item, full_iterable)` tuple.
    """
    if isinstance(items, MutableSequence):
        return (items[0] if items else None), items

    it = iter(items)
    first = next(it, None)
    if first is None:
        return None, it
    return first, itertools.chain([first], it)


def edge_iter(graph: nx.DiGraph | nx.MultiDiGraph | Iterable[tuple[str, str, Optional[str], Optional[str]]],
             ) -> Iterator[tuple[str, str, Optional[str], Optional[str]]]:
    """
    Yield `(source_id, target_id, relationship_type, relationship_key)` tuples for a graph.

    Also accepts an already-tuple-shaped iterable directly (e.g. streamed from an offline
    `.graph.dump` file one CSV row at a time) and yields it through unchanged, so callers can
    stream edges into `GraphDatabase.save_vocabulary_graph` without ever materialising a full
    in-memory `nx.Graph`.
    """
    if isinstance(graph, nx.MultiDiGraph):
        for source, target, key, data in graph.edges(data=True, keys=True):
            yield str(source), str(target), data['label'].value if data.get('label') else None, key
    elif isinstance(graph, nx.DiGraph):
        for source, target, data in graph.edges(data=True):
            yield str(source), str(target), data['label'].value if data.get('label') else None, None
    elif isinstance(graph, Iterable):
        yield from graph
    else:
        raise TypeError('Graph must be a DiGraph, MultiDiGraph, or an iterable of edge tuples.')


async def download_file(url: str,
                        file_path: str,
                        headers: dict[str, str] = None,
                        download_client: httpx.AsyncClient = None,
                        ):
    """
    Download a file from a URL and save it to the specified file path.

    Retries up to CONFIG.download_max_retries times on a transport error (including the
    httpx.ReadTimeout that a multi-GB/multi-hour download can hit from ordinary network
    jitter, even with DOWNLOAD_CLIENT's generous read timeout). Each retry resumes via an
    HTTP Range request starting from whatever is already on disk, rather than restarting
    from byte zero -- restarting a 100GB+ file (e.g. UniProt's TrEMBL release) from scratch
    on every transient failure would be impractical. If the server does not honour the Range
    request (responds 200 instead of 206), the partial file is discarded and the download
    restarts from scratch, since it can no longer be trusted as a valid prefix of a fresh
    response. A Range request that starts exactly at the resource's current size correctly
    gets a 416 back (verified against a real server) -- read as "nothing left to do", not
    an error, so re-running a download that already completed is a cheap no-op.
    :param url: The URL to download the file from.
    :param file_path: The relative file path to save the downloaded file.
    :param headers: Optional headers to include in every request.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if download_client is None:
        download_client = DOWNLOAD_CLIENT

    absolute_file_path = os.path.join(CONFIG.data_dir, file_path)
    os.makedirs(os.path.dirname(absolute_file_path), exist_ok=True)
    file_name = os.path.basename(file_path)
    LOGGER.info('Downloading %s from %s', file_path, url)

    last_error: Exception | None = None

    for attempt in range(1, CONFIG.download_max_retries + 1):
        resume_from = (
            await aiofiles.os.path.getsize(absolute_file_path)
            if await aiofiles.os.path.exists(absolute_file_path)
            else 0
        )

        request_headers = dict(headers or {})
        if resume_from:
            request_headers['Range'] = f'bytes={resume_from}-'

        try:
            async with download_client.stream(
                    'GET',
                    url,
                    follow_redirects=True,
                    headers=request_headers,
            ) as response:
                if resume_from and response.status_code == 416:
                    # The range starts at/beyond the resource's current size: the file on
                    # disk is already the complete download.
                    LOGGER.info('Download already complete: %s (%s bytes)', file_path, resume_from)
                    return

                if resume_from and response.status_code != 206:
                    # Range not honoured (some servers just return 200 with the full body).
                    # The existing partial file's bytes can't be trusted as a prefix of this
                    # fresh, full response, so start this attempt over from scratch.
                    resume_from = 0

                response.raise_for_status()

                mode = 'ab' if resume_from else 'wb'
                response_headers = getattr(response, 'headers', {})
                remaining = int(response_headers.get('content-length', 0)) or None
                total = resume_from + remaining if remaining is not None else None
                async with aiofiles.open(absolute_file_path, mode) as data_file:
                    if CONFIG.disable_progress_bar:
                        async for chunk in response.aiter_bytes():
                            await data_file.write(chunk)
                    else:
                        columns = _download_progress_columns() if total is not None \
                            else _progress_columns(total_known=False)
                        with Progress(*columns, transient=total is None) as progress:
                            task = progress.add_task(
                                description=f'Downloading {file_name}',
                                total=total,
                                completed=resume_from,
                            )
                            async for chunk in response.aiter_bytes():
                                await data_file.write(chunk)
                                progress.advance(task, len(chunk))
            final_size = await aiofiles.os.path.getsize(absolute_file_path)
            LOGGER.info('Downloaded %s (%s bytes)', file_path, final_size)
            return
        except (httpx.TransportError, httpx.HTTPStatusError) as exc:
            last_error = exc
            if attempt < CONFIG.download_max_retries:
                verbose_print(
                    f'Download of {file_path} failed on attempt {attempt}/'
                    f'{CONFIG.download_max_retries} ({exc!r}); retrying with resume...'
                )
                await asyncio.sleep(CONFIG.download_retry_backoff_seconds * attempt)

    raise last_error


async def get_trud_release_url(resource_url: str,
                               client: httpx.AsyncClient = None,
                               ) -> str:
    """
    Get the release URL from TRUD resource URL.
    :param resource_url: The TRUD resource URL.
    :param client: Optional httpx.AsyncClient to use for the request.
    :return: The release archive file URL.
    """
    if client is None:
        client = QUERY_CLIENT

    response = await client.get(resource_url)
    response.raise_for_status()
    payload = response.json()

    if payload['httpStatus'] != 200:
        raise ValueError(f'Failed to get release URL: {payload["message"]}')

    return payload['releases'][0]['archiveFileUrl']


def _nearby_zip_entries(names: list[str], pattern: str, limit: int = 20) -> list[str]:
    """
    List zip entries in the same directory as a pattern that failed to match anything, so a
    FilesNotFound error/warning can show what's actually there instead of just "not found" --
    a release's exact filenames (edition code, date stamp, naming convention) can differ
    between releases in ways that are otherwise only discoverable by downloading again.
    :param pattern: The fnmatch pattern that produced no matches.
    :param names: All entry names in the zip archive.
    :param limit: Maximum number of nearby entries to return.
    :return: Up to `limit` entry names sharing the pattern's directory.
    """
    directory_pattern = f'{pattern.rsplit("/", 1)[0]}/*' if '/' in pattern else '*'
    return [name for name in names if fnmatch.fnmatch(name, directory_pattern)][:limit]


async def extract_file_from_zip(zip_path: str,
                                file_mapping: list[tuple[str, str] | tuple[str, str, bool]],
                                ):
    """
    Extract specific files from a zip archive based on matching patterns.
    :param zip_path: The path to the zip archive.
    :param file_mapping: List of tuples mapping relative file patterns to extracted file
        names, optionally with a third `required` bool (default True, preserving the
        original hard-fail behaviour). A `required=False` entry that matches nothing is
        skipped with a visible warning (not silent, and not gated behind CONFIG.verbose_print
        -- an intentionally-added feature quietly not working is worse than a noisy one)
        instead of aborting the whole extraction; every other pattern in file_mapping still
        gets its chance, including ones listed after it.
    :raises FilesNotFound: If a required pattern matches nothing.
    """
    async with aiofiles.open(zip_path, 'rb') as f:
        zip_bytes = await f.read()

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zip_ref:
        names = zip_ref.namelist()

        for entry in file_mapping:
            pattern, dest_path, required = entry if len(entry) == 3 else (*entry, True)
            matches = [name for name in names if fnmatch.fnmatch(name, pattern)]

            if not matches:
                nearby = _nearby_zip_entries(names, pattern)
                detail = (
                    f' Files actually present in that directory: {nearby}' if nearby
                    else ' That directory does not appear to exist in this archive at all.'
                )
                message = f'No files matching pattern "{pattern}" found in the ZIP archive.{detail}'

                if required:
                    raise FilesNotFound(message)

                warnings.warn(f'{message} Skipping this optional file.', stacklevel=2)
                continue

            member = matches[0]

            with zip_ref.open(member) as src:
                data = src.read()

            dest = Path(dest_path)
            dest.parent.mkdir(parents=True, exist_ok=True)

            async with aiofiles.open(dest, 'wb') as dest_f:
                await dest_f.write(data)


async def extract_file_from_gzip(gzip_path: str,
                                 output_path: str,
                                 chunk_size: int = 1024 * 1024,
                                 ):
    """
    Extract a gzip compressed file.
    :param gzip_path: The path to the gzip file.
    :param output_path: The path to save the decompressed output file.
    :param chunk_size: The chunk size to use when reading the gzip file.
    """
    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)

    async with aiofiles.open(gzip_path, "rb") as f_in, \
        aiofiles.open(output_path, "wb") as f_out:

        while True:
            chunk = await f_in.read(chunk_size)
            if not chunk:
                break

            data = decompressor.decompress(chunk)
            if data:
                await f_out.write(data)

        tail = decompressor.flush()
        if tail:
            await f_out.write(tail)


def _extract_tarball_sync(tarball_path: str,
                          output_dir: str,
                          members: Optional[Iterable[str]] = None,
                          mode: str = "r:gz",
                          ) -> None:
    """
    Synchronous implementation that extracts a .tar.gz archive.
    :param tarball_path: Path to the .tar.gz file.
    :param output_dir: Directory where files will be extracted.
    :param members: Optional iterable of member names to extract. If None, extracts everything.
    """
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    with tarfile.open(tarball_path, mode=mode) as tar:
        if members is None:
            tar.extractall(path=out_path)
        else:
            # members may be file names (str); convert to TarInfo objects
            # if you want to filter by name:
            selected = [m for m in tar.getmembers() if m.name in members]
            tar.extractall(path=out_path, members=selected)


async def extract_file_from_tarball(tarball_path: str,
                                    output_dir: str,
                                    members: Optional[Iterable[str]] = None,
                                    mode: str = "r:gz",
                                    ) -> None:
    """
    Asynchronous wrapper for extracting a .tar.gz archive in a separate thread.
    :param tarball_path: Path to the .tar.gz file.
    :param output_dir: Directory where files will be extracted.
    :param members: Optional iterable of member names to extract.
    :param mode: The mode to open the tar file.
    """
    await asyncio.to_thread(
        _extract_tarball_sync,
        tarball_path,
        output_dir,
        members,
        mode,
    )


async def download_rf2(release_url: str,
                       file_mapping: list[tuple[str, str]],
                       download_client: httpx.AsyncClient = None,
                       ):
    """
    Download and extract RF2 format files from a given release URL.
    :param release_url: The URL of the RF2 release zip file.
    :param file_mapping: List of tuples mapping relative file patterns to extracted file names
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    with tempfile.TemporaryDirectory() as temp_folder:
        temp_id = str(uuid.uuid4())
        zip_path = os.path.join(temp_folder, f'{temp_id}.zip')

        await download_file(
            url=release_url,
            file_path=zip_path,
            download_client=download_client,
        )

        await extract_file_from_zip(
            zip_path=zip_path,
            file_mapping=file_mapping,
        )


def rf2_dataframe_deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate RF2 dataframe by keeping the most recent effectiveTime for each id.
    :param df: The RF2 dataframe to deduplicate.
    :return: A new dataframe with duplicates removed.
    """
    sorted_concept_df = df.sort_values(
        by=['id', 'effectiveTime'],
        ascending=[True, False],
    )
    unique_df = sorted_concept_df.drop_duplicates(
        subset=['id'],
        keep='first',
    )

    return unique_df


def get_transformer() -> 'SentenceTransformer':
    """
    Get the global SentenceTransformer instance, initializing it if necessary.
    :return: The SentenceTransformer instance
    """
    global _TRANSFORMER

    if _TRANSFORMER is None:
        from sentence_transformers import SentenceTransformer

        _TRANSFORMER = SentenceTransformer(
            CONFIG.transformer_model_name,
            device=CONFIG.torch_device,
        )

    return _TRANSFORMER


def iter_progress(iterable: Iterable[T],
                  *,
                  description: str = "Working...",
                  total: float | None = None,
                  transient: bool = False,
                  **kwargs,
                  ) -> Iterator[T]:
    """
    Wrap an iterable with a progress bar using rich.
    :param iterable: The iterable to wrap.
    :param description: Description to display alongside the progress bar.
    :param total: Total number of items in the iterable, if known.
    :param transient: Whether the progress bar should be transient.
    :param kwargs: Additional keyword arguments to pass to the progress bar.
    :return: An iterator that yields items from the iterable with a progress bar.
    """
    description = kwargs.pop('desc', description)
    if total is None and isinstance(iterable, Sized):
        total = len(iterable)

    if CONFIG.disable_progress_bar:
        yield from iterable
        return

    with Progress(*_progress_columns(total_known=total is not None),
                  transient=total is None or transient) as progress:
        task = progress.add_task(description=description, total=total, **kwargs)
        for item in iterable:
            yield item
            progress.advance(task)


async def aiter_progress(async_iterable: AsyncIterable[T],
                         *,
                         description: str = "Working...",
                         total: float | None = None,
                         transient: bool = False,
                         **kwargs,
                         ) -> AsyncIterator[T]:
    """
    Wrap an async iterable with a progress bar using rich.
    :param async_iterable: The async iterable to wrap.
    :param description: Description to display alongside the progress bar.
    :param total: Total number of items in the async iterable, if known.
    :param transient: Whether the progress bar should be transient.
    :param kwargs: Additional keyword arguments to pass to the progress bar.
    :return: An async iterator that yields items from the async iterable with a progress bar.
    """
    description = kwargs.pop('desc', description)
    if total is None and isinstance(async_iterable, Sized):
        total = len(async_iterable)

    if CONFIG.disable_progress_bar:
        async for item in async_iterable:
            yield item
        return

    with Progress(*_progress_columns(total_known=total is not None),
                  transient=total is None or transient) as progress:
        task = progress.add_task(description=description, total=total, **kwargs)
        async for item in async_iterable:
            yield item
            progress.advance(task)


def verbose_print(message: str):
    """
    Print a message if verbose mode is enabled.
    :param message: The message to print.
    """
    LOGGER.debug(message)
    if CONFIG.verbose_print:
        print(message, flush=True)


def _start_optional_progress(description: str | None,
                             total: int | None,
                             transient: bool,
                             ):
    """
    Start a progress bar unless progress bars are globally disabled.
    :param description: Description for the progress bar.
    :param total: Total number of items for the progress bar.
    :param transient: Whether the progress bar should be transient.
    :return: A tuple of (progress, task_id), both None if progress bars are disabled.
    """
    if CONFIG.disable_progress_bar:
        return None, None

    progress = Progress(*_progress_columns(total_known=total is not None),
                        transient=total is None or transient)
    task = progress.add_task(description=description or "Processing...", total=total)
    progress.start()

    return progress, task


def _refill_pending(it: Iterator[T],
                    loop: asyncio.AbstractEventLoop,
                    executor: Executor,
                    func: Callable[[T], R],
                    pending: set[asyncio.Future],
                    ):
    """
    Submit the next item from the iterator to the executor, if any remain, keeping the
    pending set full so worker processes are not left idle while the caller consumes a result.
    :param it: The iterator of remaining items.
    :param loop: The asyncio event loop.
    :param executor: The executor to run tasks in.
    :param func: The function to execute for each item.
    :param pending: The set of pending futures to add the new submission to.
    """
    try:
        next_arg = next(it)
    except StopIteration:
        return

    pending.add(loop.run_in_executor(executor, func, next_arg))


async def schedule_tasks(executor: Executor,
                         func: Callable[[T], R],
                         iterable: Iterable[T],
                         max_concurrency: int = None,
                         loop: asyncio.AbstractEventLoop = None,
                         description: str = None,
                         total: int | None = None,
                         transient: bool = False,
                         ) -> AsyncIterator[R]:
    """
    Schedule tasks to run in an executor with limited concurrency.
    :param executor: The executor to run tasks in.
    :param func: The function to execute for each item.
    :param iterable: The iterable of items to process.
    :param max_concurrency: The maximum number of concurrent tasks.
    :param loop: The asyncio event loop.
    :param description: Description for the progress bar.
    :param total: Total number of items for the progress bar.
    :param transient: Whether the progress bar should be transient.
    :return: An async iterator yielding results as they complete.
    """
    if max_concurrency is None:
        max_concurrency = CONFIG.process_limit * 2
    if max_concurrency < 1:
        raise ValueError('max_concurrency must be at least 1')

    if loop is None:
        loop = asyncio.get_running_loop()

    it = iter(iterable)

    pending: set[asyncio.Future] = set()
    for arg in islice(it, max_concurrency):
        fut = loop.run_in_executor(executor, func, arg)
        pending.add(fut)

    progress, task = _start_optional_progress(description, total, transient)

    try:
        while pending:
            done, pending = await asyncio.wait(
                pending,
                return_when=asyncio.FIRST_COMPLETED,
            )

            for fut in done:
                # Keep workers occupied while the consumer handles this result.
                _refill_pending(it, loop, executor, func, pending)

                yield fut.result()
                if progress is not None:
                    progress.advance(task)
    finally:
        if progress is not None:
            progress.stop()


def initialize_error_reporting(release: str | None = None) -> bool:
    """Initialize Sentry once when error reporting is configured."""
    if not CONFIG.enable_error_reporting:
        return False

    try:
        import sentry_sdk
    except ImportError:
        LOGGER.warning('Error reporting is enabled but sentry-sdk is not installed.')
        return False

    if not CONFIG.sentry_dsn:
        LOGGER.warning('Error reporting is enabled but no Sentry DSN is configured.')
        return False
    if sentry_sdk.is_initialized():
        return True

    options = {'dsn': CONFIG.sentry_dsn}
    if release is not None:
        options['release'] = release
    if CONFIG.enable_profiling:
        options.update({
            'send_default_pii': True,
            'traces_sample_rate': 1.0,
            'profile_session_sample_rate': 1.0,
            'profile_lifecycle': 'trace',
        })
    sentry_sdk.init(**options)
    LOGGER.info('Initialized Sentry error reporting.')
    return True


def report_exception(exc: Exception = None):
    """
    Report an exception using the sentry SDK. If the SDK is not configured, this function does nothing.
    :param exc: The exception to report. If None, it will use the sys.exc_info().
    """
    if not initialize_error_reporting():
        return

    import sentry_sdk
    sentry_sdk.capture_exception(exc)
