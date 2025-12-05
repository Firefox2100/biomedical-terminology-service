import asyncio
import os
import io
import zipfile
import uuid
import tempfile
import fnmatch
import gzip
from pathlib import Path
from itertools import islice
from concurrent.futures import Executor
from typing import Iterable, Iterator, AsyncIterable, AsyncIterator, Callable, Any, TypeVar
import aiofiles
import aiofiles.os
import httpx
import pandas as pd
from sentence_transformers import SentenceTransformer
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn

from .consts import CONFIG, DOWNLOAD_CLIENT, QUERY_CLIENT
from .errors import FilesNotFound


_transformer: SentenceTransformer | None = None
T = TypeVar('T')
R = TypeVar('R')


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


def ensure_data_directory():
    """
    Ensure that the data directory exists.
    """
    if not os.path.exists(CONFIG.data_dir):
        os.makedirs(CONFIG.data_dir, exist_ok=True)


async def download_file(url: str,
                        file_path: str,
                        headers: dict[str, str] = None,
                        download_client: httpx.AsyncClient = None,
                        ):
    """
    Download a file from a URL and save it to the specified file path.
    :param url: The URL to download the file from.
    :param file_path: The relative file path to save the downloaded file.
    :param headers: Optional headers to include in the request.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if download_client is None:
        download_client = DOWNLOAD_CLIENT

    async with download_client.stream(
            'GET',
            url,
            follow_redirects=True,
            headers=headers,
    ) as response:
        response.raise_for_status()

        absolute_file_path = os.path.join(CONFIG.data_dir, file_path)
        os.makedirs(os.path.dirname(absolute_file_path), exist_ok=True)

        async with aiofiles.open(absolute_file_path, 'wb') as data_file:
            async for chunk in aiter_progress(
                response.aiter_bytes(),
                description=f'Downloading {os.path.basename(file_path)}'
            ):
                await data_file.write(chunk)


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


async def extract_file_from_zip(zip_path: str,
                                file_mapping: list[tuple[str, str]]
                                ):
    """
    Extract a specific file from a zip archive based on a matching pattern.
    :param zip_path: The path to the zip archive.
    :param file_mapping: List of tuples mapping relative file patterns to extracted file names
    """
    async with aiofiles.open(zip_path, 'rb') as f:
        zip_bytes = await f.read()

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zip_ref:
        names = zip_ref.namelist()

        for pattern, dest_path in file_mapping:
            matches = [name for name in names if fnmatch.fnmatch(name, pattern)]

            if not matches:
                raise FilesNotFound(
                    f'No files matching pattern "{pattern}" found in the ZIP archive.'
                )

            member = matches[0]

            with zip_ref.open(member) as src:
                data = src.read()

            dest = Path(dest_path)
            dest.parent.mkdir(parents=True, exist_ok=True)

            async with aiofiles.open(dest, 'wb') as dest_f:
                await dest_f.write(data)


async def extract_file_from_gzip(gzip_path: str,
                                 output_path: str,
                                 ):
    """
    Extract a gzip compressed file.
    :param gzip_path: The path to the gzip file.
    :param output_path: The path to save the decompressed output file.
    """
    # Read the gz file asynchronously, decompress in memory, then write output asynchronously
    async with aiofiles.open(gzip_path, 'rb') as f_in:
        gz_data = await f_in.read()

    decompressed = gzip.decompress(gz_data)

    async with aiofiles.open(output_path, 'wb') as f_out:
        await f_out.write(decompressed)


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
    temp_folder = tempfile.TemporaryDirectory()
    try:
        temp_id = str(uuid.uuid4())
        zip_path = os.path.join(temp_folder.name, f'{temp_id}.zip')

        await download_file(
            url=release_url,
            file_path=zip_path,
            download_client=download_client,
        )

        await extract_file_from_zip(
            zip_path=zip_path,
            file_mapping=file_mapping,
        )
    finally:
        temp_folder.cleanup()


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


def get_transformer() -> SentenceTransformer:
    """
    Get the global SentenceTransformer instance, initializing it if necessary.
    :return: The SentenceTransformer instance
    """
    global _transformer

    if _transformer is None:
        _transformer = SentenceTransformer(CONFIG.transformer_model_name)

    return _transformer


def iter_progress(iterable: Iterable[T],
                  *,
                  description: str = "Working...",
                  total: float | None = None,
                  **kwargs,
                  ) -> Iterator[T]:
    """
    Wrap an iterable with a progress bar using rich.
    :param iterable: The iterable to wrap.
    :param description: Description to display alongside the progress bar.
    :param total: Total number of items in the iterable, if known.
    :param kwargs: Additional keyword arguments to pass to the progress bar.
    :return: An iterator that yields items from the iterable with a progress bar.
    """
    if CONFIG.disable_progress_bar:
        yield from iterable
        return

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        transient=total is None,
    ) as progress:
        task = progress.add_task(description=description, total=total, **kwargs)
        for item in iterable:
            yield item
            progress.advance(task)


async def aiter_progress(async_iterable: AsyncIterable[T],
                         *,
                         description: str = "Working...",
                         total: float | None = None,
                         **kwargs) -> AsyncIterator[T]:
    """
    Wrap an async iterable with a progress bar using rich.
    :param async_iterable: The async iterable to wrap.
    :param description: Description to display alongside the progress bar.
    :param total: Total number of items in the async iterable, if known.
    :param kwargs: Additional keyword arguments to pass to the progress bar.
    :return: An async iterator that yields items from the async iterable with a progress bar.
    """
    if CONFIG.disable_progress_bar:
        async for item in async_iterable:
            yield item
            return

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        transient=total is None,
    ) as progress:
        task = progress.add_task(description=description, total=total, **kwargs)
        async for item in async_iterable:
            yield item
            progress.advance(task)


def verbose_print(message: str):
    """
    Print a message if verbose mode is enabled.
    :param message: The message to print.
    """
    if CONFIG.verbose_print:
        print(message)


async def schedule_tasks(executor: Executor,
                         func: Callable[[T], R],
                         iterable: Iterable[T],
                         max_concurrency: int = None,
                         loop: asyncio.AbstractEventLoop = None,
                         description: str = None,
                         total: int | None = None,
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

    if not CONFIG.disable_progress_bar:
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            transient=total is None,
        )
        task = progress.add_task(description=description or "Processing...", total=total)
        progress.start()
    else:
        progress = None
        task = None

    while pending:
        done, pending = await asyncio.wait(
            pending,
            return_when=asyncio.FIRST_COMPLETED,
        )

        for fut in done:
            yield fut.result()
            if progress is not None:
                progress.advance(task)

            try:
                next_arg = next(it)
            except StopIteration:
                continue

            new_fut = loop.run_in_executor(executor, func, next_arg)
            pending.add(new_fut)

    if progress is not None:
        progress.stop()
