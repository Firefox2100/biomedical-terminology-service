import os
import io
import zipfile
import uuid
import tempfile
import fnmatch
import gzip
from pathlib import Path
import aiofiles
import aiofiles.os
import httpx
import pandas as pd
from sentence_transformers import SentenceTransformer

from .consts import CONFIG, DOWNLOAD_CLIENT, QUERY_CLIENT
from .errors import FilesNotFound


_transformer: SentenceTransformer | None = None


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
            async for chunk in response.aiter_bytes():
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
