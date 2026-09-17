"""Shared acquisition helpers for independently managed Ensembl annotations."""

import os
import re
from urllib.parse import quote

import aiofiles
import httpx

from bioterms.etc.consts import CONFIG
from bioterms.etc.utils import discover_latest_numbered_release, download_file, \
    ensure_data_directory, extract_file_from_gzip


async def download_current_ensembl_tsv(filename_suffix: str,
                                       output_path: str,
                                       download_client: httpx.AsyncClient = None,
                                       ):
    """Discover and download one current human Ensembl release TSV product."""
    release, release_url = await discover_latest_numbered_release(
        'https://ftp.ensembl.org/pub/', download_client,
    )
    directory_url = f'{release_url}tsv/homo_sapiens/'
    close_client = download_client is None
    client = download_client or httpx.AsyncClient(follow_redirects=True)
    try:
        response = await client.get(directory_url)
        response.raise_for_status()
        pattern = (
            rf'href="(Homo_sapiens\.GRCh38\.{release}\.'
            rf'{re.escape(filename_suffix)}\.tsv\.gz)"'
        )
        filenames = re.findall(pattern, response.text)
    finally:
        if close_client:
            await client.aclose()

    if not filenames:
        raise ValueError(f'Could not discover current Ensembl {filename_suffix} TSV release.')

    ensure_data_directory()
    gzip_relative_path = f'{output_path}.gz'
    gzip_path = os.path.join(CONFIG.data_dir, gzip_relative_path)
    try:
        await download_file(
            url=f'{directory_url}{filenames[0]}',
            file_path=gzip_relative_path,
            download_client=download_client,
        )
        await extract_file_from_gzip(
            gzip_path=gzip_path,
            output_path=os.path.join(CONFIG.data_dir, output_path),
        )
    finally:
        try:
            os.remove(gzip_path)
        except FileNotFoundError:
            pass


async def download_biomart_tsv(attributes: list[str],
                               output_path: str,
                               download_client: httpx.AsyncClient = None,
                               ):
    """Download a current human Ensembl BioMart projection as a headered TSV file."""
    attribute_xml = ''.join(f'<Attribute name="{name}" />' for name in attributes)
    query = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<!DOCTYPE Query>'
        '<Query virtualSchemaName="default" formatter="TSV" header="1" uniqueRows="1" '
        'datasetConfigVersion="0.6">'
        '<Dataset name="hsapiens_gene_ensembl" interface="default">'
        f'{attribute_xml}</Dataset></Query>'
    )
    url = f'https://www.ensembl.org/biomart/martservice?query={quote(query)}'
    close_client = download_client is None
    client = download_client or httpx.AsyncClient(follow_redirects=True, timeout=None)
    try:
        response = await client.get(url)
        response.raise_for_status()
        text = response.text
    finally:
        if close_client:
            await client.aclose()

    expected_header = '\t'.join(attributes)
    if not text or text.startswith('Query ERROR') or '<html' in text[:200].lower():
        raise ValueError('Ensembl BioMart did not return a mapping table.')

    ensure_data_directory()
    full_path = os.path.join(CONFIG.data_dir, output_path)
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    async with aiofiles.open(full_path, 'w') as output_file:
        await output_file.write(expected_header + '\n')
        # BioMart uses display labels for its own header. Store a stable internal header instead.
        await output_file.write(text.split('\n', 1)[1] if '\n' in text else '')
