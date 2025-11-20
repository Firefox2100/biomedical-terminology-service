import os
import anyio
import httpx

from .consts import CONFIG, DOWNLOAD_CLIENT


def check_files_exist(files: list[str]) -> bool:
    for file_name in files:
        if not os.path.exists(os.path.join(CONFIG.data_dir, file_name)):
            return False

    return True


def ensure_data_directory():
    if not os.path.exists(CONFIG.data_dir):
        os.makedirs(CONFIG.data_dir, exist_ok=True)


async def download_file(url: str,
                        file_path: str,
                        headers: dict[str, str] = None,
                        download_client: httpx.AsyncClient = None,
                        ):
    if download_client is None:
        download_client = DOWNLOAD_CLIENT

    async with download_client.stream(
            'GET',
            url,
            follow_redirects=True,
            headers=headers,
    ) as response:
        response.raise_for_status()

        owl_file_path = os.path.join(CONFIG.data_dir, file_path)
        os.makedirs(os.path.dirname(owl_file_path), exist_ok=True)

        async with anyio.open_file(owl_file_path, 'wb') as owl_file:
            async for chunk in response.aiter_bytes():
                await owl_file.write(chunk)
