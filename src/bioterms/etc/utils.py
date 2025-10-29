import os

from .consts import CONFIG


def check_files_exist(files: list[str]) -> bool:
    for file_name in files:
        if not os.path.exists(os.path.join(CONFIG.data_dir, file_name)):
            return False

    return True


def ensure_data_directory():
    if not os.path.exists(CONFIG.data_dir):
        os.makedirs(CONFIG.data_dir, exist_ok=True)
