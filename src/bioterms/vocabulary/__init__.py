import importlib

from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.utils import check_files_exist


ALL_VOCABULARIES = {
    ConceptPrefix.HPO: 'hpo',
}


async def download_vocabulary(prefix: ConceptPrefix,
                              redownload: bool = False
                              ):
    """
    Download the vocabulary specified by the prefix.
    :param prefix: The prefix of the vocabulary to download.
    :param redownload: Whether to redownload the files even if they exist.
    """
    vocabulary_module_name = ALL_VOCABULARIES.get(prefix)
    if not vocabulary_module_name:
        raise ValueError(f'Vocabulary with prefix {prefix} not found.')

    vocabulary_module = importlib.import_module(f'bioterms.vocabulary.{vocabulary_module_name}')

    if redownload:
        vocabulary_module.delete_vocabulary_files()

    await vocabulary_module.download_vocabulary()


async def load_vocabulary(prefix: ConceptPrefix,
                          drop_existing: bool = True
                          ):
    """
    Load the vocabulary specified by the prefix.
    :param prefix: The prefix of the vocabulary to load.
    :param drop_existing: Whether to drop existing data before loading.
    """
    vocabulary_module_name = ALL_VOCABULARIES.get(prefix)
    if not vocabulary_module_name:
        raise ValueError(f'Vocabulary with prefix {prefix} not found.')

    vocabulary_module = importlib.import_module(f'bioterms.vocabulary.{vocabulary_module_name}')

    if not check_files_exist(vocabulary_module.FILE_PATHS):
        raise ValueError(f'Vocabulary files for {prefix} not found. Are they downloaded?')

    if drop_existing:
        # Drop existing data before loading
        await vocabulary_module.delete_vocabulary_data()

    # Create indexes before loading data
    await vocabulary_module.create_indexes()
    await vocabulary_module.load_vocabulary_from_file()
