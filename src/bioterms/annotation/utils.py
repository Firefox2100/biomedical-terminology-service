import os
import re
import csv
from dataclasses import dataclass
from typing import Mapping
from urllib.parse import quote

import aiofiles
import httpx

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import VocabularyNotLoaded, FilesNotFound
from bioterms.etc.utils import check_files_exist, discover_latest_numbered_release, download_file, \
    ensure_data_directory, extract_file_from_gzip, verbose_print
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.model.annotation import Annotation


_HPOA_FILE_PATH = 'hpo/phenotype.hpoa'
_HPOA_SOURCE = 'phenotype.hpoa'


@dataclass(frozen=True)
class AnnotationSource:
    """A published annotation dataset and its publisher-defined direction."""

    name: str
    publisher_prefix: ConceptPrefix | str
    other_prefix: ConceptPrefix | str

    def create(self,
               publisher_concept_id: str,
               other_concept_id: str,
               annotation_type: AnnotationType = AnnotationType.ANNOTATED_WITH,
               properties: Mapping[str, str] | None = None,
               ) -> Annotation:
        """Create an annotation with immutable provenance and publisher-first direction."""
        annotation_properties = dict(properties or {})
        existing_source = annotation_properties.get('source')
        if existing_source is not None and existing_source != self.name:
            raise ValueError(
                f'Annotation source is managed by AnnotationSource: '
                f'{existing_source!r} != {self.name!r}'
            )
        annotation_properties['source'] = self.name
        return Annotation(
            prefixFrom=self.publisher_prefix,
            conceptIdFrom=publisher_concept_id,
            prefixTo=self.other_prefix,
            conceptIdTo=other_concept_id,
            annotationType=annotation_type,
            properties=annotation_properties,
        )


async def download_hpoa(download_client: httpx.AsyncClient = None):
    """Download the disease-to-phenotype annotations published in the HPO release."""
    if check_files_exist([_HPOA_FILE_PATH]):
        return

    ensure_data_directory()
    await download_file(
        url=(
            'https://github.com/obophenotype/human-phenotype-ontology/'
            'releases/latest/download/phenotype.hpoa'
        ),
        file_path=_HPOA_FILE_PATH,
        download_client=download_client,
    )


def hpoa_file_path() -> str:
    """Return the shared relative path of the HPO annotation release."""
    return _HPOA_FILE_PATH


def load_hpoa_annotations(disease_namespace: str,
                          disease_prefix: ConceptPrefix,
                          ) -> list[Annotation]:
    """
    Load one disease namespace from ``phenotype.hpoa``.

    HPO publishes the combined file, so HPO is deliberately the source side even for rows
    contributed upstream by another organisation. This keeps the release distinct from datasets
    published in the opposite direction, such as HOOM (ORDO -> HPO).
    """
    source = AnnotationSource(_HPOA_SOURCE, ConceptPrefix.HPO, disease_prefix)
    path = os.path.join(CONFIG.data_dir, _HPOA_FILE_PATH)
    records: dict[tuple[str, str], dict[str, list[str]]] = {}

    accepted_namespaces = {disease_namespace.upper()}
    if disease_namespace.upper() == 'OMIM':
        # Older releases and the format documentation use MIM; current releases use OMIM.
        accepted_namespaces.add('MIM')

    with open(path, encoding='utf-8', newline='') as stream:
        rows = csv.DictReader(
            (line for line in stream if not line.startswith('#')),
            delimiter='\t',
        )
        for row in rows:
            namespace, separator, disease_id = row['database_id'].partition(':')
            if separator and namespace.upper() in accepted_namespaces:
                hpo_id = row['hpo_id'].split(':', 1)[-1]
                property_values = records.setdefault((hpo_id, disease_id), {})
                for key in (
                    'qualifier', 'reference', 'evidence', 'onset', 'frequency', 'sex',
                    'modifier', 'aspect', 'biocuration',
                ):
                    value = row.get(key)
                    values = property_values.setdefault(key, [])
                    if value and value not in values:
                        values.append(value)

    return [
        source.create(
            publisher_concept_id=hpo_id,
            other_concept_id=disease_id,
            properties={
                key: ';'.join(values)
                for key, values in property_values.items()
                if values
            },
        )
        for (hpo_id, disease_id), property_values in records.items()
    ]


def load_hpoa_negated_pairs() -> set[tuple[str, str]]:
    """Return ``(disease CURIE, HPO CURIE)`` pairs explicitly qualified as absent."""
    pairs = set()
    path = os.path.join(CONFIG.data_dir, _HPOA_FILE_PATH)
    with open(path, encoding='utf-8', newline='') as stream:
        rows = csv.DictReader(
            (line for line in stream if not line.startswith('#')),
            delimiter='\t',
        )
        for row in rows:
            if row.get('qualifier') == 'NOT':
                pairs.add((row['database_id'], row['hpo_id']))
    return pairs


def is_gene_annotation_prefix(prefix: ConceptPrefix | str) -> bool:
    """Return whether a target belongs to the provenance-excluded gene/symbol boundary."""
    value = prefix.value if isinstance(prefix, ConceptPrefix) else prefix
    return value.lower() in {
        ConceptPrefix.HGNC.value,
        ConceptPrefix.HGNC_SYMBOL.value,
    }


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


async def assert_vocabulary_loaded(prefix_1: ConceptPrefix,
                                   prefix_2: ConceptPrefix,
                                   graph_db: GraphDatabase = None):
    """
    Check if the annotation vocabulary graph is loaded in the primary graph database.
    :param prefix_1: The first vocabulary prefix to check.
    :param prefix_2: The second vocabulary prefix to check.
    :param graph_db: Optional GraphDatabase instance to use.
    :raises VocabularyNotLoaded: If any of the vocabularies is not loaded.
    """
    if graph_db is None:
        graph_db = get_active_graph_db()

    prefix_1_count = await graph_db.count_terms(prefix_1)
    if prefix_1_count == 0:
        raise VocabularyNotLoaded(f'Vocabulary with prefix {prefix_1} is not loaded in the graph database.')

    prefix_2_count = await graph_db.count_terms(prefix_2)
    if prefix_2_count == 0:
        raise VocabularyNotLoaded(f'Vocabulary with prefix {prefix_2} is not loaded in the graph database.')


async def assert_pre_requisite(annotation_name: str,
                               prefix_1: ConceptPrefix,
                               prefix_2: ConceptPrefix,
                               file_paths: list[str],
                               graph_db: GraphDatabase = None,
                               ):
    verbose_print(f'Loading annotation: {annotation_name}...')

    await assert_vocabulary_loaded(
        prefix_1=prefix_1,
        prefix_2=prefix_2,
        graph_db=graph_db,
    )

    verbose_print(f'Confirmed {prefix_1} and {prefix_2} vocabularies are loaded.')

    annotation_count = await graph_db.count_annotations(
        prefix_1=prefix_1,
        prefix_2=prefix_2,
    )

    if annotation_count > 0:
        verbose_print(f'Annotation {annotation_name} already loaded with {annotation_count} entries, skipping.')
        return  # Annotations already exist, skip loading

    if not check_files_exist(file_paths):
        raise FilesNotFound(f'{annotation_name} file not found. Please download it first.')
