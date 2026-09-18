import gzip
import os
import re
import httpx
import networkx as nx

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus, ConceptType, AnnotationType
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, download_file, iter_progress, verbose_print
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.model.annotation import Annotation
from bioterms.annotation.utils import AnnotationSource
from bioterms.model.concept import UniProtConcept
from .utils import write_concepts_to_file, write_graph_to_file, \
    write_annotations_to_file


VOCABULARY_NAME = 'UniProtKB'
VOCABULARY_PREFIX = ConceptPrefix.UNIPROT
ANNOTATIONS = [
    ConceptPrefix.ENSEMBL,
    ConceptPrefix.GO,
    ConceptPrefix.HGNC,
    ConceptPrefix.HGNC_SYMBOL,
    ConceptPrefix.REACTOME,
]
SIMILARITY_METHODS = []

FILE_PATHS = [
    'uniprot/uniprot_sprot.dat.gz',
    'uniprot/uniprot_trembl.dat.gz',
]
TIMESTAMP_FILE = 'uniprot/.timestamp'
CONCEPT_CLASS = UniProtConcept

_BATCH_SIZE = 100000

_ID_LINE = re.compile(r'^ID\s+\S+\s+(Reviewed|Unreviewed);')
_DE_NAME_LINE = re.compile(r'^DE\s+(?:RecName|SubName): Full=(.+?);?\s*$')
_GN_NAME = re.compile(r'Name=([^;{]+)')
_OX_TAXID = re.compile(r'NCBI_TaxID=(\d+)')
_DR_HGNC_LINE = re.compile(r'^DR\s+HGNC;\s*(HGNC:\d+);\s*([^.]+)\.')
_DR_GO_LINE = re.compile(r'^DR\s+GO;\s*GO:(\d+);\s*([CFP]):([^;]+);\s*([^.]+)\.')
_EVIDENCE_TAG = re.compile(r'\s*\{[^}]*\}')


async def download_vocabulary(download_client: httpx.AsyncClient = None):
    """
    Download the complete UniProtKB release (Swiss-Prot + TrEMBL flat files) from UniProt's
    FTP site. Large: TrEMBL alone is on the order of 100GB. Files are left gzip-compressed
    on disk; the loader streams and decompresses them on the fly (see _iter_dat_records).

    Deliberately does NOT skip a file just because something already exists at its path --
    at this scale, a previous attempt failing partway (e.g. a transient network timeout
    hours into a 100GB download) is expected, not exceptional, and existence alone can't
    tell a complete file from a truncated one. Every call goes through download_file(),
    which resumes an incomplete file via an HTTP Range request and cheaply no-ops (one
    small request, no re-transfer) when a file is already complete.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    for file_path in FILE_PATHS:
        full_path = os.path.join(CONFIG.data_dir, file_path)
        if os.path.exists(full_path):
            continue

        filename = os.path.basename(file_path)
        verbose_print(f'Downloading {filename} (this file may be very large)...')
        await download_file(
            url=(
                'https://ftp.uniprot.org/pub/databases/uniprot/current_release/'
                f'knowledgebase/complete/{filename}'
            ),
            file_path=file_path,
            download_client=download_client,
        )


def _iter_dat_records(gz_path: str):
    """
    Stream one UniProtKB flat-file (.dat.gz) release file, yielding the raw lines of each
    entry (from its 'ID' line up to, but not including, its '//' terminator). The gzip
    stream is decompressed on the fly (never written to disk decompressed), and sequence
    data (the 'SQ' header and the sequence lines that follow it) is dropped while reading --
    this vocabulary has no use for it, and it is the largest field per entry.
    :param gz_path: Path to the .dat.gz release file.
    :return: An iterator of per-entry line lists.
    """
    with gzip.open(gz_path, 'rt', encoding='utf-8', errors='replace') as f:
        record_lines: list[str] = []
        in_sequence = False

        for line in f:
            if line.startswith('//'):
                if record_lines:
                    yield record_lines
                record_lines = []
                in_sequence = False
                continue
            if line.startswith('SQ'):
                in_sequence = True
                continue
            if in_sequence:
                continue
            record_lines.append(line)


def _parse_dat_record(lines: list[str]) -> dict | None:
    """
    Parse one UniProtKB flat-file entry's lines into the minimal fields this vocabulary
    needs: primary accession, reviewed status, protein name, organism, and HGNC symbol
    cross-reference (when present). Everything else in the entry (citations, comments,
    features, sequence) is deliberately not extracted.
    :param lines: One entry's lines, as yielded by _iter_dat_records.
    :return: A dict of the extracted fields, or None if no accession line was found.
    """
    accession = None
    reviewed = None
    label = None
    organism_name = None
    organism_tax_id = None
    hgnc_symbol = None

    for line in lines:
        if accession is None and line.startswith('AC   '):
            accession = line[5:].strip().split(';')[0].strip()
        elif reviewed is None and line.startswith('ID   '):
            match = _ID_LINE.match(line)
            if match:
                reviewed = match.group(1) == 'Reviewed'
        elif label is None and (line.startswith('DE   RecName:') or line.startswith('DE   SubName:')):
            match = _DE_NAME_LINE.match(line)
            if match:
                label = _EVIDENCE_TAG.sub('', match.group(1)).strip()
        elif organism_name is None and line.startswith('OS   '):
            organism_name = line[5:].strip().rstrip('.')
        elif organism_tax_id is None and line.startswith('OX   '):
            match = _OX_TAXID.search(line)
            if match:
                organism_tax_id = match.group(1)
        elif hgnc_symbol is None and line.startswith('DR   HGNC;'):
            match = _DR_HGNC_LINE.match(line)
            if match:
                hgnc_symbol = match.group(2).strip()

    if accession is None:
        return None

    return {
        'accession': accession,
        'reviewed': reviewed,
        'label': label,
        'organism_name': organism_name,
        'organism_tax_id': organism_tax_id,
        'hgnc_symbol': hgnc_symbol,
    }


def _build_uniprot_concept(record: dict) -> UniProtConcept:
    """
    Build a UniProtConcept from one parsed flat-file record.
    :param record: A dict as returned by _parse_dat_record.
    :return: The built UniProtConcept instance.
    """
    return UniProtConcept(
        prefix=VOCABULARY_PREFIX,
        conceptId=record['accession'],
        conceptTypes=[ConceptType.PROTEIN],
        label=record['label'],
        status=ConceptStatus.ACTIVE,
        reviewed=record['reviewed'],
        organismTaxId=record['organism_tax_id'],
        organismName=record['organism_name'],
    )


def _build_symbol_annotation(record: dict) -> Annotation | None:
    """
    Build the has_symbol Annotation for one parsed flat-file record, if it has an HGNC
    cross-reference.
    :param record: A dict as returned by _parse_dat_record.
    :return: The built Annotation, or None if the record has no HGNC cross-reference.
    """
    if not record['hgnc_symbol']:
        return None

    return Annotation(
        prefixFrom=VOCABULARY_PREFIX,
        prefixTo=ConceptPrefix.HGNC_SYMBOL,
        conceptIdFrom=record['accession'],
        conceptIdTo=record['hgnc_symbol'],
        annotationType=AnnotationType.HAS_SYMBOL,
    )


def iter_gene_annotations():
    """Stream UniProt to HGNC-symbol annotations from the downloaded release files."""
    for file_path in FILE_PATHS:
        full_path = os.path.join(CONFIG.data_dir, file_path)
        for lines in iter_progress(
            _iter_dat_records(full_path),
            description=f'Processing UniProt gene annotations from {file_path}',
        ):
            record = _parse_dat_record(lines)
            if record is None:
                continue
            annotation = _build_symbol_annotation(record)
            if annotation is not None:
                yield annotation


def iter_go_annotations():
    """Stream UniProt-published protein-to-GO annotations from the release flat files."""
    source = AnnotationSource(
        'UniProtKB GO cross-reference', ConceptPrefix.UNIPROT, ConceptPrefix.GO,
    )
    aspect_names = {
        'C': 'cellular_component',
        'F': 'molecular_function',
        'P': 'biological_process',
    }
    for file_path in FILE_PATHS:
        full_path = os.path.join(CONFIG.data_dir, file_path)
        for lines in iter_progress(
            _iter_dat_records(full_path),
            description=f'Processing UniProt GO annotations from {file_path}',
        ):
            accession = None
            mappings: dict[str, dict[str, list[str] | str]] = {}
            for line in lines:
                if accession is None and line.startswith('AC   '):
                    accession = line[5:].strip().split(';')[0].strip()
                elif line.startswith('DR   GO;'):
                    match = _DR_GO_LINE.match(line)
                    if match:
                        go_id, aspect, term, evidence = match.groups()
                        mapping = mappings.setdefault(go_id, {
                            'aspect': aspect_names[aspect], 'term': term.strip(), 'evidence': [],
                        })
                        if evidence.strip() not in mapping['evidence']:
                            mapping['evidence'].append(evidence.strip())
            if accession is None:
                continue
            for go_id, mapping in mappings.items():
                yield source.create(
                    publisher_concept_id=accession,
                    other_concept_id=go_id,
                    annotation_type=AnnotationType.ANNOTATED_WITH,
                    properties={
                        'aspect': mapping['aspect'],
                        'term': mapping['term'],
                        'evidence': ';'.join(mapping['evidence']),
                    },
                )


async def _flush_batch(concepts: list[UniProtConcept],
                       annotations: list[Annotation],
                       *,
                       is_first_batch: bool,
                       doc_db: DocumentDatabase = None,
                       graph_db: GraphDatabase = None,
                       offline: bool,
                       build_search_index: bool = True,
                       load_annotations: bool = True,
                       ):
    """
    Save one batch of concepts/annotations, either directly to the primary databases or as
    an offline dump. Successive calls with is_first_batch=False append to the same offline
    dump files rather than overwriting them.
    """
    if not offline:
        await doc_db.save_terms(terms=concepts, no_upsert=True)

        batch_graph = nx.MultiDiGraph()
        for concept in concepts:
            batch_graph.add_node(concept.concept_id)
        await graph_db.save_vocabulary_graph(concepts=concepts, graph=batch_graph)

        if load_annotations:
            await graph_db.save_annotations(annotations)
    else:
        await write_concepts_to_file(
            prefix=VOCABULARY_PREFIX,
            concepts=concepts,
            overwrite=is_first_batch,
            build_search_index=build_search_index,
        )

        batch_graph = nx.MultiDiGraph()
        for concept in concepts:
            batch_graph.add_node(concept.concept_id)
        await write_graph_to_file(
            prefix=VOCABULARY_PREFIX,
            concepts=concepts,
            vocabulary_graph=batch_graph,
            overwrite=is_first_batch,
        )

        if load_annotations:
            await write_annotations_to_file(
                prefix_from=VOCABULARY_PREFIX,
                prefix_to=ConceptPrefix.HGNC_SYMBOL,
                annotations=annotations,
                overwrite=is_first_batch,
            )


async def load_vocabulary_from_file(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    offline: bool = False,
                                    build_search_index: bool = True,
                                    load_annotations: bool = True,
                                    ):
    """
    Load the complete UniProtKB vocabulary from the downloaded flat files into the primary
    databases, streaming and batching throughout so peak memory stays bounded regardless of
    total release size.
    :param doc_db: Optional DocumentDatabase instance to use.
    :param graph_db: Optional GraphDatabase instance to use.
    :param offline: Whether to operate in offline mode and write to data files only.
    """
    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('UniProt release files not found')

    if not offline:
        if doc_db is None:
            doc_db = await get_active_doc_db()
        if graph_db is None:
            graph_db = get_active_graph_db()

        load_annotations = load_annotations and (
            await graph_db.count_terms(ConceptPrefix.HGNC_SYMBOL) > 0
        )

    total_loaded = 0
    total_batches = 0

    for file_path in FILE_PATHS:
        full_path = os.path.join(CONFIG.data_dir, file_path)
        verbose_print(f'Streaming UniProt entries from {file_path}...')

        concepts: list[UniProtConcept] = []
        annotations: list[Annotation] = []

        for lines in iter_progress(_iter_dat_records(full_path), description=f'Processing {file_path}'):
            record = _parse_dat_record(lines)
            if record is None:
                continue

            concepts.append(_build_uniprot_concept(record))
            if load_annotations:
                annotation = _build_symbol_annotation(record)
                if annotation is not None:
                    annotations.append(annotation)

            if len(concepts) >= _BATCH_SIZE:
                await _flush_batch(
                    concepts, annotations,
                    is_first_batch=(total_batches == 0),
                    doc_db=doc_db, graph_db=graph_db, offline=offline,
                    build_search_index=build_search_index,
                    load_annotations=load_annotations,
                )
                total_loaded += len(concepts)
                total_batches += 1
                verbose_print(f'Loaded {total_loaded} UniProt entries so far...')
                concepts = []
                annotations = []

        if concepts:
            await _flush_batch(
                concepts, annotations,
                is_first_batch=(total_batches == 0),
                doc_db=doc_db, graph_db=graph_db, offline=offline,
                build_search_index=build_search_index,
                load_annotations=load_annotations,
            )
            total_loaded += len(concepts)
            total_batches += 1

    verbose_print(f'UniProt loading complete: {total_loaded} entries in {total_batches} batches.')
