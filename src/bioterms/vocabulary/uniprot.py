import gzip
import os
import re
import httpx
import networkx as nx

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix, ConceptRelationshipType, \
    ConceptStatus, ConceptType
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
    ConceptPrefix.OMIM,
    ConceptPrefix.ORDO,
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

_ID_LINE = re.compile(r'^ID\s+(\S+)\s+(Reviewed|Unreviewed);\s+(\d+)\s+AA\.')
_DE_NAME_LINE = re.compile(r'^DE\s+(?:RecName|SubName): Full=(.+?);?\s*$')
_DE_SYNONYM = re.compile(r'(?:Full|Short)=(.+?);?\s*$')
_GN_NAMES = re.compile(r'(?:Name|Synonyms|OrderedLocusNames|ORFNames)=([^;]+)')
_OX_TAXID = re.compile(r'NCBI_TaxID=(\d+)')
_DR_HGNC_LINE = re.compile(r'^DR\s+HGNC;\s*(HGNC:\d+);\s*([^.]+)\.')
_DR_GO_LINE = re.compile(r'^DR\s+GO;\s*GO:(\d+);\s*([CFP]):([^;]+);\s*([^.]+)\.')
_EVIDENCE_TAG = re.compile(r'\s*\{[^}]*\}')


async def download_vocabulary(download_client: httpx.AsyncClient = None):
    """
    Download the complete UniProtKB release (Swiss-Prot + TrEMBL flat files) from UniProt's
    FTP site. Large: TrEMBL alone is on the order of 100GB. Files are left gzip-compressed
    on disk; the loader streams and decompresses them on the fly (see _iter_dat_records).

    Existing paths are treated as complete, matching the CLI's explicit redownload policy.
    A new transfer still uses the shared resumable downloader, so transport failures within
    that invocation continue with HTTP Range rather than restarting a 100GB file.
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
    Parse one UniProtKB flat-file entry into identifier, naming, organism, evidence, and
    supported cross-reference fields. Citations, comments, features, and sequence content
    are deliberately not retained.
    :param lines: One entry's lines, as yielded by _iter_dat_records.
    :return: A dict of the extracted fields, or None if no accession line was found.
    """
    accessions = []
    reviewed = None
    entry_name = None
    sequence_length = None
    label = None
    synonyms = []
    gene_names = []
    organism_lines = []
    organism_tax_id = None
    protein_existence = None
    fragment = False
    hgnc_ids = []
    hgnc_symbols = []
    hgnc_references = []
    cross_references: dict[str, list[list[str]]] = {
        'Ensembl': [], 'Reactome': [], 'MIM': [], 'Orphanet': [],
    }

    for line in lines:
        if line.startswith('AC   '):
            accessions.extend(value.strip() for value in line[5:].split(';') if value.strip())
        elif reviewed is None and line.startswith('ID   '):
            match = _ID_LINE.match(line)
            if match:
                entry_name, review_status, length = match.groups()
                reviewed = review_status == 'Reviewed'
                sequence_length = int(length)
        elif line.startswith('DE   '):
            if label is None and (line.startswith('DE   RecName:') or
                                  line.startswith('DE   SubName:')):
                match = _DE_NAME_LINE.match(line)
                if match:
                    label = _EVIDENCE_TAG.sub('', match.group(1)).strip()
            name_match = _DE_SYNONYM.search(line)
            if name_match:
                name = _EVIDENCE_TAG.sub('', name_match.group(1)).strip()
                if name and name != label and name not in synonyms:
                    synonyms.append(name)
            if line.startswith('DE   Flags:') and 'Fragment' in line:
                fragment = True
        elif line.startswith('GN   '):
            for values in _GN_NAMES.findall(line):
                for value in values.split(','):
                    value = _EVIDENCE_TAG.sub('', value).strip()
                    if value and value not in gene_names:
                        gene_names.append(value)
        elif line.startswith('OS   '):
            organism_lines.append(line[5:].strip())
        elif organism_tax_id is None and line.startswith('OX   '):
            match = _OX_TAXID.search(line)
            if match:
                organism_tax_id = match.group(1)
        elif line.startswith('PE   '):
            protein_existence = line[5:].strip().rstrip(';')
            if ': ' in protein_existence:
                protein_existence = protein_existence.split(': ', 1)[1]
        elif line.startswith('DR   HGNC;'):
            match = _DR_HGNC_LINE.match(line)
            if match:
                hgnc_id = match.group(1).split(':', 1)[-1]
                symbol = match.group(2).strip()
                if hgnc_id not in hgnc_ids:
                    hgnc_ids.append(hgnc_id)
                if symbol not in hgnc_symbols:
                    hgnc_symbols.append(symbol)
                if (hgnc_id, symbol) not in hgnc_references:
                    hgnc_references.append((hgnc_id, symbol))
        elif line.startswith('DR   '):
            fields = [field.strip().rstrip('.') for field in line[5:].split(';')]
            database = fields[0]
            if database in cross_references:
                cross_references[database].append(fields[1:])

    if not accessions:
        return None

    organism_name = ' '.join(organism_lines).rstrip('.') or None
    synonyms = list(dict.fromkeys([*synonyms, *gene_names]))

    return {
        'accession': accessions[0],
        'secondary_accessions': accessions[1:],
        'reviewed': reviewed,
        'entry_name': entry_name,
        'sequence_length': sequence_length,
        'label': label,
        'synonyms': synonyms or None,
        'gene_names': gene_names or None,
        'organism_name': organism_name,
        'organism_tax_id': organism_tax_id,
        'protein_existence': protein_existence,
        'fragment': fragment,
        'hgnc_ids': hgnc_ids,
        'hgnc_symbols': hgnc_symbols,
        'hgnc_references': hgnc_references,
        'hgnc_symbol': hgnc_symbols[0] if hgnc_symbols else None,
        'cross_references': cross_references,
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
        synonyms=record.get('synonyms'),
        status=ConceptStatus.ACTIVE,
        reviewed=record['reviewed'],
        organismTaxId=record['organism_tax_id'],
        organismName=record['organism_name'],
        entryName=record.get('entry_name'),
        geneNames=record.get('gene_names'),
        proteinExistence=record.get('protein_existence'),
        sequenceLength=record.get('sequence_length'),
        fragment=record.get('fragment'),
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


def _build_symbol_annotations(record: dict) -> list[Annotation]:
    """Build every UniProt-to-gene-symbol link stated by an entry."""
    return [
        Annotation(
            prefixFrom=VOCABULARY_PREFIX, prefixTo=ConceptPrefix.HGNC_SYMBOL,
            conceptIdFrom=record['accession'], conceptIdTo=symbol,
            annotationType=AnnotationType.HAS_SYMBOL,
        )
        for symbol in record.get('hgnc_symbols', [])
    ]


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
            yield from _build_symbol_annotations(record)


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


def _iter_parsed_records(description: str):
    """Re-stream both compressed products and yield parsed entries one at a time."""
    for file_path in FILE_PATHS:
        full_path = os.path.join(CONFIG.data_dir, file_path)
        for lines in iter_progress(
            _iter_dat_records(full_path), description=f'{description} from {file_path}',
        ):
            record = _parse_dat_record(lines)
            if record is not None:
                yield record


def iter_hgnc_annotations():
    """Stream UniProt-published protein-to-HGNC cross-references."""
    source = AnnotationSource(
        'UniProtKB HGNC cross-reference', ConceptPrefix.UNIPROT, ConceptPrefix.HGNC,
    )
    for record in _iter_parsed_records('Processing UniProt HGNC annotations'):
        for hgnc_id, symbol in record['hgnc_references']:
            yield source.create(
                record['accession'], hgnc_id, AnnotationType.ANNOTATED_WITH,
                {'symbol': symbol},
            )


def iter_ensembl_annotations():
    """Stream human UniProt-to-Ensembl-protein cross-references."""
    source = AnnotationSource(
        'UniProtKB Ensembl cross-reference', ConceptPrefix.UNIPROT, ConceptPrefix.ENSEMBL,
    )
    for record in _iter_parsed_records('Processing UniProt Ensembl annotations'):
        if record['organism_tax_id'] != '9606':
            continue
        seen = set()
        for fields in record['cross_references']['Ensembl']:
            if len(fields) < 2 or not fields[1] or fields[1] == '-':
                continue
            transcript_id = fields[0].split('.', 1)[0]
            protein_id = fields[1].split('.', 1)[0]
            gene_id = fields[2].split('.', 1)[0] if len(fields) > 2 else None
            if protein_id in seen:
                continue
            seen.add(protein_id)
            properties = {'transcriptId': transcript_id}
            if gene_id:
                properties['geneId'] = gene_id
            yield source.create(
                record['accession'], protein_id, AnnotationType.EXACT, properties,
            )


def iter_reactome_annotations():
    """Stream pathway assignments published as UniProt Reactome cross-references."""
    source = AnnotationSource(
        'UniProtKB Reactome cross-reference', ConceptPrefix.UNIPROT, ConceptPrefix.REACTOME,
    )
    for record in _iter_parsed_records('Processing UniProt Reactome annotations'):
        seen = set()
        for fields in record['cross_references']['Reactome']:
            if not fields or not fields[0] or fields[0] in seen:
                continue
            reactome_id = fields[0]
            seen.add(reactome_id)
            properties = {'pathway': fields[1]} if len(fields) > 1 and fields[1] else None
            yield source.create(
                record['accession'], reactome_id, AnnotationType.ANNOTATED_WITH, properties,
            )


def iter_omim_annotations():
    """Stream UniProt-published links to OMIM gene and phenotype records."""
    source = AnnotationSource(
        'UniProtKB MIM cross-reference', ConceptPrefix.UNIPROT, ConceptPrefix.OMIM,
    )
    for record in _iter_parsed_records('Processing UniProt OMIM annotations'):
        seen = set()
        for fields in record['cross_references']['MIM']:
            if not fields or not fields[0] or fields[0] in seen:
                continue
            omim_id = fields[0]
            seen.add(omim_id)
            properties = {'recordType': fields[1]} if len(fields) > 1 and fields[1] else None
            yield source.create(
                record['accession'], omim_id, AnnotationType.ANNOTATED_WITH, properties,
            )


def iter_ordo_annotations():
    """Stream UniProt-published protein associations to Orphanet disease records."""
    source = AnnotationSource(
        'UniProtKB Orphanet cross-reference', ConceptPrefix.UNIPROT, ConceptPrefix.ORDO,
    )
    for record in _iter_parsed_records('Processing UniProt ORDO annotations'):
        seen = set()
        for fields in record['cross_references']['Orphanet']:
            if not fields or not fields[0] or fields[0] in seen:
                continue
            ordo_id = fields[0]
            seen.add(ordo_id)
            properties = {'disease': fields[1]} if len(fields) > 1 and fields[1] else None
            yield source.create(
                record['accession'], ordo_id, AnnotationType.ANNOTATED_WITH, properties,
            )


async def _flush_batch(concepts: list[UniProtConcept],
                       annotations: list[Annotation],
                       replacements: list[tuple[str, str]],
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
        for old_id, current_id in replacements:
            batch_graph.add_edge(
                old_id, current_id, key=ConceptRelationshipType.REPLACED_BY.value,
                label=ConceptRelationshipType.REPLACED_BY,
            )
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
        for old_id, current_id in replacements:
            batch_graph.add_edge(
                old_id, current_id, key=ConceptRelationshipType.REPLACED_BY.value,
                label=ConceptRelationshipType.REPLACED_BY,
            )
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
        replacements: list[tuple[str, str]] = []

        for lines in iter_progress(_iter_dat_records(full_path), description=f'Processing {file_path}'):
            record = _parse_dat_record(lines)
            if record is None:
                continue

            concept = _build_uniprot_concept(record)
            concepts.append(concept)
            for secondary_accession in record['secondary_accessions']:
                # A secondary accession only needs enough metadata to identify the entry and
                # reach its primary node. Avoid duplicating potentially large synonym/gene-name
                # arrays across every historical identifier.
                concepts.append(UniProtConcept(
                    prefix=VOCABULARY_PREFIX, conceptId=secondary_accession,
                    conceptTypes=[ConceptType.PROTEIN], label=concept.label,
                    status=ConceptStatus.DEPRECATED, reviewed=concept.reviewed,
                    organismTaxId=concept.organism_tax_id,
                    organismName=concept.organism_name,
                ))
                replacements.append((secondary_accession, record['accession']))
            if load_annotations:
                annotations.extend(_build_symbol_annotations(record))

            if len(concepts) >= _BATCH_SIZE:
                await _flush_batch(
                    concepts, annotations, replacements,
                    is_first_batch=(total_batches == 0),
                    doc_db=doc_db, graph_db=graph_db, offline=offline,
                    build_search_index=build_search_index,
                    load_annotations=load_annotations,
                )
                total_loaded += len(concepts)
                total_batches += 1
                verbose_print(f'Loaded {total_loaded} UniProt identifiers so far...')
                concepts = []
                annotations = []
                replacements = []

        if concepts:
            await _flush_batch(
                concepts, annotations, replacements,
                is_first_batch=(total_batches == 0),
                doc_db=doc_db, graph_db=graph_db, offline=offline,
                build_search_index=build_search_index,
                load_annotations=load_annotations,
            )
            total_loaded += len(concepts)
            total_batches += 1

    verbose_print(
        f'UniProt loading complete: {total_loaded} identifiers in {total_batches} batches.'
    )
