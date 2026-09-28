import gzip
import os
import re
from dataclasses import dataclass, field
import httpx

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix, ConceptRelationshipType, \
    ConceptStatus, ConceptType
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, download_file, iter_progress, verbose_print
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.model.annotation import Annotation
from bioterms.annotation.utils import AnnotationSource
from bioterms.model.concept import UniProtConcept
from bioterms.model.edge_buffer import EdgeBuffer
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
# Name captures run to end of line; `_field_value` drops the trailing `;` without backtracking.
_DE_NAME_LINE = re.compile(r'^DE\s++(?:RecName|SubName): Full=(.+)')
_DE_SYNONYM = re.compile(r'(?:Full|Short)=(.+)')
_GN_NAMES = re.compile(r'(?:Name|Synonyms|OrderedLocusNames|ORFNames)=([^;]+)')
_OX_TAXID = re.compile(r'NCBI_TaxID=(\d+)')
_DR_HGNC_LINE = re.compile(r'^DR\s++HGNC;\s*+(HGNC:\d++);\s*+([^.]++)\.')
_DR_GO_LINE = re.compile(r'^DR\s++GO;\s*+GO:(\d++);\s*+([CFP]):([^;]++);\s*+([^.]++)\.')
# The lookbehind starts matches only at the head of a whitespace run, keeping `sub` linear.
_EVIDENCE_TAG = re.compile(r'(?<!\s)\s*+\{[^}]*+\}')


def _field_value(raw: str) -> str:
    """Strip the trailing whitespace and optional `;` terminator from a DE field value."""
    return raw.rstrip().removesuffix(';')


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


def _append_unique(values: list, value) -> None:
    """Append value to values unless it is already present, preserving first-seen order."""
    if value not in values:
        values.append(value)


@dataclass
class _DatRecord:
    """Accumulates the supported fields of one UniProtKB flat-file entry, line by line."""
    accessions: list[str] = field(default_factory=list)
    reviewed: bool | None = None
    entry_name: str | None = None
    sequence_length: int | None = None
    label: str | None = None
    synonyms: list[str] = field(default_factory=list)
    gene_names: list[str] = field(default_factory=list)
    organism_lines: list[str] = field(default_factory=list)
    organism_tax_id: str | None = None
    protein_existence: str | None = None
    fragment: bool = False
    hgnc_ids: list[str] = field(default_factory=list)
    hgnc_symbols: list[str] = field(default_factory=list)
    hgnc_references: list[tuple[str, str]] = field(default_factory=list)
    cross_references: dict[str, list[list[str]]] = field(default_factory=lambda: {
        'Ensembl': [], 'Reactome': [], 'MIM': [], 'Orphanet': [],
    })

    def parse_line(self, line: str) -> None:
        """Dispatch one entry line to the handler for its two-letter line code."""
        handler = self._HANDLERS.get(line[:5])
        if handler is not None:
            handler(self, line)

    def _parse_ac(self, line: str) -> None:
        self.accessions.extend(value.strip() for value in line[5:].split(';') if value.strip())

    def _parse_id(self, line: str) -> None:
        if self.reviewed is not None:
            return
        match = _ID_LINE.match(line)
        if match:
            self.entry_name, review_status, length = match.groups()
            self.reviewed = review_status == 'Reviewed'
            self.sequence_length = int(length)

    def _parse_de(self, line: str) -> None:
        if self.label is None and line.startswith(('DE   RecName:', 'DE   SubName:')):
            match = _DE_NAME_LINE.match(line)
            if match:
                self.label = _EVIDENCE_TAG.sub('', _field_value(match.group(1))).strip()
        name_match = _DE_SYNONYM.search(line)
        if name_match:
            name = _EVIDENCE_TAG.sub('', _field_value(name_match.group(1))).strip()
            if name and name != self.label:
                _append_unique(self.synonyms, name)
        if line.startswith('DE   Flags:') and 'Fragment' in line:
            self.fragment = True

    def _parse_gn(self, line: str) -> None:
        for values in _GN_NAMES.findall(line):
            for value in values.split(','):
                value = _EVIDENCE_TAG.sub('', value).strip()
                if value:
                    _append_unique(self.gene_names, value)

    def _parse_os(self, line: str) -> None:
        self.organism_lines.append(line[5:].strip())

    def _parse_ox(self, line: str) -> None:
        if self.organism_tax_id is not None:
            return
        match = _OX_TAXID.search(line)
        if match:
            self.organism_tax_id = match.group(1)

    def _parse_pe(self, line: str) -> None:
        protein_existence = line[5:].strip().rstrip(';')
        if ': ' in protein_existence:
            protein_existence = protein_existence.split(': ', 1)[1]
        self.protein_existence = protein_existence

    def _parse_dr(self, line: str) -> None:
        if line.startswith('DR   HGNC;'):
            self._parse_dr_hgnc(line)
            return
        fields = [field_value.strip().rstrip('.') for field_value in line[5:].split(';')]
        if fields[0] in self.cross_references:
            self.cross_references[fields[0]].append(fields[1:])

    def _parse_dr_hgnc(self, line: str) -> None:
        match = _DR_HGNC_LINE.match(line)
        if not match:
            return
        hgnc_id = match.group(1).split(':', 1)[-1]
        symbol = match.group(2).strip()
        _append_unique(self.hgnc_ids, hgnc_id)
        _append_unique(self.hgnc_symbols, symbol)
        _append_unique(self.hgnc_references, (hgnc_id, symbol))

    _HANDLERS = {
        'AC   ': _parse_ac,
        'ID   ': _parse_id,
        'DE   ': _parse_de,
        'GN   ': _parse_gn,
        'OS   ': _parse_os,
        'OX   ': _parse_ox,
        'PE   ': _parse_pe,
        'DR   ': _parse_dr,
    }

    def to_dict(self) -> dict:
        """Render the accumulated fields in the shape returned by _parse_dat_record."""
        return {
            'accession': self.accessions[0],
            'secondary_accessions': self.accessions[1:],
            'reviewed': self.reviewed,
            'entry_name': self.entry_name,
            'sequence_length': self.sequence_length,
            'label': self.label,
            'synonyms': list(dict.fromkeys([*self.synonyms, *self.gene_names])) or None,
            'gene_names': self.gene_names or None,
            'organism_name': ' '.join(self.organism_lines).rstrip('.') or None,
            'organism_tax_id': self.organism_tax_id,
            'protein_existence': self.protein_existence,
            'fragment': self.fragment,
            'hgnc_ids': self.hgnc_ids,
            'hgnc_symbols': self.hgnc_symbols,
            'hgnc_references': self.hgnc_references,
            'hgnc_symbol': self.hgnc_symbols[0] if self.hgnc_symbols else None,
            'cross_references': self.cross_references,
        }


def _parse_dat_record(lines: list[str]) -> dict | None:
    """
    Parse one UniProtKB flat-file entry into identifier, naming, organism, evidence, and
    supported cross-reference fields. Citations, comments, features, and sequence content
    are deliberately not retained.
    :param lines: One entry's lines, as yielded by _iter_dat_records.
    :return: A dict of the extracted fields, or None if no accession line was found.
    """
    record = _DatRecord()
    for line in lines:
        record.parse_line(line)

    if not record.accessions:
        return None

    return record.to_dict()


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


_GO_ASPECT_NAMES = {
    'C': 'cellular_component',
    'F': 'molecular_function',
    'P': 'biological_process',
}


def _parse_go_mappings(lines: list[str]) -> tuple[str | None, dict[str, dict]]:
    """
    Extract the primary accession and GO cross-references from one flat-file entry.
    :param lines: One entry's lines, as yielded by _iter_dat_records.
    :return: The accession (None if absent) and a mapping of GO ID to aspect, term and
        de-duplicated evidence codes, in first-seen order.
    """
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
                    'aspect': _GO_ASPECT_NAMES[aspect], 'term': term.strip(), 'evidence': [],
                })
                _append_unique(mapping['evidence'], evidence.strip())
    return accession, mappings


def iter_go_annotations():
    """Stream UniProt-published protein-to-GO annotations from the release flat files."""
    source = AnnotationSource(
        'UniProtKB GO cross-reference', ConceptPrefix.UNIPROT, ConceptPrefix.GO,
    )
    for file_path in FILE_PATHS:
        full_path = os.path.join(CONFIG.data_dir, file_path)
        for lines in iter_progress(
            _iter_dat_records(full_path),
            description=f'Processing UniProt GO annotations from {file_path}',
        ):
            accession, mappings = _parse_go_mappings(lines)
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
        # UniProt is intentionally streamed in bounded batches. Use idempotent indexing so an
        # interrupted load can be resumed over batches that already reached the document store.
        # ``create`` operations make the entire next batch fail on those existing identifiers.
        await doc_db.save_terms(terms=concepts, no_upsert=False)

        batch_graph = EdgeBuffer()
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

        batch_graph = EdgeBuffer()
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


def _build_record_concepts(record: dict) -> tuple[list[UniProtConcept], list[tuple[str, str]]]:
    """
    Build the primary concept for one parsed entry plus a deprecated stub per secondary
    accession, together with the secondary-to-primary replacement edges.
    """
    concept = _build_uniprot_concept(record)
    concepts = [concept]
    replacements = []
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
    return concepts, replacements


def _iter_record_batches(file_path: str, load_annotations: bool):
    """
    Stream one release flat file as bounded batches of concepts, symbol annotations and replacement
    edges. Each yielded batch is flagged as full (reached _BATCH_SIZE) or the file's remainder.
    """
    concepts: list[UniProtConcept] = []
    annotations: list[Annotation] = []
    replacements: list[tuple[str, str]] = []

    full_path = os.path.join(CONFIG.data_dir, file_path)
    for lines in iter_progress(_iter_dat_records(full_path), description=f'Processing {file_path}'):
        record = _parse_dat_record(lines)
        if record is None:
            continue

        record_concepts, record_replacements = _build_record_concepts(record)
        concepts.extend(record_concepts)
        replacements.extend(record_replacements)
        if load_annotations:
            annotations.extend(_build_symbol_annotations(record))

        if len(concepts) >= _BATCH_SIZE:
            yield concepts, annotations, replacements, True
            concepts, annotations, replacements = [], [], []

    if concepts:
        yield concepts, annotations, replacements, False


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
        verbose_print(f'Streaming UniProt entries from {file_path}...')

        batches = _iter_record_batches(file_path, load_annotations)
        for concepts, annotations, replacements, is_full in batches:
            await _flush_batch(
                concepts, annotations, replacements,
                is_first_batch=(total_batches == 0),
                doc_db=doc_db, graph_db=graph_db, offline=offline,
                build_search_index=build_search_index,
                load_annotations=load_annotations,
            )
            total_loaded += len(concepts)
            total_batches += 1
            if is_full:
                verbose_print(f'Loaded {total_loaded} UniProt identifiers so far...')

    verbose_print(
        f'UniProt loading complete: {total_loaded} identifiers in {total_batches} batches.'
    )
