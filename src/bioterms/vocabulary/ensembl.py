import os
import re
import aiofiles
import aiofiles.os
import httpx
import networkx as nx
import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus, ConceptRelationshipType, ConceptType, AnnotationType
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, ensure_data_directory, download_file, extract_file_from_gzip, \
    iter_progress, verbose_print
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.model.annotation import Annotation
from bioterms.model.concept import EnsemblConcept
from .utils import ensure_gene_symbol_loaded, write_concepts_to_file, write_graph_to_file, \
    write_annotations_to_file


VOCABULARY_NAME = 'Ensembl'
VOCABULARY_PREFIX = ConceptPrefix.ENSEMBL
ANNOTATIONS = []
SIMILARITY_METHODS = []
FILE_PATHS = [
    'ensembl/homo-sapien.gtf',
]
TIMESTAMP_FILE = 'ensembl/.timestamp'
CONCEPT_CLASS = EnsemblConcept


async def download_vocabulary(download_client: httpx.AsyncClient = None):
    """
    Download the Ensembl vocabulary files.
    :param download_client: Optional httpx.AsyncClient to use for downloading.
    """
    if check_files_exist(FILE_PATHS):
        return

    ensure_data_directory()

    annotation_url = 'https://ftp.ensembl.org/pub/release-115/gtf/homo_sapiens/Homo_sapiens.GRCh38.115.gtf.gz'
    gzip_path = os.path.join(CONFIG.data_dir, 'ensembl/homo-sapien.gz')

    try:
        await download_file(
            url=annotation_url,
            file_path='ensembl/homo-sapien.gz',
            download_client=download_client,
        )

        await extract_file_from_gzip(
            gzip_path=gzip_path,
            output_path=os.path.join(CONFIG.data_dir, FILE_PATHS[0]),
        )
    finally:
        try:
            await aiofiles.os.remove(gzip_path)
        except Exception:
            pass


def _load_hgnc_ensembl_symbol_lookup() -> dict[str, str]:
    """
    Build an ensembl_gene_id -> HGNC-approved symbol lookup from HGNC's own release file.

    Ensembl's GTF gene_name is externally documented (Ensembl genebuild gene-naming docs)
    as HGNC-sourced for the great majority of human protein-coding genes -- so asserting it
    as a fresh has_symbol edge risks double-counting one HGNC nomenclature decision as two
    independent votes (Ensembl's and HGNC's own). This lookup is used only to TAG (never to
    filter, replace, or remove) Ensembl's GTF-derived has_symbol edges when the GTF's
    gene_name matches HGNC's own ensembl_gene_id crosswalk for that gene -- downstream
    consensus/provenance modelling can then discount a tagged edge without bts having to
    change what it serves.
    :return: A dict mapping ensembl_gene_id to HGNC's approved symbol. Empty if HGNC's
        release file is not present -- this is a provenance enrichment, not a hard
        prerequisite for loading Ensembl's own gene/transcript/exon/protein data.
    """
    hgnc_symbol_path = os.path.join(CONFIG.data_dir, 'hgnc/symbol.txt')
    if not os.path.exists(hgnc_symbol_path):
        verbose_print(
            'HGNC symbol file not found -- Ensembl has_symbol edges will not be tagged '
            'with HGNC crosswalk provenance.'
        )
        return {}

    hgnc_df = pd.read_csv(
        hgnc_symbol_path,
        sep='\t',
        dtype=str,
        usecols=['ensembl_gene_id', 'symbol'],
    )

    lookup: dict[str, str] = {}
    for _, row in hgnc_df.iterrows():
        if pd.isna(row['ensembl_gene_id']) or pd.isna(row['symbol']):
            continue
        # A small number of ensembl_gene_id values (3 of 42,346 in the 2026-07-22 release)
        # appear on more than one HGNC row; last one wins, which is an acceptable
        # approximation for a tagging-only lookup.
        lookup[row['ensembl_gene_id']] = row['symbol']

    return lookup


def _handle_gene_feature(attributes: dict,
                         row,
                         genes: dict[str, CONCEPT_CLASS],
                         ensembl_graph: nx.DiGraph,
                         annotations: list[Annotation],
                         hgnc_ensembl_lookup: dict[str, str] = None,
                         ):
    """
    Process a GTF 'gene' feature row into a gene Concept and its HGNC symbol annotation.
    """
    if attributes['gene_id'] in genes:
        return

    hgnc_ensembl_lookup = hgnc_ensembl_lookup or {}

    if 'gene_name' in attributes:
        label = attributes['gene_name']
        hgnc_symbol = hgnc_ensembl_lookup.get(attributes['gene_id'])
        properties = (
            {'derivation': 'hgnc_ensembl_gene_id_xref'}
            if hgnc_symbol is not None and hgnc_symbol == attributes['gene_name']
            else None
        )
        annotations.append(Annotation(
            prefixFrom=VOCABULARY_PREFIX,
            prefixTo=ConceptPrefix.HGNC_SYMBOL,
            conceptIdFrom=attributes['gene_id'],
            conceptIdTo=attributes['gene_name'],
            annotationType=AnnotationType.HAS_SYMBOL,
            properties=properties,
        ))
    else:
        label = None

    gene_concept = CONCEPT_CLASS(
        prefix=VOCABULARY_PREFIX,
        conceptId=attributes['gene_id'],
        label=label,
        conceptTypes=[ConceptType.GENE],
        bioType=attributes['gene_biotype'],
        start=int(row['start']),
        end=int(row['end']),
        sequence=row['seqname'],
        status=ConceptStatus.ACTIVE,
    )

    genes[attributes['gene_id']] = gene_concept
    ensembl_graph.add_node(gene_concept.concept_id)


def _handle_transcript_feature(attributes: dict,
                               row,
                               transcripts: dict[str, CONCEPT_CLASS],
                               ensembl_graph: nx.DiGraph,
                               ):
    """
    Process a GTF 'transcript' feature row into a transcript Concept, part-of its gene.
    """
    if attributes['transcript_id'] not in transcripts:
        transcript_concept = CONCEPT_CLASS(
            prefix=VOCABULARY_PREFIX,
            conceptId=attributes['transcript_id'],
            label=attributes.get('transcript_name'),
            conceptTypes=[ConceptType.TRANSCRIPT],
            bioType=attributes.get('transcript_biotype'),
            start=int(row['start']),
            end=int(row['end']),
            sequence=row['seqname'],
            status=ConceptStatus.ACTIVE,
        )

        transcripts[attributes['transcript_id']] = transcript_concept
        ensembl_graph.add_node(transcript_concept.concept_id)

    ensembl_graph.add_edge(
        attributes['transcript_id'],
        attributes['gene_id'],
        label=ConceptRelationshipType.PART_OF,
    )


def _handle_exon_feature(attributes: dict,
                         row,
                         exons: dict[str, CONCEPT_CLASS],
                         ensembl_graph: nx.DiGraph,
                         ):
    """
    Process a GTF 'exon' feature row into an exon Concept, part-of its transcript.
    """
    if attributes['exon_id'] not in exons:
        exon_concept = CONCEPT_CLASS(
            prefix=VOCABULARY_PREFIX,
            conceptId=attributes['exon_id'],
            conceptTypes=[ConceptType.EXON],
            start=int(row['start']),
            end=int(row['end']),
            sequence=row['seqname'],
            status=ConceptStatus.ACTIVE,
        )

        exons[attributes['exon_id']] = exon_concept
        ensembl_graph.add_node(exon_concept.concept_id)

    ensembl_graph.add_edge(
        attributes['exon_id'],
        attributes['transcript_id'],
        label=ConceptRelationshipType.PART_OF,
    )


def _handle_cds_feature(attributes: dict,
                        row,
                        proteins: dict[str, CONCEPT_CLASS],
                        ensembl_graph: nx.DiGraph,
                        ):
    """
    Process a GTF 'CDS' feature row into a protein Concept, part-of its transcript.
    """
    if attributes['protein_id'] not in proteins:
        protein_concept = CONCEPT_CLASS(
            prefix=VOCABULARY_PREFIX,
            conceptId=attributes['protein_id'],
            conceptTypes=[ConceptType.PROTEIN],
            start=int(row['start']),
            end=int(row['end']),
            sequence=row['seqname'],
            status=ConceptStatus.ACTIVE,
        )

        proteins[attributes['protein_id']] = protein_concept
        ensembl_graph.add_node(protein_concept.concept_id)

    ensembl_graph.add_edge(
        attributes['protein_id'],
        attributes['transcript_id'],
        label=ConceptRelationshipType.PART_OF,
    )


async def load_vocabulary_from_file(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    offline: bool = False,
                                    ):
    """
    Load the Ensembl vocabulary from gtf file into the primary databases.
    :param doc_db: Optional DocumentDatabase instance to use.
    :param graph_db: Optional GraphDatabase instance to use.
    :param offline: Whether to operate in offline mode and write to data files only.
    """
    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('Ensembl gtf file not found')

    verbose_print('Checking if HGNC symbols are loaded...')

    if not offline:
        await ensure_gene_symbol_loaded(
            doc_db=doc_db,
            graph_db=graph_db,
        )

    gene_df = pd.read_csv(
        str(os.path.join(CONFIG.data_dir, FILE_PATHS[0])),
        sep='\t',
        comment='#',
        header=None,
        names=[
            'seqname',
            'source',
            'feature',
            'start',
            'end',
            'score',
            'strand',
            'frame',
            'attribute',
        ],
        dtype={
            'seqname': str,
        }
    )

    genes: dict[str, CONCEPT_CLASS] = {}
    transcripts: dict[str, CONCEPT_CLASS] = {}
    exons: dict[str, CONCEPT_CLASS] = {}
    proteins: dict[str, CONCEPT_CLASS] = {}
    ensembl_graph = nx.DiGraph()
    annotations = []
    hgnc_ensembl_lookup = _load_hgnc_ensembl_symbol_lookup()

    verbose_print('Ensembl GTF file read, processing entries...')

    for _, row in iter_progress(gene_df.iterrows(), description='Processing GTF entries', total=len(gene_df)):
        # Parse the attribute into a dictionary
        attributes = dict(
            re.findall(r'([^\s"]+)\s"([^"]+)"', row['attribute'])
        )

        feature = row['feature']
        if feature == 'gene':
            _handle_gene_feature(attributes, row, genes, ensembl_graph, annotations, hgnc_ensembl_lookup)
        elif feature == 'transcript':
            _handle_transcript_feature(attributes, row, transcripts, ensembl_graph)
        elif feature == 'exon':
            _handle_exon_feature(attributes, row, exons, ensembl_graph)
        elif feature == 'CDS':
            _handle_cds_feature(attributes, row, proteins, ensembl_graph)

    del gene_df

    verbose_print('Ensembl concepts processed, saving to databases...')

    concepts = list(genes.values()) + \
               list(transcripts.values()) + \
               list(exons.values()) + \
               list(proteins.values())
    del genes
    del transcripts
    del exons
    del proteins

    if not offline:
        if doc_db is None:
            doc_db = await get_active_doc_db()
        if graph_db is None:
            graph_db = get_active_graph_db()

        await doc_db.save_terms(
            terms=concepts,
            no_upsert=True,
        )

        await graph_db.save_vocabulary_graph(
            concepts=concepts,
            graph=ensembl_graph,
        )
        await graph_db.save_annotations(annotations)
    else:
        await write_concepts_to_file(
            prefix=VOCABULARY_PREFIX,
            concepts=concepts,
        )
        await write_graph_to_file(
            prefix=VOCABULARY_PREFIX,
            concepts=concepts,
            vocabulary_graph=ensembl_graph,
        )
        del concepts
        await write_annotations_to_file(
            prefix_from=VOCABULARY_PREFIX,
            prefix_to=ConceptPrefix.HGNC_SYMBOL,
            annotations=annotations,
        )
