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
from bioterms.etc.utils import check_files_exist, ensure_data_directory, download_file, extract_file_from_gzip
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.model.annotation import Annotation
from bioterms.model.concept import EnsemblConcept
from .utils import ensure_gene_symbol_loaded


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

    annotation_url = 'https://ftp.ensembl.org/pub/release-113/gtf/homo_sapiens/Homo_sapiens.GRCh38.113.gtf.gz'
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


async def load_vocabulary_from_file(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    ):
    """
    Load the Ensembl vocabulary from gtf file into the primary databases.
    :param doc_db: Optional DocumentDatabase instance to use.
    :param graph_db: Optional GraphDatabase instance to use.
    """
    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('Ensembl gtf file not found')

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

    for _, row in gene_df.iterrows():
        # Parse the attribute into a dictionary
        attributes = dict(
            re.findall(r'(\S+)\s"([^"]+)"', row['attribute'])
        )

        if row['feature'] == 'gene':
            if attributes['gene_id'] in genes:
                continue

            if 'gene_name' in attributes:
                label = attributes['gene_name']
                annotations.append(Annotation(
                    prefixFrom=VOCABULARY_PREFIX,
                    prefixTo=ConceptPrefix.HGNC_SYMBOL,
                    conceptIdFrom=attributes['gene_id'],
                    conceptIdTo=attributes['gene_name'],
                    annotationType=AnnotationType.HAS_SYMBOL
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
        elif row['feature'] == 'transcript':
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
        elif row['feature'] == 'exon':
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
        elif row['feature'] == 'CDS':
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

    del gene_df

    if doc_db is None:
        doc_db = await get_active_doc_db()
    if graph_db is None:
        graph_db = get_active_graph_db()

    concepts = list(genes.values()) + \
               list(transcripts.values()) + \
               list(exons.values()) + \
               list(proteins.values())

    await doc_db.save_terms(
        terms=concepts,
    )

    await graph_db.save_vocabulary_graph(
        concepts=concepts,
        graph=ensembl_graph,
    )
    await graph_db.save_annotations(annotations)
