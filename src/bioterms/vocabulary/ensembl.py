import os
import re
import shlex
import aiofiles
import aiofiles.os
import httpx
import networkx as nx
import pandas as pd

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus, ConceptRelationshipType, ConceptType
from bioterms.etc.errors import FilesNotFound
from bioterms.etc.utils import check_files_exist, discover_latest_numbered_release, \
    ensure_data_directory, download_file, extract_file_from_gzip, iter_progress, verbose_print
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.model.concept import EnsemblConcept
from .utils import write_concepts_to_file, write_graph_to_file


VOCABULARY_NAME = 'Ensembl'
VOCABULARY_PREFIX = ConceptPrefix.ENSEMBL
ANNOTATIONS = [
    ConceptPrefix.HGNC_SYMBOL,
    ConceptPrefix.OMIM,
    ConceptPrefix.REACTOME,
    ConceptPrefix.UNIPROT,
]
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

    release, release_url = await discover_latest_numbered_release(
        'https://ftp.ensembl.org/pub/', download_client,
    )
    directory_url = f'{release_url}gtf/homo_sapiens/'
    close_client = download_client is None
    client = download_client or httpx.AsyncClient(follow_redirects=True)
    try:
        response = await client.get(directory_url)
        response.raise_for_status()
        filenames = re.findall(
            rf'href="(Homo_sapiens\.GRCh38\.{release}\.gtf\.gz)"',
            response.text,
        )
    finally:
        if close_client:
            await client.aclose()

    if not filenames:
        raise ValueError('Could not discover the current Ensembl human GTF release.')

    annotation_url = f'{directory_url}{filenames[0]}'
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


def _handle_gene_feature(attributes: dict,
                         row,
                         genes: dict[str, CONCEPT_CLASS],
                         ensembl_graph: nx.DiGraph,
                         ):
    """
    Process a GTF 'gene' feature row into a gene Concept.
    """
    if attributes['gene_id'] in genes:
        return

    gene_concept = CONCEPT_CLASS(
        prefix=VOCABULARY_PREFIX,
        conceptId=attributes['gene_id'],
        label=attributes.get('gene_name'),
        conceptTypes=[ConceptType.GENE],
        bioType=attributes['gene_biotype'],
        start=int(row['start']),
        end=int(row['end']),
        sequence=row['seqname'],
        version=attributes.get('gene_version'),
        strand=row['strand'],
        source=attributes.get('gene_source', row['source']),
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
            version=attributes.get('transcript_version'),
            strand=row['strand'],
            source=attributes.get('transcript_source', row['source']),
            transcriptSupportLevel=attributes.get('transcript_support_level'),
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
            version=attributes.get('exon_version'),
            strand=row['strand'],
            source=row['source'],
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
            version=attributes.get('protein_version'),
            strand=row['strand'],
            source=row['source'],
            status=ConceptStatus.ACTIVE,
        )

        proteins[attributes['protein_id']] = protein_concept
        ensembl_graph.add_node(protein_concept.concept_id)
    else:
        protein_concept = proteins[attributes['protein_id']]
        protein_concept.start = min(protein_concept.start, int(row['start']))
        protein_concept.end = max(protein_concept.end, int(row['end']))

    ensembl_graph.add_edge(
        attributes['protein_id'],
        attributes['transcript_id'],
        label=ConceptRelationshipType.PART_OF,
    )


async def load_vocabulary_from_file(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    offline: bool = False,
                                    build_search_index: bool = True,
                                    ):
    """
    Load the Ensembl vocabulary from gtf file into the primary databases.
    :param doc_db: Optional DocumentDatabase instance to use.
    :param graph_db: Optional GraphDatabase instance to use.
    :param offline: Whether to operate in offline mode and write to data files only.
    """
    if not check_files_exist(FILE_PATHS):
        raise FilesNotFound('Ensembl gtf file not found')

    gene_chunks = pd.read_csv(
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
        },
        chunksize=100000,
    )

    genes: dict[str, CONCEPT_CLASS] = {}
    transcripts: dict[str, CONCEPT_CLASS] = {}
    exons: dict[str, CONCEPT_CLASS] = {}
    proteins: dict[str, CONCEPT_CLASS] = {}
    ensembl_graph = nx.DiGraph()

    verbose_print('Ensembl GTF file read, processing entries...')

    for gene_df in gene_chunks:
        for _, row in iter_progress(
            gene_df.iterrows(), description='Processing GTF entries', total=len(gene_df),
        ):
            # GTF attributes are `key "value";` pairs. shlex splits each pair into two
            # tokens, which are recombined here. Repeated attributes such as `tag` are not
            # currently persisted and therefore do not affect the feature model.
            attribute_tokens = shlex.split(row['attribute'].replace(';', ' '))
            attributes = dict(zip(attribute_tokens[0::2], attribute_tokens[1::2]))

            feature = row['feature']
            if feature == 'gene':
                _handle_gene_feature(attributes, row, genes, ensembl_graph)
            elif feature == 'transcript':
                _handle_transcript_feature(attributes, row, transcripts, ensembl_graph)
            elif feature == 'exon':
                _handle_exon_feature(attributes, row, exons, ensembl_graph)
            elif feature == 'CDS':
                _handle_cds_feature(attributes, row, proteins, ensembl_graph)

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
    else:
        await write_concepts_to_file(
            prefix=VOCABULARY_PREFIX,
            concepts=concepts,
            build_search_index=build_search_index,
        )
        await write_graph_to_file(
            prefix=VOCABULARY_PREFIX,
            concepts=concepts,
            vocabulary_graph=ensembl_graph,
        )
        del concepts
