import importlib

from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import VocabularyNotLoaded
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.model.vocabulary_status import VocabularyStatus


ALL_VOCABULARIES = {
    ConceptPrefix.CTV3: 'ctv3',
    ConceptPrefix.ENSEMBL: 'ensembl',
    ConceptPrefix.HGNC: 'hgnc',
    ConceptPrefix.HGNC_SYMBOL: 'hgnc_symbol',
    ConceptPrefix.HPO: 'hpo',
    ConceptPrefix.NCIT: 'ncit',
    ConceptPrefix.OMIM: 'omim',
    ConceptPrefix.ORDO: 'ordo',
    ConceptPrefix.REACTOME: 'reactome',
    ConceptPrefix.SNOMED: 'snomed',
}


def get_vocabulary_module(prefix: ConceptPrefix):
    """
    Get the vocabulary module for the given prefix.
    :param prefix: The prefix of the vocabulary.
    :return: The vocabulary module.
    """
    vocabulary_module_name = ALL_VOCABULARIES.get(prefix)
    if not vocabulary_module_name:
        raise ValueError(f'Vocabulary with prefix {prefix} not found.')

    vocabulary_module = importlib.import_module(f'bioterms.vocabulary.{vocabulary_module_name}')

    return vocabulary_module


async def get_vocabulary_status(prefix: ConceptPrefix,
                                doc_db: DocumentDatabase = None,
                                graph_db: GraphDatabase = None,
                                ) -> VocabularyStatus:
    """
    Get the status of the vocabulary specified by the prefix.
    :param prefix: The prefix of the vocabulary.
    :param doc_db: The document database instance.
    :param graph_db: The graph database instance.
    :return: The vocabulary status.
    """
    vocabulary_module = get_vocabulary_module(prefix)

    if doc_db is None:
        doc_db = await get_active_doc_db()

    if graph_db is None:
        graph_db = get_active_graph_db()

    concept_count = await doc_db.count_terms(prefix)
    relationship_count = await graph_db.count_internal_relationships(prefix)
    annotations = vocabulary_module.ANNOTATIONS

    return VocabularyStatus(
        prefix=prefix,
        name=vocabulary_module.VOCABULARY_NAME,
        loaded=concept_count > 0,
        conceptCount=concept_count,
        relationshipCount=relationship_count,
        annotations=annotations,
        similarityMethods=vocabulary_module.SIMILARITY_METHODS,
    )


async def ensure_gene_symbol_loaded(doc_db: DocumentDatabase = None,
                                    graph_db: GraphDatabase = None,
                                    ):
    """
    Ensure that the HGNC gene symbol vocabulary is loaded.

    This is usually the prerequisite for loading other gene-related vocabularies, because they all
    map to the HGNC gene symbols.
    :param doc_db: The document database instance.
    :param graph_db: The graph database instance.
    :raises VocabularyNotLoaded: If the HGNC vocabulary is not loaded.
    """
    status = await get_vocabulary_status(
        prefix=ConceptPrefix.HGNC_SYMBOL,
        doc_db=doc_db,
        graph_db=graph_db,
    )

    if not status.loaded:
        raise VocabularyNotLoaded('HGNC gene symbol vocabulary is not loaded.')
