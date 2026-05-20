from enum import StrEnum
from fastmcp.dependencies import Depends

from bioterms.etc.enums import ConceptPrefix, SimilarityMethod
from bioterms.database import DocumentDatabase, GraphDatabase, VectorDatabase, get_active_doc_db, \
    get_active_graph_db, get_active_vector_db
from bioterms.model.concept import ConceptUnion
from bioterms.model.related_term import RelatedTerm
from bioterms.model.similar_term import SimilarTerm
from bioterms.model.translated_term import TranslatedTerm
from bioterms.vocabulary import get_vocabulary_config
from .app import mcp


class OntologyPrefix(StrEnum):
    """
    The ontology prefixes supported by the MCP API.
    """
    CTV3 = 'ctv3'
    HPO = 'hpo'
    NCIT = 'ncit'
    OHDSI = 'ohdsi'
    OMIM = 'omim'
    ORDO = 'ordo'
    REACTOME = 'reactome'
    SNOMED = 'snomed'


@mcp.tool
async def auto_complete(vocabulary: ConceptPrefix,
                        query: str,
                        limit: int = None,
                        doc_db: DocumentDatabase = Depends(get_active_doc_db),
                        ) -> list[ConceptUnion]:
    """
    Runs an auto-complete search against the specified vocabulary.

    The result is a list of matching concepts as JSON objects. They must contain the search string
    in their labels, definitions, or synonyms. The list is sorted by relevance.
    :param vocabulary: The vocabulary to search through.
    :param query: The input search string, case-insensitive and white spaces are matched as-is.
    :param limit: The maximum number of results to return. Set to null or omit to return all results.
    :param doc_db: The document database to use for the search.
    :return: A list of matching concepts as JSON objects.
    """
    config = get_vocabulary_config(vocabulary)
    concepts_iter = doc_db.auto_complete_iter(
        prefix=vocabulary,
        query=query,
        limit=limit,
        model_class=config['conceptClass'],
    )

    return [concept async for concept in concepts_iter]


@mcp.tool
async def expand_ontology(ontology: OntologyPrefix,
                          concept_ids: list[str],
                          depth: int = None,
                          limit: int = None,
                          graph_db: GraphDatabase = Depends(get_active_graph_db),
                          ) -> list[RelatedTerm]:
    """
    Expand ontology concepts by their IDs.

    This tool traverses the ontology structure to find descendant concepts of the given concept IDs up
    to a specified depth.
    :param ontology: The ontology to expand.
    :param concept_ids: The concept IDs to expand, without the domain prefix.
    :param depth: The maximum depth to expand to. A depth of 1 will return direct children, a depth of 2 will
        return children and grandchildren, etc. Set to null or omit for unlimited depth, but this may result
        in very large responses.
    :param limit: The maximum number of results to return. Set to null or omit to return all results.
    :param graph_db: The graph database to use for the expansion.
    :return: A list of related concepts as JSON objects.
    """
    expand_iter = graph_db.expand_terms_iter(
        prefix=ConceptPrefix(ontology.value),
        concept_ids=concept_ids,
        max_depth=depth,
        limit=limit,
    )

    return [term async for term in expand_iter]


@mcp.tool
async def map_concepts(source_vocabulary: ConceptPrefix,
                       target_vocabulary: ConceptPrefix,
                       concept_ids: list[str],
                       max_hops: int = 1,
                       limit: int = None,
                       graph_db: GraphDatabase = Depends(get_active_graph_db),
                       ) -> list[RelatedTerm]:
    """
    Map concepts from one vocabulary to another.

    This tool uses the published annotations and mapping data to map concepts from one vocabulary to another.
    It only considers annotation and mapping, not the internal structure of the vocabulary.
    :param source_vocabulary: The source vocabulary.
    :param target_vocabulary: The target vocabulary.
    :param concept_ids: The concept IDs to map.
    :param max_hops: The maximum number of hops to consider during mapping. If 1, the source concept must be
        directly annotated with the target concept. If 2, there may be another concept of different vocabulary
        in between.
    :param limit: The maximum number of results to return.
    :param graph_db: The graph database to use for the mapping.
    :return: A list of mapped concepts as JSON objects.
    """
    map_iter = graph_db.map_terms_iter(
        prefix=source_vocabulary,
        target_prefix=target_vocabulary,
        concept_ids=concept_ids,
        max_hops=max_hops,
        limit=limit,
    )

    return [term async for term in map_iter]


@mcp.tool
async def search_vocabulary(vocabulary: ConceptPrefix,
                            query: str,
                            limit: int = 10,
                            doc_db: DocumentDatabase = Depends(get_active_doc_db),
                            vector_db: VectorDatabase = Depends(get_active_vector_db),
                            ):
    """
    Search the specified vocabulary for the given query.

    It will return the top-k results sorted by relevance. The search is performed using embedding-based
    similarity, so the results may not contain the exact query string but will be similar.
    :param vocabulary: The vocabulary to search.
    :param query: The search query.
    :param limit: The maximum number of results to return.
    :param doc_db: The document database to use for the search.
    :param vector_db: The vector database to use for the search.
    :return: A list of search results as JSON objects.
    """
    config = get_vocabulary_config(vocabulary)

    concept_ids = await vector_db.search_concepts(
        query=query,
        prefix=vocabulary,
        limit=limit or 10,
    )

    concepts_iter = doc_db.get_terms_by_ids_iter(
        prefix=vocabulary,
        concept_ids=concept_ids,
        model_class=config['conceptClass']
    )

    return [concept async for concept in concepts_iter]


@mcp.tool
async def get_similar_concepts(vocabulary: ConceptPrefix,
                               concept_ids: list[str],
                               threshold: float = 1.0,
                               same_vocabulary: bool = True,
                               corpus_vocabulary: ConceptPrefix | None = None,
                               method: SimilarityMethod | None = None,
                               limit: int | None = None,
                               graph_db: GraphDatabase = Depends(get_active_graph_db),
                               ) -> list[SimilarTerm]:
    """
    Find concepts that are similar to given concepts in the specified vocabulary.

    This tool uses pre-computed semantic similarity scores to find concepts that are similar to the given
    concepts. It allows specifying which score(s) to use and the threshold for similarity.
    :param vocabulary: The vocabulary to search within.
    :param concept_ids: The concept IDs to find similar concepts for.
    :param threshold: The minimum similarity score required for a concept to be considered similar. It's a value
        between 0.0 and 1.0, where 1.0 is an exact match, and 0.0 is no similarity at all. The server may not
        have stored all similarity scores, but only the ones above a certain value. Setting this parameter to too
        low may significantly impact performance and result in very large responses.
    :param same_vocabulary: Whether the resulting concepts must be from the same vocabulary as the input concepts.
    :param corpus_vocabulary: Optional parameter to specify the corpus vocabulary used for the scores.
    :param method: Optional parameter to specify the similarity method used to calculate the scores.
    :param limit: The maximum number of similar concepts to return for each input concept.
    :param graph_db: The graph database to use for the similarity search.
    :return: A list of similar concepts as JSON objects.
    """
    similarity_iter = graph_db.get_similar_terms_iter(
        prefix=vocabulary,
        concept_ids=concept_ids,
        threshold=threshold,
        same_prefix=same_vocabulary,
        corpus_prefix=corpus_vocabulary,
        method=method,
        limit=limit,
    )

    return [term async for term in similarity_iter]


@mcp.tool
async def translate_concepts_to_constraints(vocabulary: ConceptPrefix,
                                            original_concepts: list[str],
                                            constraint_concepts: list[str],
                                            threshold: float = 1.0,
                                            limit: int | None = None,
                                            graph_db: GraphDatabase = Depends(get_active_graph_db),
                                            ) -> list[TranslatedTerm]:
    """
    Get a list of concepts that are similar to the given concepts in the specified vocabulary, but are strictly
    from the provided list of constraint IDs.

    This tool is used to query a target system with known IDs (the constraint list), but the user chose different
    concepts that are not within the constraint list.
    :param vocabulary: The vocabulary to search within.
    :param original_concepts: The original concepts the user chose.
    :param constraint_concepts: The list of constraint concepts to filter by. This is a list of concept IDs with
        the domain prefix, such as "HPO:0000001".
    :param threshold: The minimum similarity score required for a concept to be considered similar.
    :param limit: The maximum number of similar concepts to return for each input concept.
    :param graph_db: The graph database to use for the similarity search.
    :return: A list of similar concepts as JSON objects.
    """
    constraint_dict = {}
    for concept in constraint_concepts:
        try:
            concept_prefix_str, concept_id = concept.split(':', 1)
            concept_prefix = ConceptPrefix(concept_prefix_str)
        except ValueError as e:
            raise ValueError(
                f'Invalid constraint concept format: {concept}. Expected format is prefix:id'
            ) from e

        if concept_prefix not in constraint_dict:
            constraint_dict[concept_prefix] = set()
        constraint_dict[concept_prefix].add(concept_id)

    translate_iter = graph_db.translate_terms_iter(
        original_ids=original_concepts,
        original_prefix=vocabulary,
        constraint_ids=constraint_dict,
        threshold=threshold,
        limit=limit,
    )

    return [term async for term in translate_iter]
