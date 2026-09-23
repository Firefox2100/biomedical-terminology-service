"""
Utility functions for GraphQL resolvers.
"""

import asyncio
from time import perf_counter
from ariadne import QueryType

from bioterms.etc.enums import ConceptPrefix
from bioterms.database import DocumentDatabase
from bioterms.vocabulary import get_vocabulary_status, get_vocabulary_config
from bioterms.search import execute_hybrid_search
from ..data_loader import DataLoader


GRAPHQL_QUERY_TYPE = QueryType()


@GRAPHQL_QUERY_TYPE.field('search')
async def resolve_global_search(_, info, query: str,
                                vocabularies: list[str], limit: int = 10,
                                include_match_details: bool = True) -> dict:
    """Resolve the multi-vocabulary V2 search at the GraphQL root."""
    if not query.strip():
        raise ValueError('query must not be empty')
    if not 1 <= limit <= 100:
        raise ValueError('limit must be between 1 and 100')
    if not vocabularies:
        raise ValueError('at least one vocabulary is required')

    prefixes = list(dict.fromkeys(ConceptPrefix(prefix) for prefix in vocabularies))
    model_classes = {
        prefix: get_vocabulary_config(prefix)['conceptClass'] for prefix in prefixes
    }
    started = perf_counter()
    execution = await execute_hybrid_search(
        query=query,
        prefixes=prefixes,
        doc_db=info.context['doc_db'],
        vector_db=info.context['vector_db'],
        model_classes=model_classes,
        limit=limit,
    )

    results = []
    for rank, hit in enumerate(execution.hits, start=1):
        concept = hit.concept.model_dump(exclude_none=True)
        concept['__typename'] = prefix_to_concept_type(hit.concept.prefix)
        match = None
        if include_match_details:
            match = {
                'type': 'exact' if hit.exact else 'hybrid',
                'exact': hit.exact,
                'field': hit.match_field,
                'text': hit.matched_text,
            }
        results.append({'rank': rank, 'concept': concept, 'match': match})

    return {
        'query': query.strip(),
        'results': results,
        'meta': {
            'returned': len(results),
            'limit': limit,
            'durationMs': (perf_counter() - started) * 1000,
            'vocabularies': prefixes,
            'pipeline': {
                'lexical': True,
                'vector': execution.vector_used,
                'mapped': execution.mapped_used,
                'reranker': execution.reranker_used,
            },
        },
    }


def assemble_response(data: dict | list[dict] = None,
                      error_str: str = None,
                      error_code: int = None,
                      ) -> dict:
    """
    Assemble a standardised response dictionary.
    :param data: The data to include in the response.
    :param error_str: The error message, if any.
    :param error_code: The error code, if any.
    :return: A dictionary containing 'data' and 'error' keys.
    """
    error = None
    if error_str is not None:
        if error_code is not None:
            error = {
                'message': error_str,
                'code': error_code
            }
        else:
            error = {
                'message': error_str,
                'code': 400  # Default error code
            }

    return {
        'data': data if data is not None else None,
        'error': error,
    }


def prefix_to_concept_type(prefix: ConceptPrefix) -> str:
    """
    Map a ConceptPrefix to its corresponding GraphQL concept type name.
    :param prefix: The ConceptPrefix enum value.
    :return: The name of the corresponding GraphQL concept type.
    """
    mapping = {
        ConceptPrefix.CTV3: 'Ctv3Concept',
        ConceptPrefix.ENSEMBL: 'EnsemblConcept',
        ConceptPrefix.GO: 'GoConcept',
        ConceptPrefix.HGNC: 'HgncConcept',
        ConceptPrefix.HGNC_SYMBOL: 'HgncSymbolConcept',
        ConceptPrefix.HPO: 'HpoConcept',
        ConceptPrefix.LOINC: 'LoincConcept',
        ConceptPrefix.MONDO: 'MondoConcept',
        ConceptPrefix.NCIT: 'NcitConcept',
        ConceptPrefix.OHDSI: 'OhdsiConcept',
        ConceptPrefix.OMIM: 'OmimConcept',
        ConceptPrefix.ORDO: 'OrdoConcept',
        ConceptPrefix.REACTOME: 'ReactomeConcept',
        ConceptPrefix.RXNORM: 'RxNormConcept',
        ConceptPrefix.SNOMED: 'SnomedConcept',
        ConceptPrefix.UBERON: 'UberonConcept',
        ConceptPrefix.UNIPROT: 'UniProtConcept',
    }
    
    if prefix in mapping:
        return mapping[prefix]
    
    raise ValueError(f'Unknown concept prefix: {prefix}')


def type_to_reactome_concept_type(type_name: str) -> str:
    """
    Map a concept type name to its corresponding GraphQL ReactomeConcept type name.
    :param type_name: The concept type name.
    :return: The name of the corresponding GraphQL ReactomeConcept type.
    """
    if type_name == 'pathway':
        return 'ReactomePathway'
    if type_name == 'reaction':
        return 'ReactomeReaction'
    if type_name == 'gene':
        return 'ReactomeGene'

    raise ValueError(f'Unknown Reactome concept type: {type_name}')


def type_to_ensembl_concept_type(type_name: str) -> str:
    """Map an Ensembl concept type value to its concrete GraphQL object type."""
    mapping = {
        'gene': 'EnsemblGene',
        'transcript': 'EnsemblTranscript',
        'exon': 'EnsemblExon',
        'protein': 'EnsemblProtein',
    }
    try:
        return mapping[type_name]
    except KeyError as exc:
        raise ValueError(f'Unknown Ensembl concept type: {type_name}') from exc


@GRAPHQL_QUERY_TYPE.field('loadedPrefixes')
async def resolve_loaded_prefixes(_, info) -> list[str]:
    """
    Resolve the list of loaded vocabulary prefixes.
    :param _: The GraphQL parent object, not used.
    :param info: The GraphQL resolver info.
    :return: A list of loaded vocabulary prefixes as strings.
    """
    doc_db = info.context['doc_db']
    graph_db = info.context['graph_db']

    # Cache/DB lookups per prefix are independent, so fetch them concurrently rather than
    # paying for one sequential round-trip per vocabulary.
    statuses = await asyncio.gather(*(
        get_vocabulary_status(prefix, doc_db=doc_db, graph_db=graph_db)
        for prefix in ConceptPrefix
    ))

    return [
        prefix.value
        for prefix, status in zip(ConceptPrefix, statuses)
        if status.loaded
    ]


async def resolve_concept_info_fields(obj,
                                      info,
                                      prefix: ConceptPrefix,
                                      ) -> str | int | float | bool | list | None:
    """
    Resolve fields for ConceptInfo type.
    :param obj: The GraphQL passed-in object that represent the concept being resolved.
    :param info: The GraphQL resolver info.
    :param prefix: The vocabulary prefix.
    :return: The value of the requested field.
    """
    concept_id = obj['conceptId']
    field_name = info.field_name
    data_loader: DataLoader = info.context['data_loader']

    concept_loader = data_loader.get_concept_loader(prefix)
    concept = await concept_loader.id.load(concept_id)

    if not concept:
        if field_name == 'prefix':
            return str(prefix.value)

        if field_name == 'status':
            return 'unknown'

        return None

    return concept.get(field_name)


async def resolve_concept_replaces(obj,
                                   info,
                                   prefix: ConceptPrefix,
                                   ) -> list[dict]:
    """
    Resolve the 'replaces' field for a concept.
    :param obj: The GraphQL passed-in object that represent the concept being resolved.
    :param info: The GraphQL resolver info.
    :param prefix: The vocabulary prefix.
    :return: A list of replaced concepts.
    """
    concept_id = obj['conceptId']
    data_loader: DataLoader = info.context['data_loader']

    concept_loader = data_loader.get_concept_loader(prefix)
    replaced_ids = await concept_loader.replaced.load(concept_id)

    return [
        {'conceptId': replaced_id} for replaced_id in replaced_ids
    ]


async def resolve_concept_replaced_by(obj,
                                      info,
                                      prefix: ConceptPrefix,
                                      ):
    """
    Resolve the 'replacedBy' field for a concept.
    :param obj: The GraphQL passed-in object that represent the concept being resolved.
    :param info: The GraphQL resolver info.
    :param prefix: The vocabulary prefix.
    :return: A list of concepts that replace the given concept.
    """
    concept_id = obj['conceptId']
    data_loader: DataLoader = info.context['data_loader']

    concept_loader = data_loader.get_concept_loader(prefix)
    replacing_ids = await concept_loader.replacement.load(concept_id)

    return [
        {'conceptId': replacing_id} for replacing_id in replacing_ids
    ]


async def resolve_concept_children(obj,
                                   info,
                                   prefix: ConceptPrefix,
                                   ) -> list[dict]:
    """
    Resolve the 'children' field for a concept.
    :param obj: The GraphQL passed-in object that represent the concept being resolved.
    :param info: The GraphQL resolver info.
    :param prefix: The vocabulary prefix.
    :return: A list of child concepts.
    """
    concept_id = obj['conceptId']
    data_loader: DataLoader = info.context['data_loader']

    concept_loader = data_loader.get_concept_loader(prefix)
    children_ids = await concept_loader.children.load(concept_id)

    return [
        {'conceptId': child_id} for child_id in children_ids
    ]


async def resolve_concept_parents(obj,
                                  info,
                                  prefix: ConceptPrefix,
                                  ) -> list[dict]:
    """
    Resolve the 'parents' field for a concept.
    :param obj: The GraphQL passed-in object that represent the concept being resolved.
    :param info: The GraphQL resolver info.
    :param prefix: The vocabulary prefix.
    :return: A list of parent concepts.
    """
    concept_id = obj['conceptId']
    data_loader: DataLoader = info.context['data_loader']

    concept_loader = data_loader.get_concept_loader(prefix)
    parents_ids = await concept_loader.parents.load(concept_id)

    return [
        {'conceptId': parent_id} for parent_id in parents_ids
    ]


async def resolve_concept_similar_concepts(obj,
                                           info,
                                           prefix: ConceptPrefix,
                                           threshold: float = 1.0,
                                           ) -> list[dict]:
    """
    Resolve the 'similarConcepts' field for a concept.
    :param obj: The GraphQL passed-in object that represent the concept being resolved.
    :param info: The GraphQL resolver info.
    :param prefix: The vocabulary prefix.
    :param threshold: The similarity score threshold.
    :return: A list of similar concepts with their scores.
    """
    concept_id = obj['conceptId']
    data_loader: DataLoader = info.context['data_loader']

    concept_loader = data_loader.get_concept_loader(prefix)
    similar_concepts = await concept_loader.similar.load((concept_id, threshold))

    return [
        {
            'from': {
                'conceptId': concept_id
            },
            'to': {
                'conceptId': similar_id
            },
            'score': score,
        } for similar_id, score in similar_concepts
    ]


async def resolve_concept_paths_to(obj,
                                   info,
                                   prefix: ConceptPrefix,
                                   target_prefix: str,
                                   target_concept_id: str,
                                   relationship: str,
                                   direction: str,
                                   max_depth: int,
                                   ) -> list[dict]:
    """
    Resolve the 'pathsTo' field for a concept.
    :param obj: The GraphQL passed-in object that represent the concept being resolved.
    :param info: The GraphQL resolver info.
    :param prefix: The source vocabulary prefix.
    :param target_prefix: The target vocabulary prefix.
    :param target_concept_id: The target concept ID.
    :param relationship: The type of relationship to trace.
    :param direction: The direction of the path ('forward', 'backward', or 'undirected').
    :param max_depth: The maximum depth of the path.
    :return: A list of concept paths.
    """
    concept_id = obj['conceptId']
    data_loader: DataLoader = info.context['data_loader']

    concept_loader = data_loader.get_concept_loader(prefix)
    paths = await concept_loader.paths_to.load((
        concept_id,
        target_prefix,
        target_concept_id,
        relationship,
        direction,
        max_depth,
    ))

    reactome_loader = data_loader.get_concept_loader(ConceptPrefix.REACTOME)
    ensembl_loader = data_loader.get_concept_loader(ConceptPrefix.ENSEMBL)
    reactome_concept_ids = set()
    ensembl_concept_ids = set()
    for path in paths:
        for node in path[1]:
            if node[0] == ConceptPrefix.REACTOME.value:
                reactome_concept_ids.add(node[1])
            elif node[0] == ConceptPrefix.ENSEMBL.value:
                ensembl_concept_ids.add(node[1])

    reactome_concepts = await reactome_loader.id.load_many(list(reactome_concept_ids))
    reactome_concept_map = {concept['conceptId']: concept for concept in reactome_concepts if concept}
    ensembl_concepts = await ensembl_loader.id.load_many(list(ensembl_concept_ids))
    ensembl_concept_map = {concept['conceptId']: concept for concept in ensembl_concepts if concept}

    def graphql_type(node_prefix: str, concept_id: str) -> str:
        prefix_value = ConceptPrefix(node_prefix)
        if prefix_value == ConceptPrefix.REACTOME:
            return type_to_reactome_concept_type(
                reactome_concept_map[concept_id]['conceptTypes'][0],
            )
        if prefix_value == ConceptPrefix.ENSEMBL:
            return type_to_ensembl_concept_type(
                ensembl_concept_map[concept_id]['conceptTypes'][0],
            )
        return prefix_to_concept_type(prefix_value)

    return [
        {
            'length': length,
            'nodes': [
                {
                    # Concept is an interface so need to set the type manually
                    '__typename': graphql_type(node[0], node[1]),
                    'conceptId': node[1],
                    'prefix': node[0],
                } for node in nodes
            ]
        } for length, nodes in paths
    ]


async def resolve_concept_annotated_concepts(obj,
                                             info,
                                             source_prefix: ConceptPrefix,
                                             target_prefix: ConceptPrefix,
                                             ) -> list[dict]:
    """
    Resolve the 'annotatedConcepts' field for a concept.
    :param obj: The GraphQL passed-in object that represent the concept being resolved.
    :param info: The GraphQL resolver info.
    :param source_prefix: The source vocabulary prefix.
    :param target_prefix: The target vocabulary prefix.
    :return: A list of annotated concepts.
    """
    concept_id = obj['conceptId']
    data_loader: DataLoader = info.context['data_loader']
    concept_loader = data_loader.get_concept_loader(source_prefix)
    annotation_loader = concept_loader.get_mapping_loader(target_prefix)

    mapped_concepts = await annotation_loader.load(concept_id)

    return [
        {'conceptId': mapped_id} for mapped_id in mapped_concepts
    ]


async def resolve_get_concept(info,
                              concept_id: str,
                              prefix: ConceptPrefix,
                              ) -> dict:
    """
    Resolve a concept by its ID.
    :param info: The GraphQL resolver info.
    :param concept_id: The ID of the concept to retrieve.
    :param prefix: The vocabulary prefix.
    :return: A dictionary containing the concept data or an error message.
    """
    data_loader: DataLoader = info.context['data_loader']
    concept_loader = data_loader.get_concept_loader(prefix)
    concept = await concept_loader.id.load(concept_id)

    if not concept:
        return assemble_response(
            error_str=f'Concept not found for ID: {concept_id} in {prefix.value}',
            error_code=404,
        )

    return assemble_response(concept)


async def resolve_auto_complete(info,
                                query: str,
                                prefix: ConceptPrefix,
                                limit: int = None,
                                ) -> dict:
    """
    Resolve auto-completion for concepts.

    Auto-completion request cannot be repeated within the same GraphQL request,
    and does not link to term identity, so no need for data loader.
    :param info: The GraphQL resolver info.
    :param query: The search query string.
    :param prefix: The vocabulary prefix.
    :param limit: Maximum number of concepts to return.
    :return: A dictionary containing the auto-complete results.
    """
    doc_db: DocumentDatabase = info.context['doc_db']
    config = get_vocabulary_config(prefix)

    concepts = await doc_db.auto_complete_search(
        prefix=prefix,
        query=query,
        limit=limit,
        model_class=config['conceptClass'],
    )

    results = [
        concept.model_dump(exclude_none=True) for concept in concepts
    ]

    return assemble_response(results)
