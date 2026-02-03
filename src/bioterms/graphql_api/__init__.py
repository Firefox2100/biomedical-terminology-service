"""
Module containing the GraphQL API implementation using Ariadne.
"""

from typing import Any
from ariadne import ObjectType, make_executable_schema
from ariadne.asgi import GraphQL
from ariadne.graphql import GraphQLSchema
from ariadne.explorer import ExplorerGraphiQL
from ariadne.utils import convert_camel_case_to_snake
from fastapi import Request
from fastapi.middleware.cors import CORSMiddleware

from bioterms.etc.enums import ConceptPrefix
from bioterms.database import get_active_cache, get_active_doc_db, get_active_graph_db
from bioterms.vocabulary import get_vocabulary_status
from bioterms.annotation import get_annotation_status
from bioterms.model.vocabulary_status import VocabularyStatus
from bioterms.model.annotation_status import AnnotationStatus
from .data_loader import DataLoader


def _convert_schema_names(graphql_name: str,
                          _: GraphQLSchema,
                          path: tuple[str, ...],
                          ) -> str:
    """
    Convert GraphQL schema names from camelCase to snake_case for queries and
    mutations only.
    :param graphql_name: The original GraphQL name
    :param _: The GraphQL schema, not used
    :param path: The path in the schema
    :return: The converted name
    """
    if 'Query' not in path[0] and 'pathsTo' not in path:
        return graphql_name

    return convert_camel_case_to_snake(graphql_name)


async def get_context_value(request: Request,
                            _: dict,
                            ) -> dict:
    """
    Get the context values for GraphQL resolvers
    :param request: The FastAPI request object
    :param _: Raw JSON payload of the GraphQL request
    :return: A dictionary containing the context values
    """
    cache = get_active_cache()
    doc_db = await get_active_doc_db()
    graph_db = get_active_graph_db()

    data_loader = DataLoader(
        doc_db=doc_db,
        graph_db=graph_db,
    )

    context_value: dict[str, Any] = {
        'request': request,
        'cache': cache,
        'doc_db': doc_db,
        'graph_db': graph_db,
        'data_loader': data_loader,
    }

    return context_value


async def create_graphql_app() -> GraphQL:
    """
    Create a GraphQL application using Ariadne.
    :return: An instance of GraphQL ASGI application.
    """
    cache = get_active_cache()
    doc_db = await get_active_doc_db()
    graph_db = get_active_graph_db()

    graphql_schemas = []
    graphql_objects = []
    graphql_queries = []

    vocabulary_statuses: dict[ConceptPrefix, VocabularyStatus] = {
        prefix: await get_vocabulary_status(prefix, cache, doc_db, graph_db)
        for prefix in ConceptPrefix
    }
    supported_annotations = [
        (ConceptPrefix.CTV3, ConceptPrefix.SNOMED),
        (ConceptPrefix.HGNC_SYMBOL, ConceptPrefix.HPO),
        (ConceptPrefix.HGNC_SYMBOL, ConceptPrefix.NCIT),
        (ConceptPrefix.HGNC_SYMBOL, ConceptPrefix.OMIM),
        (ConceptPrefix.HGNC_SYMBOL, ConceptPrefix.ORDO),
        (ConceptPrefix.HPO, ConceptPrefix.ORDO),
        (ConceptPrefix.OMIM, ConceptPrefix.ORDO),
        (ConceptPrefix.ORDO, ConceptPrefix.SNOMED),
    ]
    annotation_statuses: dict[tuple[ConceptPrefix, ConceptPrefix], AnnotationStatus] = {
        (prefix_1, prefix_2): await get_annotation_status(
            prefix_1=prefix_1,
            prefix_2=prefix_2,
            cache=cache,
            graph_db=graph_db,
        )
        for prefix_1, prefix_2 in supported_annotations
    }

    if vocabulary_statuses[ConceptPrefix.CTV3].loaded:
        from .schemas import CTV3_SCHEMA
        from .resolver.ctv3 import CTV3_CONCEPT, CTV3_QUERY

        graphql_schemas.append(CTV3_SCHEMA)
        graphql_objects.append(CTV3_CONCEPT)
        graphql_queries.append(CTV3_QUERY)
    if vocabulary_statuses[ConceptPrefix.ENSEMBL].loaded:
        from .schemas import ENSEMBL_SCHEMA
        from .resolver.ensembl import ENSEMBL_CONCEPT, ENSEMBL_QUERY

        graphql_schemas.append(ENSEMBL_SCHEMA)
        graphql_objects.append(ENSEMBL_CONCEPT)
        graphql_queries.append(ENSEMBL_QUERY)
    if vocabulary_statuses[ConceptPrefix.HGNC].loaded:
        from .schemas import HGNC_SCHEMA
        from .resolver.hgnc import HGNC_CONCEPT, HGNC_QUERY

        graphql_schemas.append(HGNC_SCHEMA)
        graphql_objects.append(HGNC_CONCEPT)
        graphql_queries.append(HGNC_QUERY)
    if vocabulary_statuses[ConceptPrefix.HGNC_SYMBOL].loaded:
        from .schemas import GENE_SCHEMA
        from .resolver.gene import GENE_CONCEPT, GENE_QUERY

        graphql_schemas.append(GENE_SCHEMA)
        graphql_objects.append(GENE_CONCEPT)
        graphql_queries.append(GENE_QUERY)
    if vocabulary_statuses[ConceptPrefix.HPO].loaded:
        from .schemas import HPO_SCHEMA
        from .resolver.hpo import HPO_CONCEPT, HPO_QUERY

        graphql_schemas.append(HPO_SCHEMA)
        graphql_objects.append(HPO_CONCEPT)
        graphql_queries.append(HPO_QUERY)
    if vocabulary_statuses[ConceptPrefix.NCIT].loaded:
        from .schemas import NCIT_SCHEMA
        from .resolver.ncit import NCIT_CONCEPT, NCIT_QUERY

        graphql_schemas.append(NCIT_SCHEMA)
        graphql_objects.append(NCIT_CONCEPT)
        graphql_queries.append(NCIT_QUERY)
    if vocabulary_statuses[ConceptPrefix.OMIM].loaded:
        from .schemas import OMIM_SCHEMA
        from .resolver.omim import OMIM_CONCEPT, OMIM_QUERY

        graphql_schemas.append(OMIM_SCHEMA)
        graphql_objects.append(OMIM_CONCEPT)
        graphql_queries.append(OMIM_QUERY)
    if vocabulary_statuses[ConceptPrefix.ORDO].loaded:
        from .schemas import ORDO_SCHEMA
        from .resolver.ordo import ORDO_CONCEPT, ORDO_QUERY

        graphql_schemas.append(ORDO_SCHEMA)
        graphql_objects.append(ORDO_CONCEPT)
        graphql_queries.append(ORDO_QUERY)
    if vocabulary_statuses[ConceptPrefix.REACTOME].loaded:
        from .schemas import REACTOME_SCHEMA
        from .resolver.reactome import REACTOME_PATHWAY, REACTOME_REACTION, REACTOME_GENE, \
            REACTOME_QUERY, REACTOME_CONCEPT

        graphql_schemas.append(REACTOME_SCHEMA)
        graphql_objects.extend([
            REACTOME_CONCEPT,
            REACTOME_PATHWAY,
            REACTOME_REACTION,
            REACTOME_GENE,
        ])
        graphql_queries.append(REACTOME_QUERY)
    if vocabulary_statuses[ConceptPrefix.SNOMED].loaded:
        from .schemas import SNOMED_SCHEMA
        from .resolver.snomed import SNOMED_CONCEPT, SNOMED_QUERY

        graphql_schemas.append(SNOMED_SCHEMA)
        graphql_objects.append(SNOMED_CONCEPT)
        graphql_queries.append(SNOMED_QUERY)

    # Replace the graphql objects if the annotation data is also loaded
    if annotation_statuses[(ConceptPrefix.HPO, ConceptPrefix.ORDO)].loaded:
        from .schemas import HPO_ORDO_SCHEMA
        import bioterms.graphql_api.resolver.hpo_ordo

        graphql_schemas.append(HPO_ORDO_SCHEMA)
    if annotation_statuses[(ConceptPrefix.CTV3, ConceptPrefix.SNOMED)].loaded:
        from .schemas import CTV3_SNOMED_SCHEMA
        import bioterms.graphql_api.resolver.ctv3_snomed

        graphql_schemas.append(CTV3_SNOMED_SCHEMA)
    if annotation_statuses[(ConceptPrefix.HGNC_SYMBOL, ConceptPrefix.HPO)].loaded:
        from .schemas import GENE_HPO_SCHEMA
        import bioterms.graphql_api.resolver.gene_hpo

        graphql_schemas.append(GENE_HPO_SCHEMA)
    if annotation_statuses[(ConceptPrefix.HGNC_SYMBOL, ConceptPrefix.NCIT)].loaded:
        from .schemas import GENE_NCIT_SCHEMA
        import bioterms.graphql_api.resolver.gene_ncit

        graphql_schemas.append(GENE_NCIT_SCHEMA)
    if annotation_statuses[(ConceptPrefix.HGNC_SYMBOL, ConceptPrefix.OMIM)].loaded:
        from .schemas import GENE_OMIM_SCHEMA
        import bioterms.graphql_api.resolver.gene_omim

        graphql_schemas.append(GENE_OMIM_SCHEMA)
    if annotation_statuses[(ConceptPrefix.HGNC_SYMBOL, ConceptPrefix.ORDO)].loaded:
        from .schemas import GENE_ORDO_SCHEMA
        import bioterms.graphql_api.resolver.gene_ordo

        graphql_schemas.append(GENE_ORDO_SCHEMA)
    if annotation_statuses[(ConceptPrefix.OMIM, ConceptPrefix.ORDO)].loaded:
        from .schemas import OMIM_ORDO_SCHEMA
        import bioterms.graphql_api.resolver.omim_ordo

        graphql_schemas.append(OMIM_ORDO_SCHEMA)
    if annotation_statuses[(ConceptPrefix.ORDO, ConceptPrefix.SNOMED)].loaded:
        from .schemas import ORDO_SNOMED_SCHEMA
        import bioterms.graphql_api.resolver.ordo_snomed

        graphql_schemas.append(ORDO_SNOMED_SCHEMA)

    if graphql_schemas:
        from .schemas import CONCEPT_SCHEMA

        graphql_schemas.insert(0, CONCEPT_SCHEMA)

    if graphql_queries:
        from .resolver.utils import GRAPHQL_QUERY_TYPE

        graphql_queries.insert(0, GRAPHQL_QUERY_TYPE)

    graphql_object_list = []
    for obj in graphql_objects:
        if isinstance(obj, list):
            graphql_object_list.extend(obj)
        else:
            graphql_object_list.append(obj)

    schema = make_executable_schema(
        graphql_schemas,
        graphql_object_list,
        graphql_queries,
        convert_names_case=_convert_schema_names,
    )

    graphql_app = GraphQL(
        schema=schema,
        context_value=get_context_value,
        explorer=ExplorerGraphiQL(
            title='Biomedical Terminology Service GraphQL Explorer',
        ),
    )

    return CORSMiddleware(
        graphql_app,
        allow_origins=['*'],
        allow_credentials=False,
        allow_methods=("GET", "POST", "OPTIONS"),
    )
