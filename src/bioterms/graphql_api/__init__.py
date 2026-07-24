"""
Module containing the GraphQL API implementation using Ariadne.
"""

import importlib
from typing import Any
from ariadne import ObjectType, make_executable_schema
from ariadne.asgi import GraphQL
from ariadne.graphql import GraphQLSchema
from ariadne.explorer import ExplorerGraphiQL
from ariadne.utils import convert_camel_case_to_snake
from starlette.types import ASGIApp
from fastapi import Request
from fastapi.middleware.cors import CORSMiddleware

from bioterms.etc.enums import ConceptPrefix
from bioterms.database import get_active_cache, get_active_doc_db, get_active_graph_db, get_active_vector_db
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
    vector_db = get_active_vector_db()

    data_loader = DataLoader(
        doc_db=doc_db,
        graph_db=graph_db,
    )

    context_value: dict[str, Any] = {
        'request': request,
        'cache': cache,
        'doc_db': doc_db,
        'graph_db': graph_db,
        'vector_db': vector_db,
        'data_loader': data_loader,
    }

    return context_value


# Maps a vocabulary prefix to its (schema constant name, resolver module name, concept object
# constant name(s), query object constant name) in bioterms.graphql_api.schemas / .resolver.
_VOCABULARY_GRAPHQL_MODULES: dict[ConceptPrefix, tuple[str, str, list[str], str]] = {
    ConceptPrefix.CTV3: ('CTV3_SCHEMA', 'ctv3', ['CTV3_CONCEPT'], 'CTV3_QUERY'),
    ConceptPrefix.ENSEMBL: ('ENSEMBL_SCHEMA', 'ensembl', ['ENSEMBL_CONCEPT'], 'ENSEMBL_QUERY'),
    ConceptPrefix.HGNC: ('HGNC_SCHEMA', 'hgnc', ['HGNC_CONCEPT'], 'HGNC_QUERY'),
    ConceptPrefix.HGNC_SYMBOL: ('GENE_SCHEMA', 'gene', ['GENE_CONCEPT'], 'GENE_QUERY'),
    ConceptPrefix.HPO: ('HPO_SCHEMA', 'hpo', ['HPO_CONCEPT'], 'HPO_QUERY'),
    ConceptPrefix.MONDO: ('MONDO_SCHEMA', 'mondo', ['MONDO_CONCEPT'], 'MONDO_QUERY'),
    ConceptPrefix.NCIT: ('NCIT_SCHEMA', 'ncit', ['NCIT_CONCEPT'], 'NCIT_QUERY'),
    ConceptPrefix.OHDSI: ('OHDSI_SCHEMA', 'ohdsi', ['OHDSI_CONCEPT'], 'OHDSI_QUERY'),
    ConceptPrefix.OMIM: ('OMIM_SCHEMA', 'omim', ['OMIM_CONCEPT'], 'OMIM_QUERY'),
    ConceptPrefix.ORDO: ('ORDO_SCHEMA', 'ordo', ['ORDO_CONCEPT'], 'ORDO_QUERY'),
    ConceptPrefix.REACTOME: (
        'REACTOME_SCHEMA', 'reactome',
        ['REACTOME_CONCEPT', 'REACTOME_PATHWAY', 'REACTOME_REACTION', 'REACTOME_GENE'],
        'REACTOME_QUERY',
    ),
    ConceptPrefix.SNOMED: ('SNOMED_SCHEMA', 'snomed', ['SNOMED_CONCEPT'], 'SNOMED_QUERY'),
}

# Maps an annotation prefix pair to its (schema constant name, resolver module name). The
# resolver module is imported only for its side effect of registering field resolvers.
_ANNOTATION_GRAPHQL_SCHEMAS: dict[tuple[ConceptPrefix, ConceptPrefix], tuple[str, str]] = {
    (ConceptPrefix.HPO, ConceptPrefix.ORDO): ('HPO_ORDO_SCHEMA', 'hpo_ordo'),
    (ConceptPrefix.CTV3, ConceptPrefix.SNOMED): ('CTV3_SNOMED_SCHEMA', 'ctv3_snomed'),
    (ConceptPrefix.HGNC_SYMBOL, ConceptPrefix.HPO): ('GENE_HPO_SCHEMA', 'gene_hpo'),
    (ConceptPrefix.HGNC_SYMBOL, ConceptPrefix.NCIT): ('GENE_NCIT_SCHEMA', 'gene_ncit'),
    (ConceptPrefix.HGNC_SYMBOL, ConceptPrefix.OMIM): ('GENE_OMIM_SCHEMA', 'gene_omim'),
    (ConceptPrefix.HGNC_SYMBOL, ConceptPrefix.ORDO): ('GENE_ORDO_SCHEMA', 'gene_ordo'),
    (ConceptPrefix.HGNC, ConceptPrefix.MONDO): ('HGNC_MONDO_SCHEMA', 'hgnc_mondo'),
    (ConceptPrefix.HPO, ConceptPrefix.MONDO): ('HPO_MONDO_SCHEMA', 'hpo_mondo'),
    (ConceptPrefix.MONDO, ConceptPrefix.NCIT): ('MONDO_NCIT_SCHEMA', 'mondo_ncit'),
    (ConceptPrefix.MONDO, ConceptPrefix.OMIM): ('MONDO_OMIM_SCHEMA', 'mondo_omim'),
    (ConceptPrefix.MONDO, ConceptPrefix.ORDO): ('MONDO_ORDO_SCHEMA', 'mondo_ordo'),
    (ConceptPrefix.MONDO, ConceptPrefix.SNOMED): ('MONDO_SNOMED_SCHEMA', 'mondo_snomed'),
    (ConceptPrefix.NCIT, ConceptPrefix.OHDSI): ('NCIT_OHDSI_SCHEMA', 'ncit_ohdsi'),
    (ConceptPrefix.OHDSI, ConceptPrefix.SNOMED): ('OHDSI_SNOMED_SCHEMA', 'ohdsi_snomed'),
    (ConceptPrefix.OMIM, ConceptPrefix.ORDO): ('OMIM_ORDO_SCHEMA', 'omim_ordo'),
    (ConceptPrefix.ORDO, ConceptPrefix.SNOMED): ('ORDO_SNOMED_SCHEMA', 'ordo_snomed'),
}


def _load_vocabulary_graphql_module(prefix: ConceptPrefix,
                                    graphql_schemas: list,
                                    graphql_objects: list,
                                    graphql_queries: list,
                                    ):
    """
    Load and register the schema, resolvers, and query type for one vocabulary's GraphQL
    module, if it has one.
    :param prefix: The vocabulary prefix to load the GraphQL module for.
    :param graphql_schemas: The list of schemas to append the vocabulary's schema to.
    :param graphql_objects: The list of objects to append the vocabulary's concept object(s) to.
    :param graphql_queries: The list of query types to append the vocabulary's query type to.
    """
    module_config = _VOCABULARY_GRAPHQL_MODULES.get(prefix)
    if module_config is None:
        return

    schema_name, resolver_module_name, object_names, query_name = module_config

    schemas_module = importlib.import_module('bioterms.graphql_api.schemas')
    resolver_module = importlib.import_module(f'bioterms.graphql_api.resolver.{resolver_module_name}')

    graphql_schemas.append(getattr(schemas_module, schema_name))
    graphql_objects.extend(getattr(resolver_module, name) for name in object_names)
    graphql_queries.append(getattr(resolver_module, query_name))


def _load_annotation_graphql_module(pair: tuple[ConceptPrefix, ConceptPrefix],
                                    graphql_schemas: list,
                                    ):
    """
    Load and register the schema for one annotation pair's GraphQL module, if it has one.
    The resolver module is imported only for its side effect of registering field resolvers.
    :param pair: The annotation prefix pair to load the GraphQL module for.
    :param graphql_schemas: The list of schemas to append the annotation's schema to.
    """
    module_config = _ANNOTATION_GRAPHQL_SCHEMAS.get(pair)
    if module_config is None:
        return

    schema_name, resolver_module_name = module_config

    schemas_module = importlib.import_module('bioterms.graphql_api.schemas')
    importlib.import_module(f'bioterms.graphql_api.resolver.{resolver_module_name}')

    graphql_schemas.append(getattr(schemas_module, schema_name))


async def create_graphql_app() -> ASGIApp:
    """
    Create a GraphQL application using Ariadne.
    :return: An instance of GraphQL ASGI application in CORS middleware wrapper.
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
        (ConceptPrefix.HGNC, ConceptPrefix.MONDO),
        (ConceptPrefix.HPO, ConceptPrefix.MONDO),
        (ConceptPrefix.HPO, ConceptPrefix.ORDO),
        (ConceptPrefix.MONDO, ConceptPrefix.NCIT),
        (ConceptPrefix.MONDO, ConceptPrefix.OMIM),
        (ConceptPrefix.MONDO, ConceptPrefix.ORDO),
        (ConceptPrefix.MONDO, ConceptPrefix.SNOMED),
        (ConceptPrefix.NCIT, ConceptPrefix.OHDSI),
        (ConceptPrefix.OHDSI, ConceptPrefix.SNOMED),
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

    for prefix in _VOCABULARY_GRAPHQL_MODULES:
        if vocabulary_statuses[prefix].loaded:
            _load_vocabulary_graphql_module(prefix, graphql_schemas, graphql_objects, graphql_queries)

    # Replace the graphql objects if the annotation data is also loaded
    for pair in _ANNOTATION_GRAPHQL_SCHEMAS:
        if annotation_statuses[pair].loaded:
            _load_annotation_graphql_module(pair, graphql_schemas)

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
