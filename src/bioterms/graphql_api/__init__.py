from typing import Any
from ariadne import make_executable_schema
from ariadne.asgi import GraphQL
from ariadne.graphql import GraphQLSchema
from ariadne.explorer import ExplorerGraphiQL
from ariadne.utils import convert_camel_case_to_snake
from fastapi import Request

from bioterms.etc.enums import ConceptPrefix
from bioterms.database import get_active_doc_db, get_active_graph_db
from bioterms.vocabulary import get_vocabulary_status
from .data_loader import DataLoader


def _convert_schema_names(graphql_name: str,
                          _: GraphQLSchema,
                          path: tuple[str, ...],
                          ) -> str:
    """
    Convert GraphQL schema names from camelCase to snake_case for queries and
    :param graphql_name: The original GraphQL name
    :param _: The GraphQL schema, not used
    :param path: The path in the schema
    :return: The converted name
    """
    if 'Query' not in path[0]:
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
    doc_db = await get_active_doc_db()
    graph_db = get_active_graph_db()

    data_loader = DataLoader(
        doc_db=doc_db,
        graph_db=graph_db,
    )

    context_value: dict[str, Any] = {
        'request': request,
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
    doc_db = await get_active_doc_db()

    graphql_schemas = []
    graphql_objects = []
    graphql_queries = []

    if (await get_vocabulary_status(ConceptPrefix.HPO, doc_db)).loaded:
        from .schemas import HPO_SCHEMA
        from .resolver.hpo import HPO_CONCEPT, HPO_QUERY

        graphql_schemas.append(HPO_SCHEMA)
        graphql_objects.append(HPO_CONCEPT)
        graphql_queries.append(HPO_QUERY)
    if (await get_vocabulary_status(ConceptPrefix.ORDO, doc_db)).loaded:
        from .schemas import ORDO_SCHEMA
        from .resolver.ordo import ORDO_CONCEPT, ORDO_QUERY

        graphql_schemas.append(ORDO_SCHEMA)
        graphql_objects.append(ORDO_CONCEPT)
        graphql_queries.append(ORDO_QUERY)

    if graphql_schemas:
        from .schemas import CONCEPT_SCHEMA

        graphql_schemas.insert(0, CONCEPT_SCHEMA)

    if graphql_queries:
        from .resolver.utils import GRAPHQL_QUERY_TYPE

        graphql_queries.insert(0, GRAPHQL_QUERY_TYPE)

    schema = make_executable_schema(
        graphql_schemas,
        graphql_objects,
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

    return graphql_app
