from types import SimpleNamespace

import pytest
from ariadne import make_executable_schema

from bioterms.etc.enums import ConceptPrefix, ConceptStatus
from bioterms.graphql_api import (
    _ANNOTATION_GRAPHQL_SCHEMAS,
    _VOCABULARY_GRAPHQL_MODULES,
    _load_annotation_graphql_module,
    _load_vocabulary_graphql_module,
)
from bioterms.graphql_api.resolver import utils as resolver_utils
from bioterms.graphql_api.resolver.utils import GRAPHQL_QUERY_TYPE, prefix_to_concept_type
from bioterms.graphql_api.schemas import CONCEPT_SCHEMA
from bioterms.model.concept import Concept
from bioterms.search import SearchExecution, SearchHit
from bioterms.vocabulary import get_vocabulary_license
from bioterms.vocabulary.utils import get_vocabulary_module


def _declared_annotation_pairs():
    pairs = set()
    for prefix in ConceptPrefix:
        for annotated_prefix in get_vocabulary_module(prefix).ANNOTATIONS:
            pairs.add(tuple(sorted((prefix, annotated_prefix), key=lambda value: value.value)))
    return pairs


def test_graphql_annotations_match_declared_capabilities():
    assert set(_ANNOTATION_GRAPHQL_SCHEMAS) == _declared_annotation_pairs()


def test_complete_graphql_schema_builds():
    schemas = [CONCEPT_SCHEMA]
    objects = []
    queries = [GRAPHQL_QUERY_TYPE]

    for prefix in _VOCABULARY_GRAPHQL_MODULES:
        _load_vocabulary_graphql_module(prefix, schemas, objects, queries)
    for pair in _ANNOTATION_GRAPHQL_SCHEMAS:
        _load_annotation_graphql_module(pair, schemas)

    flat_objects = []
    for graphql_object in objects:
        flat_objects.extend(graphql_object if isinstance(graphql_object, list) else [graphql_object])

    schema = make_executable_schema(schemas, flat_objects, queries)

    assert 'search' in schema.query_type.fields


def test_search_replaces_vocabulary_scoped_search_fields():
    schemas = [CONCEPT_SCHEMA]
    objects = []
    queries = [GRAPHQL_QUERY_TYPE]
    for prefix in _VOCABULARY_GRAPHQL_MODULES:
        _load_vocabulary_graphql_module(prefix, schemas, objects, queries)
    for pair in _ANNOTATION_GRAPHQL_SCHEMAS:
        _load_annotation_graphql_module(pair, schemas)

    flat_objects = []
    for graphql_object in objects:
        flat_objects.extend(graphql_object if isinstance(graphql_object, list) else [graphql_object])
    schema = make_executable_schema(schemas, flat_objects, queries)

    assert 'search' in schema.query_type.fields
    for field in schema.query_type.fields.values():
        nested_type = getattr(field.type, 'name', None)
        if nested_type and nested_type.endswith('Query'):
            assert 'search' not in schema.get_type(nested_type).fields


@pytest.mark.asyncio
async def test_root_search_exposes_v2_results(monkeypatch):
    concept = Concept(
        prefix=ConceptPrefix.HPO,
        conceptId='0004322',
        label='Short stature',
        status=ConceptStatus.ACTIVE,
    )

    async def fake_execute_hybrid_search(**kwargs):
        assert kwargs['prefixes'] == [ConceptPrefix.HPO, ConceptPrefix.MONDO]
        assert kwargs['limit'] == 5
        return SearchExecution(
            hits=[SearchHit(concept, exact=True, match_field='label',
                            matched_text='Short stature')],
            vector_used=True,
            reranker_used=False,
        )

    monkeypatch.setattr(resolver_utils, 'execute_hybrid_search', fake_execute_hybrid_search)
    info = SimpleNamespace(context={'doc_db': object(), 'vector_db': object()})

    response = await resolver_utils.resolve_global_search(
        None, info, ' Short stature ', ['hpo', 'mondo', 'hpo'], 5, True,
    )

    assert response['query'] == 'Short stature'
    assert response['results'][0] == {
        'rank': 1,
        'concept': {
            'conceptTypes': [],
            'prefix': 'hpo',
            'conceptId': '0004322',
            'label': 'Short stature',
            'status': 'active',
            '__typename': 'HpoConcept',
        },
        'match': {
            'type': 'exact',
            'exact': True,
            'field': 'label',
            'text': 'Short stature',
        },
    }
    assert response['meta']['vocabularies'] == [ConceptPrefix.HPO, ConceptPrefix.MONDO]
    assert response['meta']['pipeline'] == {
        'lexical': True, 'vector': True, 'mapped': False, 'reranker': False,
    }


def test_all_vocabulary_graphql_types_and_licenses_are_available():
    assert set(_VOCABULARY_GRAPHQL_MODULES) == set(ConceptPrefix)
    for prefix in ConceptPrefix:
        assert prefix_to_concept_type(prefix)
        assert get_vocabulary_license(prefix)
