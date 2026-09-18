from ariadne import make_executable_schema

from bioterms.etc.enums import ConceptPrefix
from bioterms.graphql_api import (
    _ANNOTATION_GRAPHQL_SCHEMAS,
    _VOCABULARY_GRAPHQL_MODULES,
    _load_annotation_graphql_module,
    _load_vocabulary_graphql_module,
)
from bioterms.graphql_api.resolver.utils import GRAPHQL_QUERY_TYPE, prefix_to_concept_type
from bioterms.graphql_api.schemas import CONCEPT_SCHEMA
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

    make_executable_schema(schemas, flat_objects, queries)


def test_all_vocabulary_graphql_types_and_licenses_are_available():
    assert set(_VOCABULARY_GRAPHQL_MODULES) == set(ConceptPrefix)
    for prefix in ConceptPrefix:
        assert prefix_to_concept_type(prefix)
        assert get_vocabulary_license(prefix)
