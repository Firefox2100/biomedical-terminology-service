import json
import os

os.environ.setdefault('BTS_SERVER_HMAC_KEY', 'dGVzdC1obWFjLWtleQ==')
os.environ.setdefault('BTS_ENABLE_METRICS', 'false')

import pytest

from bioterms.etc.enums import AnnotationType, ConceptPrefix, ConceptRelationshipType, ConceptStatus, \
    SimilarityMethod
from bioterms.model.concept import Concept
from bioterms.model.concept_path import ConceptPath, NodeInPath
from bioterms.model.related_term import RelatedTerm
from bioterms.model.similar_term import SimilarTerm, SimilarTermByPrefix, SimilarTermWithScores
from bioterms.model.translated_term import TranslatedTerm
from bioterms.router import search as search_module
from bioterms.router.expand import ExpandRequestV1, expand_terms_v1, expand_terms_v2
from bioterms.router.map import MapRequestV1, map_terms_v1, map_terms_v2
from bioterms.router.search import search_terms_v1
from bioterms.router.similarity import SimilarityRequestV1, TranslateRequestV1, get_similar_terms_v1, \
    get_similar_terms_v2, translate_terms_v1, translate_terms_v2
from bioterms.router.trace import trace_terms_v1


async def collect_streaming_json(response):
    body = b''.join([chunk async for chunk in response.body_iterator])
    return json.loads(body.decode())


def make_concept(concept_id, label):
    return Concept(
        conceptTypes=[],
        prefix=ConceptPrefix.HPO,
        conceptId=concept_id,
        label=label,
        status=ConceptStatus.ACTIVE,
    )


class FakeDocumentDatabase:
    def __init__(self, concepts):
        self.concepts = concepts
        self.calls = []

    async def get_terms_by_ids_iter(self, prefix, concept_ids, model_class=Concept):
        self.calls.append({
            'prefix': prefix,
            'concept_ids': concept_ids,
            'model_class': model_class,
        })
        for concept_id in concept_ids:
            yield self.concepts[concept_id]


class FakeVectorDatabase:
    def __init__(self, concept_ids):
        self.concept_ids = concept_ids
        self.calls = []

    async def search_concepts(self, query, prefix, limit):
        self.calls.append({
            'query': query,
            'prefix': prefix,
            'limit': limit,
        })
        return self.concept_ids


class FakeGraphDatabase:
    def __init__(self):
        self.calls = []

    async def expand_terms_iter(self, prefix, concept_ids, max_depth=None, limit=None):
        self.calls.append({
            'method': 'expand_terms_iter',
            'prefix': prefix,
            'concept_ids': concept_ids,
            'max_depth': max_depth,
            'limit': limit,
        })
        yield RelatedTerm(conceptId=concept_ids[0], relatedConcepts=['child-1', 'child-2'])

    async def map_terms_iter(self, prefix, target_prefix, concept_ids, max_hops=1, limit=None):
        self.calls.append({
            'method': 'map_terms_iter',
            'prefix': prefix,
            'target_prefix': target_prefix,
            'concept_ids': concept_ids,
            'max_hops': max_hops,
            'limit': limit,
        })
        yield RelatedTerm(conceptId=concept_ids[0], relatedConcepts=['target-1'])

    async def get_similar_terms_iter(self, prefix, concept_ids, threshold=1.0, same_prefix=True,
                                     corpus_prefix=None, method=None, limit=None):
        self.calls.append({
            'method': 'get_similar_terms_iter',
            'prefix': prefix,
            'concept_ids': concept_ids,
            'threshold': threshold,
            'same_prefix': same_prefix,
            'corpus_prefix': corpus_prefix,
            'similarity_method': method,
            'limit': limit,
        })
        yield SimilarTerm(
            conceptId=concept_ids[0],
            similarGroups=[
                SimilarTermByPrefix(
                    prefix=ConceptPrefix.HPO,
                    similarConcepts=[
                        SimilarTermWithScores(
                            conceptId='similar-1',
                            similarity_scores={'relevance:hpo': 0.91},
                        ),
                    ],
                ),
            ],
        )

    async def translate_terms_iter(self, original_ids, original_prefix, constraint_ids,
                                   threshold=1.0, limit=None):
        self.calls.append({
            'method': 'translate_terms_iter',
            'original_ids': original_ids,
            'original_prefix': original_prefix,
            'constraint_ids': constraint_ids,
            'threshold': threshold,
            'limit': limit,
        })
        yield TranslatedTerm(
            conceptId='translated-1',
            prefix=ConceptPrefix.MONDO,
            score=0.88,
        )

    async def trace_term_iter(self, prefix_start, prefix_end, id_start, id_end,
                              relationship_type, forward=True, max_depth=12):
        self.calls.append({
            'method': 'trace_term_iter',
            'prefix_start': prefix_start,
            'prefix_end': prefix_end,
            'id_start': id_start,
            'id_end': id_end,
            'relationship_type': relationship_type,
            'forward': forward,
            'max_depth': max_depth,
        })
        yield ConceptPath(
            startConceptId=id_start,
            endConceptId=id_end,
            startPrefix=prefix_start,
            endPrefix=prefix_end,
            length=2,
            nodes=[
                NodeInPath(conceptId=id_start, prefix=prefix_start),
                NodeInPath(conceptId=id_end, prefix=prefix_end),
            ],
        )


@pytest.mark.asyncio
async def test_search_terms_v1_uses_vector_ids_to_stream_document_results(monkeypatch):
    concepts = {
        '0000001': make_concept('0000001', 'First Concept'),
        '0000002': make_concept('0000002', 'Second Concept'),
    }
    doc_db = FakeDocumentDatabase(concepts)
    vector_db = FakeVectorDatabase(['0000002', '0000001'])
    monkeypatch.setattr(search_module, 'get_vocabulary_config', lambda prefix: {'conceptClass': Concept})

    response = await search_terms_v1(
        prefix=ConceptPrefix.HPO,
        query='phenotype',
        limit=2,
        doc_db=doc_db,
        vector_db=vector_db,
    )
    body = await collect_streaming_json(response)

    assert [item['conceptId'] for item in body] == ['0000002', '0000001']
    assert vector_db.calls == [{
        'query': 'phenotype',
        'prefix': ConceptPrefix.HPO,
        'limit': 2,
    }]
    assert doc_db.calls == [{
        'prefix': ConceptPrefix.HPO,
        'concept_ids': ['0000002', '0000001'],
        'model_class': Concept,
    }]


@pytest.mark.asyncio
async def test_expand_terms_v1_maps_depth_zero_to_unbounded_query_and_v1_response():
    graph_db = FakeGraphDatabase()

    result = await expand_terms_v1(
        prefix=ConceptPrefix.HPO,
        requested_terms=ExpandRequestV1(termIds=['0000001']),
        depth=0,
        result_threshold=5,
        graph_db=graph_db,
    )

    assert result[0].term_id == '0000001'
    assert result[0].children == ['child-1', 'child-2']
    assert result[0].depth == 0
    assert graph_db.calls == [{
        'method': 'expand_terms_iter',
        'prefix': ConceptPrefix.HPO,
        'concept_ids': ['0000001'],
        'max_depth': None,
        'limit': 5,
    }]


@pytest.mark.asyncio
async def test_expand_terms_v2_streams_related_terms_with_depth_and_limit():
    graph_db = FakeGraphDatabase()

    response = await expand_terms_v2(
        prefix=ConceptPrefix.HPO,
        concept_ids=['0000001'],
        depth=3,
        limit=10,
        graph_db=graph_db,
    )
    body = await collect_streaming_json(response)

    assert body == [{
        'conceptId': '0000001',
        'relatedConcepts': ['child-1', 'child-2'],
    }]
    assert graph_db.calls[0]['max_depth'] == 3
    assert graph_db.calls[0]['limit'] == 10


@pytest.mark.asyncio
async def test_map_terms_v1_uses_result_threshold_as_limit_and_target_type():
    graph_db = FakeGraphDatabase()

    result = await map_terms_v1(
        prefix=ConceptPrefix.HPO,
        target_prefix=ConceptPrefix.MONDO,
        requested_terms=MapRequestV1(termIds=['0000001']),
        result_threshold=2,
        graph_db=graph_db,
    )

    assert result[0].term_id == '0000001'
    assert result[0].mapped_ids == ['target-1']
    assert result[0].target_type == ConceptPrefix.MONDO
    assert graph_db.calls == [{
        'method': 'map_terms_iter',
        'prefix': ConceptPrefix.HPO,
        'target_prefix': ConceptPrefix.MONDO,
        'concept_ids': ['0000001'],
        'max_hops': 1,
        'limit': 2,
    }]


@pytest.mark.asyncio
async def test_map_terms_v2_streams_related_terms_and_passes_max_hops():
    graph_db = FakeGraphDatabase()

    response = await map_terms_v2(
        prefix=ConceptPrefix.HPO,
        target_prefix=ConceptPrefix.MONDO,
        concept_ids=['0000001'],
        max_hops=3,
        limit=None,
        graph_db=graph_db,
    )
    body = await collect_streaming_json(response)

    assert body[0]['relatedConcepts'] == ['target-1']
    assert graph_db.calls[0]['max_hops'] == 3
    assert graph_db.calls[0]['limit'] is None


@pytest.mark.asyncio
async def test_get_similar_terms_v1_flattens_first_similarity_group():
    graph_db = FakeGraphDatabase()

    result = await get_similar_terms_v1(
        prefix=ConceptPrefix.HPO,
        requested_terms=SimilarityRequestV1(termIds=['0000001'], threshold=0.75),
        result_threshold=4,
        graph_db=graph_db,
    )

    assert result[0].term_id == '0000001'
    assert result[0].similar_ids == ['similar-1']
    assert result[0].similarity_threshold == 0.75
    assert result[0].threshold == 4
    assert graph_db.calls[0]['threshold'] == 0.75
    assert graph_db.calls[0]['limit'] == 4


@pytest.mark.asyncio
async def test_get_similar_terms_v2_passes_filtering_options_and_streams_results():
    graph_db = FakeGraphDatabase()

    response = await get_similar_terms_v2(
        prefix=ConceptPrefix.HPO,
        concept_ids=['0000001'],
        threshold=0.5,
        same_prefix=False,
        corpus=ConceptPrefix.MONDO,
        method=SimilarityMethod.RELEVANCE,
        limit=6,
        graph_db=graph_db,
    )
    body = await collect_streaming_json(response)

    assert body[0]['similarGroups'][0]['similarConcepts'][0]['conceptId'] == 'similar-1'
    assert graph_db.calls[0]['same_prefix'] is False
    assert graph_db.calls[0]['corpus_prefix'] == ConceptPrefix.MONDO
    assert graph_db.calls[0]['similarity_method'] == SimilarityMethod.RELEVANCE
    assert graph_db.calls[0]['limit'] == 6


@pytest.mark.asyncio
async def test_translate_terms_v1_uses_term_ids_constraints_and_score():
    graph_db = FakeGraphDatabase()

    result = await translate_terms_v1(
        prefix=ConceptPrefix.HPO,
        translate_request=TranslateRequestV1(
            termIds=['0000001'],
            constraintIds=['constraint-1'],
            threshold=0.8,
        ),
        result_threshold=3,
        graph_db=graph_db,
    )

    assert result[0].term_id == 'translated-1'
    assert result[0].score == 0.88
    assert graph_db.calls == [{
        'method': 'translate_terms_iter',
        'original_ids': ['0000001'],
        'original_prefix': ConceptPrefix.HPO,
        'constraint_ids': {ConceptPrefix.HPO: {'constraint-1'}},
        'threshold': 0.8,
        'limit': 3,
    }]


@pytest.mark.asyncio
async def test_translate_terms_v2_parses_prefixed_constraints_and_streams_results():
    graph_db = FakeGraphDatabase()

    response = await translate_terms_v2(
        prefix=ConceptPrefix.HPO,
        original_ids=['0000001'],
        constraint_concepts=['mondo:0001', 'hpo:0002'],
        threshold=0.7,
        limit=2,
        graph_db=graph_db,
    )
    body = await collect_streaming_json(response)

    assert body == [{
        'conceptId': 'translated-1',
        'prefix': 'mondo',
        'score': 0.88,
    }]
    assert graph_db.calls[0]['constraint_ids'] == {
        ConceptPrefix.MONDO: {'0001'},
        ConceptPrefix.HPO: {'0002'},
    }


@pytest.mark.asyncio
async def test_trace_terms_v1_passes_relationship_direction_and_streams_paths():
    graph_db = FakeGraphDatabase()

    response = await trace_terms_v1(
        prefix=ConceptPrefix.HPO,
        target_prefix=ConceptPrefix.MONDO,
        start_id='0000001',
        end_id='0000002',
        relationship=AnnotationType.EXACT,
        forward=False,
        max_hops=5,
        graph_db=graph_db,
    )
    body = await collect_streaming_json(response)

    assert body[0]['startConceptId'] == '0000001'
    assert body[0]['endConceptId'] == '0000002'
    assert body[0]['nodes'][1] == {'conceptId': '0000002', 'prefix': 'mondo'}
    assert graph_db.calls == [{
        'method': 'trace_term_iter',
        'prefix_start': ConceptPrefix.HPO,
        'prefix_end': ConceptPrefix.MONDO,
        'id_start': '0000001',
        'id_end': '0000002',
        'relationship_type': AnnotationType.EXACT,
        'forward': False,
        'max_depth': 5,
    }]
