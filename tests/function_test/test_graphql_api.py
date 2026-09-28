"""
End-to-end GraphQL tests: the real schema from `create_graphql_app()` (every vocabulary and
annotation reported as loaded) is queried over HTTP, so resolvers, data loaders and the
interface-type mapping run together against in-memory database fakes.
"""
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

import bioterms.graphql_api as graphql_api
from bioterms.etc.enums import ConceptPrefix
from bioterms.graphql_api.resolver import utils as resolver_utils
from bioterms.model.annotation_status import AnnotationStatus
from bioterms.model.vocabulary_status import VocabularyStatus
from bioterms.search import SearchExecution, SearchHit
from bioterms.vocabulary import get_vocabulary_config


def _concept(prefix, concept_id, label, **extra):
    model = get_vocabulary_config(prefix)['conceptClass']
    return model(prefix=prefix, conceptId=concept_id, label=label, **extra)


CONCEPTS = {
    (ConceptPrefix.HPO, 'HP:0000118'): _concept(ConceptPrefix.HPO, 'HP:0000118', 'Phenotypic abnormality'),
    (ConceptPrefix.HPO, 'HP:0000478'): _concept(ConceptPrefix.HPO, 'HP:0000478', 'Abnormality of the eye'),
    (ConceptPrefix.HPO, 'HP:0000001'): _concept(ConceptPrefix.HPO, 'HP:0000001', 'All'),
    (ConceptPrefix.MONDO, 'MONDO:0000001'): _concept(ConceptPrefix.MONDO, 'MONDO:0000001', 'disease'),
    (ConceptPrefix.GO, 'GO:0008150'): _concept(ConceptPrefix.GO, 'GO:0008150', 'biological_process'),
    (ConceptPrefix.REACTOME, 'R-HSA-1'): _concept(ConceptPrefix.REACTOME, 'R-HSA-1', 'Signal pathway',
                                                   conceptTypes=['pathway']),
    (ConceptPrefix.REACTOME, 'R-HSA-2'): _concept(ConceptPrefix.REACTOME, 'R-HSA-2', 'Sub pathway',
                                                   conceptTypes=['pathway']),
    (ConceptPrefix.REACTOME, 'R-HSA-10'): _concept(ConceptPrefix.REACTOME, 'R-HSA-10', 'Binding',
                                                    conceptTypes=['reaction']),
    (ConceptPrefix.REACTOME, 'R-HSA-11'): _concept(ConceptPrefix.REACTOME, 'R-HSA-11', 'Release',
                                                    conceptTypes=['reaction']),
    (ConceptPrefix.REACTOME, 'R-HSA-20'): _concept(ConceptPrefix.REACTOME, 'R-HSA-20', 'TP53',
                                                    conceptTypes=['gene']),
}

# (method, concept_id) -> related concept IDs, answered by every graph relationship method.
RELATIONS = {
    ('expand_terms', 'HP:0000118'): ['HP:0000478'],
    ('trace_ancestors', 'HP:0000118'): ['HP:0000001'],
    ('get_replaced_terms', 'HP:0000118'): ['HP:0009999'],
    ('get_replacing_terms', 'HP:0000118'): [],
    ('map_terms', 'HP:0000118'): ['MONDO:0000001'],
    ('map_terms', 'GO:0008150'): ['R-HSA-1', 'R-HSA-20'],
    ('get_sub_pathways', 'R-HSA-1'): ['R-HSA-2'],
    ('get_super_pathways', 'R-HSA-2'): ['R-HSA-1'],
    ('get_reactions_in_pathway', 'R-HSA-1'): ['R-HSA-10'],
    ('get_subsequent_reactions', 'R-HSA-10'): ['R-HSA-11'],
    ('get_preceding_reactions', 'R-HSA-11'): ['R-HSA-10'],
    ('get_reaction_inputs', 'R-HSA-10'): ['R-HSA-20'],
    ('get_reaction_outputs', 'R-HSA-11'): ['R-HSA-20'],
    ('get_gene_input_reactions', 'R-HSA-20'): ['R-HSA-10'],
    ('get_gene_output_reactions', 'R-HSA-20'): ['R-HSA-11'],
}


class _Relations:
    """Answers any `graph_db.<method>(concept_ids=...)` call from RELATIONS."""

    def __init__(self):
        self.calls = []

    def __getattr__(self, method):
        async def lookup(*args, concept_ids=None, **_kwargs):
            concept_ids = concept_ids if concept_ids is not None else args[-1]
            self.calls.append((method, tuple(concept_ids)))
            return [SimpleNamespace(concept_id=c, related_concepts=RELATIONS.get((method, c), []))
                    for c in concept_ids]
        return lookup


class FakeGraphDb(_Relations):
    def __init__(self):
        super().__init__()
        self.reactome = _Relations()

    async def get_similar_terms_aggregate(self, prefix, similarity_queries):
        return [SimpleNamespace(concept_id=cid, similar_concepts=[('HP:0000478', 0.8)])
                for cid, threshold in similarity_queries if threshold <= 0.8]

    async def trace_term_aggregate(self, trace_queries):
        node = lambda prefix, cid: SimpleNamespace(prefix=prefix, concept_id=cid)
        return [SimpleNamespace(start_concept_id=q[1], length=2, nodes=[
            node(ConceptPrefix.HPO, q[1]), node(ConceptPrefix.REACTOME, 'R-HSA-20'),
            node(ConceptPrefix.MONDO, 'MONDO:0000001'),
        ]) for q in trace_queries]


class FakeDocDb:
    def __init__(self):
        self.batches = []

    async def get_terms_by_ids(self, prefix, concept_ids, model_class):
        self.batches.append((prefix, tuple(concept_ids)))
        return [CONCEPTS[(prefix, c)] for c in concept_ids if (prefix, c) in CONCEPTS]

    async def auto_complete_search(self, prefix, query, limit, model_class):
        return [c for (p, _), c in CONCEPTS.items() if p == prefix and query.lower() in c.label.lower()][:limit]


@pytest.fixture
def graphql(monkeypatch):
    doc_db, graph_db = FakeDocDb(), FakeGraphDb()

    async def vocabulary_status(prefix, *_args, **_kwargs):
        return VocabularyStatus(prefix=prefix, name=prefix.value, fileDownloaded=True, loaded=True,
                                conceptCount=1, relationshipCount=1, vectorCount=0,
                                annotations=[], similarityMethods=[])

    async def annotation_status(prefix_1, prefix_2, **_kwargs):
        return AnnotationStatus(prefixSource=prefix_1, prefixTarget=prefix_2, name='x',
                                loaded=True, relationshipCount=1)

    async def active_doc_db():
        return doc_db

    for target in (graphql_api, resolver_utils):
        monkeypatch.setattr(target, 'get_vocabulary_status', vocabulary_status, raising=False)
    monkeypatch.setattr(graphql_api, 'get_annotation_status', annotation_status)
    monkeypatch.setattr(graphql_api, 'get_active_doc_db', active_doc_db)
    monkeypatch.setattr(graphql_api, 'get_active_graph_db', lambda: graph_db)
    monkeypatch.setattr(graphql_api, 'get_active_cache', lambda: object())
    monkeypatch.setattr(graphql_api, 'get_active_vector_db', lambda: object())

    import asyncio
    app = asyncio.run(graphql_api.create_graphql_app())
    client = TestClient(app)

    def run(query, **variables):
        response = client.post('/', json={'query': query, 'variables': variables})
        body = response.json()
        assert 'errors' not in body, body.get('errors')
        return body['data']

    return run, doc_db, graph_db


def test_hpo_concept_with_hierarchy_replacements_similarity_and_mappings(graphql):
    run, doc_db, graph_db = graphql

    data = run('''{ hpo { hpoConcept(conceptId: "HP:0000118") {
        error { code }
        data {
            conceptId label status prefix
            children { conceptId label }
            parents { conceptId label }
            replaces { conceptId status prefix }
            replacedBy { conceptId }
            similarConcepts(threshold: 0.5) { from { conceptId } to { conceptId label } score }
            annotatedMondo { conceptId label }
        } } } }''')['hpo']['hpoConcept']

    concept = data['data']
    assert data['error'] is None
    assert (concept['label'], concept['status'], concept['prefix']) == ('Phenotypic abnormality', 'active', 'hpo')
    assert concept['children'] == [{'conceptId': 'HP:0000478', 'label': 'Abnormality of the eye'}]
    assert concept['parents'] == [{'conceptId': 'HP:0000001', 'label': 'All'}]
    # A replaced concept missing from the document store still resolves its fallbacks.
    assert concept['replaces'] == [{'conceptId': 'HP:0009999', 'status': 'unknown', 'prefix': 'hpo'}]
    assert concept['replacedBy'] == []
    assert concept['similarConcepts'] == [{
        'from': {'conceptId': 'HP:0000118'},
        'to': {'conceptId': 'HP:0000478', 'label': 'Abnormality of the eye'}, 'score': 0.8,
    }]
    assert concept['annotatedMondo'] == [{'conceptId': 'MONDO:0000001', 'label': 'disease'}]
    # Every HPO field lookup for this request was batched through one data loader.
    assert (ConceptPrefix.HPO, ('HP:0000118',)) == doc_db.batches[0]


def test_missing_concept_returns_error_payload(graphql):
    run, *_ = graphql

    data = run('{ hpo { hpoConcept(conceptId: "HP:404") { error { code message } data { conceptId } } } }')

    assert data['hpo']['hpoConcept'] == {
        'error': {'code': '404', 'message': 'Concept not found for ID: HP:404 in hpo'}, 'data': None,
    }


def test_paths_to_types_mixed_vocabulary_nodes(graphql):
    run, *_ = graphql

    data = run('''{ hpo { hpoConcept(conceptId: "HP:0000118") { data {
        pathsTo(targetPrefix: mondo, targetConceptId: "MONDO:0000001", relationship: annotated_with,
                direction: undirected) {
            length nodes { __typename conceptId prefix }
        } } } } }''')

    [path] = data['hpo']['hpoConcept']['data']['pathsTo']
    assert path['length'] == 2
    assert [n['__typename'] for n in path['nodes']] == ['HpoConcept', 'ReactomeGene', 'MondoConcept']


def test_reactome_pathway_reaction_and_gene_graph(graphql):
    run, _, graph_db = graphql

    pathway = run('''{ reactome { reactomeConcept(conceptId: "R-HSA-1") { data {
        __typename
        ... on ReactomePathway {
            subPathways { conceptId superPathways { conceptId } }
            reactions {
                conceptId
                subsequentReactions { conceptId precedingReactions { conceptId } }
                inputs { __typename conceptId ... on ReactomeGene { isInput { conceptId } isOutput { conceptId } } }
            }
        } } } } }''')['reactome']['reactomeConcept']['data']

    assert pathway['__typename'] == 'ReactomePathway'
    assert pathway['subPathways'] == [{'conceptId': 'R-HSA-2', 'superPathways': [{'conceptId': 'R-HSA-1'}]}]
    [reaction] = pathway['reactions']
    assert reaction['subsequentReactions'] == [{'conceptId': 'R-HSA-11', 'precedingReactions': [{'conceptId': 'R-HSA-10'}]}]
    assert reaction['inputs'] == [{'__typename': 'ReactomeGene', 'conceptId': 'R-HSA-20',
                                   'isInput': [{'conceptId': 'R-HSA-10'}], 'isOutput': [{'conceptId': 'R-HSA-11'}]}]
    assert ('get_sub_pathways', ('R-HSA-1',)) in graph_db.reactome.calls


def test_auto_complete_and_loaded_prefixes(graphql):
    run, *_ = graphql

    data = run('''{ loadedPrefixes
        hpo { autoComplete(query: "abnormal", limit: 5) { error { code } data { conceptId } } } }''')

    assert set(data['loadedPrefixes']) == {p.value for p in ConceptPrefix}
    assert {c['conceptId'] for c in data['hpo']['autoComplete']['data']} == {'HP:0000118', 'HP:0000478'}


def test_global_search_reports_ranked_hits_and_pipeline(graphql, monkeypatch):
    run, *_ = graphql

    async def fake_search(**kwargs):
        assert kwargs['prefixes'] == [ConceptPrefix.HPO, ConceptPrefix.MONDO]
        return SearchExecution(
            hits=[SearchHit(concept=CONCEPTS[(ConceptPrefix.HPO, 'HP:0000118')], exact=True,
                            match_field='label', matched_text='Phenotypic abnormality')],
            fuzzy_used=False, vector_used=True, mapped_used=False, reranker_used=False,
        )

    monkeypatch.setattr(resolver_utils, 'execute_hybrid_search', fake_search)

    data = run('''{ search(query: " phenotypic ", vocabularies: [hpo, mondo, hpo], limit: 3) {
        query results { rank match { type exact field } concept { __typename conceptId } }
        meta { returned limit vocabularies pipeline { lexical vector fuzzy } } } }''')['search']

    assert data['query'] == 'phenotypic'
    assert data['results'] == [{'rank': 1, 'match': {'type': 'exact', 'exact': True, 'field': 'label'},
                                'concept': {'__typename': 'HpoConcept', 'conceptId': 'HP:0000118'}}]
    assert data['meta']['vocabularies'] == ['hpo', 'mondo']
    assert data['meta']['pipeline'] == {'lexical': True, 'vector': True, 'fuzzy': False}


@pytest.mark.parametrize(('query', 'message'), [
    ('{ search(query: "  ", vocabularies: [hpo]) { query } }', 'query must not be empty'),
    ('{ search(query: "x", vocabularies: [hpo], limit: 0) { query } }', 'limit must be between'),
    ('{ search(query: "x", vocabularies: []) { query } }', 'at least one vocabulary'),
])
def test_global_search_validates_arguments(graphql, query, message):
    run, *_ = graphql

    with pytest.raises(AssertionError, match=message):
        run(query)


def test_type_mapping_helpers_reject_unknown_values():
    with pytest.raises(ValueError):
        resolver_utils.type_to_reactome_concept_type('complex')
    with pytest.raises(ValueError):
        resolver_utils.type_to_ensembl_concept_type('operon')
    assert resolver_utils.type_to_ensembl_concept_type('exon') == 'EnsemblExon'
    assert resolver_utils.assemble_response(error_str='bad') == {'data': None, 'error': {'message': 'bad', 'code': 400}}


def test_graphql_prefix_enum_covers_every_vocabulary():
    import re
    from bioterms.graphql_api.schemas import CONCEPT_SCHEMA

    enum_body = re.search(r'enum ConceptPrefix \{(.*?)\}', CONCEPT_SCHEMA, re.S).group(1)
    assert set(enum_body.split()) == {prefix.value for prefix in ConceptPrefix}


def test_annotated_reactome_resolves_concrete_types_on_demand(graphql):
    run, *_ = graphql

    data = run('''{ go { goConcept(conceptId: "GO:0008150") { data {
        annotatedReactome { __typename conceptId label }
    } } } }''')

    assert data['go']['goConcept']['data']['annotatedReactome'] == [
        {'__typename': 'ReactomePathway', 'conceptId': 'R-HSA-1', 'label': 'Signal pathway'},
        {'__typename': 'ReactomeGene', 'conceptId': 'R-HSA-20', 'label': 'TP53'},
    ]
