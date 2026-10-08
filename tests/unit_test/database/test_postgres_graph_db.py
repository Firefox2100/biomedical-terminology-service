from bioterms.database.graph_db.postgres_graph_db import _group_similar_terms
from bioterms.etc.enums import ConceptPrefix


def test_group_similar_terms_orders_prefixes_and_ranks_by_best_score():
    scores = {
        'mondo': {'MONDO:1': {'co_annotation': 0.2}, 'MONDO:2': {'co_annotation': 0.4, 'relevance:hpo': 0.9}},
        'hpo': {'HP:2': {'relevance': 0.5}, 'HP:3': {'relevance': 0.7}, 'HP:4': {'relevance': 0.1}},
    }

    groups = _group_similar_terms(scores, limit=2)

    assert [group.prefix for group in groups] == [ConceptPrefix.HPO, ConceptPrefix.MONDO]
    assert [c.concept_id for c in groups[0].similar_concepts] == ['HP:3', 'HP:2']
    # MONDO:2 ranks first on its best score across methods, keeping every method's score.
    assert [c.concept_id for c in groups[1].similar_concepts] == ['MONDO:2', 'MONDO:1']
    assert groups[1].similar_concepts[0].similarity_scores == {'co_annotation': 0.4, 'relevance:hpo': 0.9}


def test_group_similar_terms_without_limit_or_matches():
    scores = {'hpo': {f'HP:{i}': {'relevance': i / 10} for i in range(5)}}

    assert len(_group_similar_terms(scores, limit=None)[0].similar_concepts) == 5
    assert _group_similar_terms({}, limit=3) == []
