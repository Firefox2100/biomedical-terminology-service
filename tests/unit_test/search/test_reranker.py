import pytest
import sentence_transformers

from bioterms.etc.enums import ConceptPrefix, ConceptStatus
from bioterms.model.concept import Concept
from bioterms.search import reranker


def _concept(**overrides):
    values = {
        'conceptTypes': [], 'prefix': ConceptPrefix.HPO, 'conceptId': 'HP:1',
        'label': 'Primary label', 'synonyms': ['Alias', 'alias', 'Other alias'],
        'definition': 'Definition text.', 'status': ConceptStatus.ACTIVE,
    }
    values.update(overrides)
    return Concept(**values)


def test_candidate_rendering_matches_training_shape(monkeypatch):
    monkeypatch.setattr(reranker.CONFIG, 'reranker_max_aliases', 6)

    rendered = reranker.render_reranker_candidate(_concept())

    assert rendered == 'Primary label (Alias; Other alias) Definition text.'


@pytest.mark.parametrize('source', [
    '/models/sapbert-colbert/final',
    'organisation/biomedical-sapbert-colbert',
])
def test_loader_passes_local_path_or_huggingface_id_unchanged(monkeypatch, source):
    calls = []

    class FakeColBERT:
        def __init__(self, **kwargs):
            calls.append(kwargs)

        def __getitem__(self, _index):
            return type('Transformer', (), {
                'query_length': 32, 'query_expansion': {'length': 32},
            })()

    monkeypatch.setattr(sentence_transformers, 'MultiVectorEncoder', FakeColBERT)
    monkeypatch.setattr(reranker.CONFIG, 'reranker_model', source)
    monkeypatch.setattr(reranker.CONFIG, 'torch_device', 'cpu')
    reranker._load_reranker()

    assert calls == [{
        'model_name_or_path': source, 'device': 'cpu', 'trust_remote_code': True,
    }]
