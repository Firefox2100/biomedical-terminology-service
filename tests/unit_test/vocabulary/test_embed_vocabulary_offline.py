import json
import pytest

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus, EmbeddingKind
from bioterms.model.concept import Concept, EmbeddingItem
from bioterms.model.vocabulary_status import VocabularyStatus
import bioterms.vocabulary as vocabulary_module


def make_concept(concept_id, label):
    return Concept(
        conceptTypes=[],
        prefix=ConceptPrefix.HPO,
        conceptId=concept_id,
        label=label,
        status=ConceptStatus.ACTIVE,
    )


class FakeDocDb:
    def __init__(self, concepts):
        self._concepts = concepts

    async def get_terms_iter(self, prefix, model_class=Concept):
        for concept in self._concepts:
            yield concept


class FakeConceptTransformer:
    def __init__(self, *args, **kwargs):
        pass

    async def embed_concepts(self, concepts, total_concepts=None):
        async for concept in concepts:
            yield [(
                EmbeddingItem(item_id=f'{concept.concept_id}:alias:0', concept_id=concept.concept_id,
                              kind=EmbeddingKind.ALIAS, text=concept.label),
                [0.1, 0.2, 0.3],
            )]


class FakeTextTransformer:
    def __init__(self, *args, **kwargs):
        pass

    @property
    def dimension(self):
        return 3


@pytest.mark.asyncio
async def test_embed_offline_falls_back_to_database_when_dump_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    concepts = [make_concept('HP:1', 'Foo'), make_concept('HP:2', 'Bar')]
    fake_doc_db = FakeDocDb(concepts)

    async def fake_get_active_doc_db():
        return fake_doc_db

    async def fake_get_vocabulary_status(prefix, doc_db=None, **kwargs):
        return VocabularyStatus(
            prefix=prefix, name='HPO', fileDownloaded=True, loaded=True,
            conceptCount=len(concepts), relationshipCount=0, vectorCount=0,
            annotations=[], similarityMethods=[],
        )

    monkeypatch.setattr(vocabulary_module, 'get_active_doc_db', fake_get_active_doc_db)
    monkeypatch.setattr(vocabulary_module, 'get_vocabulary_status', fake_get_vocabulary_status)
    monkeypatch.setattr('bioterms.embedding.ConceptTransformer', FakeConceptTransformer)
    monkeypatch.setattr('bioterms.embedding.TextTransformer', FakeTextTransformer)

    config = {'conceptClass': Concept}

    doc_dump_path = tmp_path / 'offline' / 'hpo.doc.dump'
    embed_dump_path = tmp_path / 'offline' / 'hpo.embed.dump'
    assert not doc_dump_path.exists()

    await vocabulary_module._embed_vocabulary_offline(ConceptPrefix.HPO, config)

    # The missing .doc.dump was produced from the database, in the same JSON-lines format
    # write_concepts_to_file always uses.
    assert doc_dump_path.exists()
    lines = doc_dump_path.read_text().strip().split('\n')
    assert len(lines) == 2
    written_ids = {json.loads(line)['conceptId'] for line in lines}
    assert written_ids == {'HP:1', 'HP:2'}

    # And embedding proceeded using that freshly-written dump.
    assert embed_dump_path.exists()


@pytest.mark.asyncio
async def test_embed_offline_raises_when_dump_missing_and_vocabulary_not_loaded(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    async def fake_get_active_doc_db():
        return FakeDocDb([])

    async def fake_get_vocabulary_status(prefix, doc_db=None, **kwargs):
        return VocabularyStatus(
            prefix=prefix, name='HPO', fileDownloaded=False, loaded=False,
            conceptCount=0, relationshipCount=0, vectorCount=0,
            annotations=[], similarityMethods=[],
        )

    monkeypatch.setattr(vocabulary_module, 'get_active_doc_db', fake_get_active_doc_db)
    monkeypatch.setattr(vocabulary_module, 'get_vocabulary_status', fake_get_vocabulary_status)

    config = {'conceptClass': Concept}

    with pytest.raises(ValueError, match='not loaded'):
        await vocabulary_module._embed_vocabulary_offline(ConceptPrefix.HPO, config)

    assert not (tmp_path / 'offline' / 'hpo.doc.dump').exists()


@pytest.mark.asyncio
async def test_embed_offline_uses_existing_dump_without_touching_database(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    offline_dir = tmp_path / 'offline'
    offline_dir.mkdir()
    doc_dump_path = offline_dir / 'hpo.doc.dump'
    doc_dump_path.write_text(
        json.dumps({'conceptId': 'HP:1', 'prefix': 'hpo', 'label': 'Foo', 'status': 'active'}) + '\n'
    )

    async def fail_get_active_doc_db():
        raise AssertionError('should not touch the database when a dump already exists')

    monkeypatch.setattr(vocabulary_module, 'get_active_doc_db', fail_get_active_doc_db)
    monkeypatch.setattr('bioterms.embedding.ConceptTransformer', FakeConceptTransformer)
    monkeypatch.setattr('bioterms.embedding.TextTransformer', FakeTextTransformer)

    config = {'conceptClass': Concept}

    await vocabulary_module._embed_vocabulary_offline(ConceptPrefix.HPO, config)

    assert (offline_dir / 'hpo.embed.dump').exists()
