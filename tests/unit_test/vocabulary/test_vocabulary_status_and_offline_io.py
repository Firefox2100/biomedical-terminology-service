"""Tests for vocabulary status computation and the offline graph/annotation dump readers."""
import types

import pytest

import bioterms.vocabulary.utils as vocab_utils
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix, ConceptRelationshipType
from bioterms.model.annotation import Annotation
from bioterms.model.concept import Concept
from bioterms.model.edge_buffer import EdgeBuffer
from bioterms.model.vocabulary_status import VocabularyStatus


class _Counts:
    def __init__(self, **values):
        self.values = values
        self.saved = []

    def __getattr__(self, name):
        async def method(*_args, **_kwargs):
            return self.values.get(name)
        return method

    async def save_vocabulary_status(self, status):
        self.saved.append(status)


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))
    (tmp_path / 'offline').mkdir()
    return tmp_path


def _module(tmp_path):
    (tmp_path / 'v').mkdir(exist_ok=True)
    return types.SimpleNamespace(
        VOCABULARY_NAME='Test', ANNOTATIONS=[ConceptPrefix.MONDO], SIMILARITY_METHODS=[],
        FILE_PATHS=['v/terms.owl'], TIMESTAMP_FILE='v/.timestamp',
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(('timestamp', 'downloaded', 'expected_year'), [
    ('2026-09-01T10:00:00+00:00\n', True, 2026),
    ('not a timestamp', True, None),
    (None, True, None),
    ('2026-09-01T10:00:00+00:00', False, None),  # time is only reported when files exist
])
async def test_status_is_computed_from_every_store_and_cached(monkeypatch, data_dir, timestamp,
                                                             downloaded, expected_year):
    module = _module(data_dir)
    monkeypatch.setattr(vocab_utils, 'get_vocabulary_module', lambda _prefix: module)
    if downloaded:
        (data_dir / 'v' / 'terms.owl').write_text('x')
    if timestamp is not None:
        (data_dir / 'v' / '.timestamp').write_text(timestamp)
    cache = _Counts()
    doc_db, graph_db, vector_db = _Counts(count_terms=3), _Counts(count_internal_relationships=2), \
        _Counts(count_vectors=5)

    status = await vocab_utils.get_vocabulary_status(
        ConceptPrefix.HPO, cache=cache, doc_db=doc_db, graph_db=graph_db, vector_db=vector_db,
    )

    assert (status.loaded, status.concept_count, status.relationship_count, status.vector_count) == (True, 3, 2, 5)
    assert status.file_downloaded is downloaded
    assert (status.file_download_time.year if status.file_download_time else None) == expected_year
    assert status.annotations == [ConceptPrefix.MONDO]
    assert cache.saved == [status]


@pytest.mark.asyncio
async def test_status_returns_cached_value_without_touching_databases(monkeypatch, data_dir):
    monkeypatch.setattr(vocab_utils, 'get_vocabulary_module', lambda _prefix: _module(data_dir))
    cached = VocabularyStatus(prefix=ConceptPrefix.HPO, name='cached', fileDownloaded=False, loaded=False,
                              conceptCount=0, relationshipCount=0, vectorCount=0, annotations=[],
                              similarityMethods=[])

    status = await vocab_utils.get_vocabulary_status(ConceptPrefix.HPO, cache=_Counts(get_vocabulary_status=cached))

    assert status is cached


@pytest.mark.asyncio
async def test_graph_data_round_trips_through_offline_dump(data_dir):
    concepts = [Concept(prefix=ConceptPrefix.HPO, conceptId=cid) for cid in ('HP:1', 'HP:2', 'HP:3')]
    graph = EdgeBuffer()
    graph.add_edge('HP:2', 'HP:1', key='is_a', label=ConceptRelationshipType.IS_A)
    graph.add_edge('HP:3', 'HP:1', key='part_of', label=ConceptRelationshipType.PART_OF)

    await vocab_utils.write_graph_to_file(ConceptPrefix.HPO, concepts, graph)
    nodes, edges = await vocab_utils.load_graph_data_from_file(ConceptPrefix.HPO)

    assert nodes == ['HP:1', 'HP:2', 'HP:3']
    assert sorted(edges) == [('HP:2', 'HP:1', 'is_a', 'is_a'), ('HP:3', 'HP:1', 'part_of', 'part_of')]


@pytest.mark.asyncio
async def test_graph_data_requires_graph_dump_but_not_node_dump(data_dir):
    with pytest.raises(FileNotFoundError, match='Offline graph file for hpo'):
        await vocab_utils.load_graph_data_from_file(ConceptPrefix.HPO)

    (data_dir / 'offline' / 'hpo.graph.dump').write_text('HP:2,HP:1,is_a\nshort,row\n')
    assert await vocab_utils.load_graph_data_from_file(ConceptPrefix.HPO) == ([], [('HP:2', 'HP:1', 'is_a', None)])


@pytest.mark.asyncio
async def test_annotation_pairs_normalise_direction_and_skip_short_rows(data_dir):
    annotations = [
        Annotation(prefixFrom=ConceptPrefix.HPO, conceptIdFrom='HP:1', prefixTo=ConceptPrefix.MONDO,
                   conceptIdTo='MONDO:7', annotationType=AnnotationType.ANNOTATED_WITH),
        Annotation(prefixFrom=ConceptPrefix.HPO, conceptIdFrom='HP:2', prefixTo=ConceptPrefix.MONDO,
                   conceptIdTo='MONDO:8', annotationType=AnnotationType.ANNOTATED_WITH),
    ]
    await vocab_utils.write_annotations_to_file(
        prefix_from=ConceptPrefix.HPO, prefix_to=ConceptPrefix.MONDO, annotations=annotations,
    )
    dump = data_dir / 'offline' / 'hpo-mondo.annotation.dump'
    dump.write_text(dump.read_text() + 'too,short\n')

    forward = await vocab_utils.load_annotation_pairs_from_file(ConceptPrefix.HPO, ConceptPrefix.MONDO)
    backward = await vocab_utils.load_annotation_pairs_from_file(ConceptPrefix.MONDO, ConceptPrefix.HPO)

    assert forward == [('HP:1', '7'), ('HP:2', '8')]
    # Asking for the reverse pair reads the same dump and swaps each tuple.
    assert backward == [('7', 'HP:1'), ('8', 'HP:2')]
