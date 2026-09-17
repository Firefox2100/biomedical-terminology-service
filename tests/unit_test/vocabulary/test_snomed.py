import inspect

import networkx as nx
import pytest

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptRelationshipType
import bioterms.vocabulary.snomed as snomed


ASSOCIATION_HEADER = 'id\teffectiveTime\tactive\tmoduleId\trefsetId\treferencedComponentId\ttargetComponentId'


def _write_association_file(path, rows: list[str]):
    path.write_text('\n'.join([ASSOCIATION_HEADER, *rows]) + '\n')


def test_process_associations_adds_edge_keyed_by_refset_id(tmp_path):
    association_path = tmp_path / 'association.txt'
    _write_association_file(association_path, [
        'a1\t20200101\t1\tmod1\t900000000000527005\t100\t200',
    ])
    graph = nx.MultiDiGraph()

    snomed._process_associations(str(association_path), graph)

    assert graph.has_edge('100', '200', key='snomed_association_900000000000527005')
    edge_data = graph.get_edge_data('100', '200', key='snomed_association_900000000000527005')
    assert edge_data['label'] == ConceptRelationshipType.SNOMED_ASSOCIATION


def test_process_associations_skips_inactive_rows(tmp_path):
    association_path = tmp_path / 'association.txt'
    _write_association_file(association_path, [
        'a1\t20200101\t0\tmod1\t900000000000527005\t100\t200',
    ])
    graph = nx.MultiDiGraph()

    snomed._process_associations(str(association_path), graph)

    assert graph.number_of_edges() == 0


def test_process_associations_distinguishes_refset_ids_between_same_pair(tmp_path):
    # Two different association types between the SAME pair must both survive as distinct
    # edges -- this is exactly the collapsing bug a plain DiGraph would reintroduce.
    association_path = tmp_path / 'association.txt'
    _write_association_file(association_path, [
        'a1\t20200101\t1\tmod1\t900000000000527005\t100\t200',
        'a2\t20200101\t1\tmod1\t900000000000526001\t100\t200',
    ])
    graph = nx.MultiDiGraph()

    snomed._process_associations(str(association_path), graph)

    assert graph.number_of_edges('100', '200') == 2
    assert graph.has_edge('100', '200', key='snomed_association_900000000000527005')
    assert graph.has_edge('100', '200', key='snomed_association_900000000000526001')


def test_process_relationships_and_associations_coexist_on_same_pair(tmp_path):
    # An is_a edge and an association edge between the same pair must not overwrite each
    # other now that snomed_graph is a MultiDiGraph.
    relationship_path = tmp_path / 'relationship.txt'
    relationship_path.write_text(
        'id\teffectiveTime\tactive\tmoduleId\tsourceId\tdestinationId\trelationshipGroup\ttypeId\tcharacteristicTypeId\tmodifierId\n'
        'r1\t20200101\t1\tmod1\t100\t200\t0\t116680003\t0\t0\n'
    )
    association_path = tmp_path / 'association.txt'
    _write_association_file(association_path, [
        'a1\t20200101\t1\tmod1\t900000000000527005\t100\t200',
    ])
    graph = nx.MultiDiGraph()

    snomed._process_relationships(str(relationship_path), graph)
    snomed._process_associations(str(association_path), graph)

    assert graph.number_of_edges('100', '200') == 2
    assert graph.has_edge('100', '200', key='is_a')
    assert graph.has_edge('100', '200', key='snomed_association_900000000000527005')


def test_association_file_paths_are_registered_for_all_three_releases():
    assert len(snomed._ASSOCIATION_FILE_PATHS) == 3
    assert all('association.txt' in path for path in snomed._ASSOCIATION_FILE_PATHS)
    # Deliberately excluded from FILE_PATHS/check_files_exist -- see module comment.
    assert not any(path in snomed.FILE_PATHS for path in snomed._ASSOCIATION_FILE_PATHS)


@pytest.mark.asyncio
async def test_delete_vocabulary_files_removes_association_files_too(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    all_paths = snomed.FILE_PATHS + snomed._ASSOCIATION_FILE_PATHS + [snomed.TIMESTAMP_FILE]
    for path in all_paths:
        full_path = tmp_path / path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_text('x')

    await snomed.delete_vocabulary_files()

    for path in all_paths:
        assert not (tmp_path / path).exists()


@pytest.mark.asyncio
async def test_delete_vocabulary_files_tolerates_missing_files(monkeypatch, tmp_path):
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    await snomed.delete_vocabulary_files()  # must not raise even though nothing exists


def test_download_vocabulary_marks_association_pattern_optional():
    # A release whose exact filename this doesn't match must not abort the whole SNOMED
    # download -- only the association refset extraction is best-effort; the core
    # concept/description/definition/relationship files are required and must still succeed.
    source = inspect.getsource(snomed.download_vocabulary)
    pattern = "der2_*Refset_Association*Full*.txt"

    # Widened wildcard, not a single hardcoded guess at the exact refset filename spelling
    # (real RF2 releases use "AssociationReferenceFull", not "AssociationReferenceSetFull").
    occurrences = source.count(pattern)
    assert occurrences == 3, f'expected one association file_mapping entry per release, found {occurrences}'

    # Each occurrence's tuple must end in `, False)` (required=False), not silently fall
    # back to the 2-tuple (required-by-default) form. Checked as a nearby-text window rather
    # than proper paren-matching, since the dest_path argument (os.path.join(...)) has its
    # own closing paren before the tuple's real one.
    start = 0
    for _ in range(occurrences):
        start = source.index(pattern, start)
        window = source[start:start + 200]
        assert ', False)' in window, (
            f'association file_mapping entry must be marked required=False, near: {window!r}'
        )
        start += len(pattern)
