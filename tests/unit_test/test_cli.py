"""
CLI tests: each command is invoked through the real Typer app from `create_cli()` with the
backend operations replaced by recorders, checking argument parsing, target selection,
dispatch and the reported outcome. Commands report per-target failures and carry on, but still
exit with status 1 so scripts and CI see the failure.
"""
import re
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

import bioterms.cli.annotation as cli_annotation
import bioterms.cli.similarity as cli_similarity
import bioterms.cli.user as cli_user
import bioterms.cli.vocabulary as cli_vocabulary
from bioterms.cli.cli import create_cli
from bioterms.etc.consts import CONFIG, PH
from bioterms.etc.enums import ConceptPrefix, SimilarityMethod
from bioterms.model.annotation_status import AnnotationStatus
from bioterms.model.user import User
from bioterms.model.vocabulary_status import VocabularyStatus

RUNNER = CliRunner()
# CI runners (GITHUB_ACTIONS / FORCE_COLOR) make Rich emit colour codes and 80-column boxes,
# which split the messages asserted on below; render plain, wide output instead.
PLAIN_TERMINAL = {'NO_COLOR': '1', 'FORCE_COLOR': None, 'GITHUB_ACTIONS': None, 'TERM': 'dumb', 'COLUMNS': '250'}
ANSI_ESCAPE = re.compile(r'\x1b\[[0-9;]*m')


def invoke(*args):
    result = RUNNER.invoke(create_cli(), list(args), env=PLAIN_TERMINAL)
    return SimpleNamespace(exit_code=result.exit_code, output=ANSI_ESCAPE.sub('', result.output))


@pytest.fixture(autouse=True)
def quiet(monkeypatch):
    monkeypatch.setattr(CONFIG, 'disable_progress_bar', True)
    monkeypatch.setattr('bioterms.cli.utils.report_exception', lambda exc: None)
    monkeypatch.setattr('bioterms.cli.utils.CONSOLE.width', 250)  # keep Rich tables unwrapped
    monkeypatch.setattr('bioterms.cli.utils.CONSOLE.no_color', True)


def record(monkeypatch, module, name, result=None, error=None):
    calls = []

    async def fake(*args, **kwargs):
        calls.append((args, kwargs))
        if error:
            raise error
        return result

    monkeypatch.setattr(module, name, fake)
    return calls


# --- top level -------------------------------------------------------------------------------

def test_bare_invocations_show_help_with_usage_error_status():
    assert invoke().exit_code == 2
    assert invoke('vocabulary').exit_code == 2


def test_version_and_hmac_key():
    assert 'CLI version' in invoke('--version').output
    key_output = invoke('generate-hmac-key')
    assert key_output.exit_code == 0
    assert 'Generated HMAC key' in key_output.output


def test_verbose_flag_enables_verbose_output(monkeypatch):
    monkeypatch.setattr(CONFIG, 'verbose_print', False)
    record(monkeypatch, cli_vocabulary, 'download_vocabulary')

    invoke('--verbose', 'vocabulary', 'download', 'hpo')

    assert CONFIG.verbose_print is True


# --- vocabulary ------------------------------------------------------------------------------

def test_vocabulary_download_one_and_all(monkeypatch):
    calls = record(monkeypatch, cli_vocabulary, 'download_vocabulary')

    one = invoke('vocabulary', 'download', 'hpo', '--redownload')
    invoke('vocabulary', 'download', '--all')

    assert 'Successfully downloaded vocabulary hpo' in one.output
    assert calls[0] == ((ConceptPrefix.HPO, True), {})
    assert [args[0] for args, _ in calls[1:]] == list(ConceptPrefix)


def test_vocabulary_commands_require_a_target():
    for command in ('download', 'load', 'restore', 'embed', 'delete'):
        result = invoke('vocabulary', command)
        assert 'Either specify a vocabulary' in result.output, command
        assert result.exit_code == 1, command


def test_vocabulary_load_passes_flags(monkeypatch):
    calls = record(monkeypatch, cli_vocabulary, 'load_vocabulary')

    invoke('vocabulary', 'load', 'mondo', '--overwrite', '--offline', '--no-index', '--no-annotation')

    assert calls == [((ConceptPrefix.MONDO,), {
        'drop_existing': True, 'offline': True, 'build_search_index': False, 'load_annotations': False,
    })]


def test_vocabulary_failures_are_reported_and_exit_non_zero(monkeypatch):
    record(monkeypatch, cli_vocabulary, 'load_vocabulary', error=RuntimeError('files missing'))

    result = invoke('vocabulary', 'load', 'hpo')

    assert result.exit_code == 1
    assert 'Failed to load vocabulary hpo: files missing' in result.output


def test_partial_failure_finishes_remaining_targets_then_exits_non_zero(monkeypatch, tmp_path):
    attempted = []

    async def restore(target_prefix, **_kwargs):
        attempted.append(target_prefix)
        if target_prefix == ConceptPrefix.HPO:
            raise RuntimeError('corrupt dump')
        return 3

    monkeypatch.setattr(cli_similarity, 'restore_similarity', restore)
    for name in ('hpo-relevance.similarity.dump', 'mondo-relevance.similarity.dump'):
        (tmp_path / name).write_text('')

    result = invoke('similarity', 'restore', '--all', '--offline-dir', str(tmp_path))

    assert attempted == [ConceptPrefix.HPO, ConceptPrefix.MONDO]
    assert 'Failed to restore similarity scores for hpo' in result.output
    assert 'Successfully restored 3 similarity scores for mondo' in result.output
    assert result.exit_code == 1


def test_successful_commands_exit_zero_after_an_earlier_failure(monkeypatch):
    # The failure record is per invocation, so one failed command must not taint the next.
    record(monkeypatch, cli_vocabulary, 'load_vocabulary', error=RuntimeError('boom'))
    assert invoke('vocabulary', 'load', 'hpo').exit_code == 1

    record(monkeypatch, cli_vocabulary, 'load_vocabulary')
    assert invoke('vocabulary', 'load', 'hpo').exit_code == 0


def test_vocabulary_restore_reports_summary(monkeypatch):
    calls = record(monkeypatch, cli_vocabulary, 'restore_vocabulary',
                   result={'conceptCount': 12, 'edgeCount': 3, 'embeddingsRestored': False})

    result = invoke('vocabulary', 'restore', 'hpo', '-o', '-b', '50', '--offline-dir', '/dumps', '--skip-embeddings')

    assert calls == [((ConceptPrefix.HPO,), {'overwrite': True, 'batch_size': 50,
                                            'offline_dir': '/dumps', 'restore_embeddings': False})]
    assert '12' in result.output
    assert 'skipped' in result.output


def test_vocabulary_embed_modes(monkeypatch):
    embed = record(monkeypatch, cli_vocabulary, 'embed_vocabulary')
    restore = record(monkeypatch, cli_vocabulary, 'restore_vocabulary_embeddings')

    invoke('vocabulary', 'embed', 'hpo', '--offline')
    invoke('vocabulary', 'embed', 'hpo', '--restore', '--overwrite')
    conflict = invoke('vocabulary', 'embed', 'hpo', '--offline', '--restore')

    assert embed == [((ConceptPrefix.HPO,), {'drop_existing': False, 'offline': True})]
    assert restore == [((ConceptPrefix.HPO,), {'drop_existing': True})]
    assert 'Cannot use --offline and --restore' in conflict.output
    assert conflict.exit_code == 1


def test_vocabulary_delete_and_status(monkeypatch):
    deleted = record(monkeypatch, cli_vocabulary, 'delete_vocabulary')
    status = VocabularyStatus(prefix=ConceptPrefix.HPO, name='Human Phenotype Ontology', fileDownloaded=True,
                              loaded=True, conceptCount=18000, relationshipCount=22000, vectorCount=0,
                              annotations=[], similarityMethods=[])
    record(monkeypatch, cli_vocabulary, 'get_vocabulary_status', result=status)

    invoke('vocabulary', 'delete', 'hpo')
    shown = invoke('vocabulary', 'status', 'hpo')

    assert deleted == [((ConceptPrefix.HPO,), {})]
    assert 'Human Phenotype Ontology' in shown.output
    assert '18000' in shown.output.replace(',', '')


# --- annotation ------------------------------------------------------------------------------

def test_annotation_target_resolution():
    assert cli_annotation._resolve_annotation_targets(ConceptPrefix.HPO, ConceptPrefix.MONDO, False) == \
        [(ConceptPrefix.HPO, ConceptPrefix.MONDO)]
    assert (ConceptPrefix.HPO, ConceptPrefix.MONDO) in cli_annotation._resolve_annotation_targets(None, None, True) \
        or (ConceptPrefix.MONDO, ConceptPrefix.HPO) in cli_annotation._resolve_annotation_targets(None, None, True)

    assert invoke('annotation', 'download', 'hpo').exit_code != 0            # second prefix missing
    assert invoke('annotation', 'download', 'hpo', 'mondo', '--all').exit_code != 0


def test_annotation_download_load_delete(monkeypatch):
    downloads = record(monkeypatch, cli_annotation, 'download_annotation')
    loads = record(monkeypatch, cli_annotation, 'load_annotation')
    deletes = record(monkeypatch, cli_annotation, 'delete_annotation')

    assert 'Success' in invoke('annotation', 'download', 'hpo', 'mondo', '--redownload').output
    invoke('annotation', 'load', 'hpo', 'mondo', '--offline')
    invoke('annotation', 'delete', 'hpo', 'mondo')

    assert downloads[0][1]['redownload'] is True
    assert loads[-1][1]['offline'] is True
    assert (deletes[0][1].get('prefix_1'), deletes[0][1].get('prefix_2')) == (ConceptPrefix.HPO, ConceptPrefix.MONDO)


def test_annotation_restore_and_status(monkeypatch, tmp_path):
    restores = record(monkeypatch, cli_annotation, 'restore_annotation', result=42)
    status = AnnotationStatus(prefixSource=ConceptPrefix.HPO, prefixTarget=ConceptPrefix.MONDO,
                              name='HPO to MONDO', loaded=True, relationshipCount=7)
    record(monkeypatch, cli_annotation, 'get_annotation_status', result=status)
    dump = tmp_path / 'hpo-mondo.annotation.dump'
    dump.write_text('')

    restored = invoke('annotation', 'restore', str(dump), '--overwrite', '--batch-size', '10')
    shown = invoke('annotation', 'status', 'hpo', 'mondo')

    assert '42' in restored.output
    assert restores[0][1]['overwrite'] is True
    assert restores[0][1]['batch_size'] == 10
    assert 'HPO to MONDO' in shown.output


# --- similarity ------------------------------------------------------------------------------

def test_similarity_calculate_expands_combinations(monkeypatch):
    calls = record(monkeypatch, cli_similarity, 'calculate_similarity')

    invoke('similarity', 'calculate', '--target', 'hpo', '--corpus', 'omim', '--method', 'relevance', '-t', '0.4')

    assert calls == [((), {
        'method': SimilarityMethod.RELEVANCE, 'target_prefix': ConceptPrefix.HPO,
        'corpus_prefix': ConceptPrefix.OMIM, 'similarity_threshold': 0.4, 'offline': False,
        'annotation_file_path': None,
    })]


def test_similarity_calculate_validates_annotation_file_and_reports_failures(monkeypatch, tmp_path):
    record(monkeypatch, cli_similarity, 'calculate_similarity', error=RuntimeError('no corpus'))

    bad = invoke('similarity', 'calculate', '--target', 'hpo', '--annotation-file', str(tmp_path / 'x'))
    failed = invoke('similarity', 'calculate', '--target', 'hpo', '--corpus', 'omim', '--method', 'relevance')

    assert bad.exit_code != 0
    assert 'requires --offline' in bad.output
    assert 'Failed to calculate similarity' in failed.output
    assert failed.exit_code == 1


def test_similarity_restore_all_skips_prefixes_without_dumps(monkeypatch, tmp_path):
    calls = record(monkeypatch, cli_similarity, 'restore_similarity', result=5)
    (tmp_path / 'hpo-relevance-omim.similarity.dump').write_text('')

    result = invoke('similarity', 'restore', '--all', '--offline-dir', str(tmp_path))

    assert [kwargs['target_prefix'] for _, kwargs in calls] == [ConceptPrefix.HPO]
    assert 'Successfully restored 5 similarity scores for hpo' in result.output
    assert invoke('similarity', 'restore').exit_code != 0
    assert invoke('similarity', 'restore', 'hpo', '--all').exit_code != 0


# --- cache and users -------------------------------------------------------------------------

def test_cache_commands(monkeypatch):
    purged = []

    class Cache:
        async def purge(self):
            purged.append(True)

    monkeypatch.setattr('bioterms.database.get_active_cache', lambda: Cache())
    rebuilt = []

    async def rebuild_cache():
        rebuilt.append(True)

    monkeypatch.setattr('bioterms.app.rebuild_cache', rebuild_cache)

    assert 'Successfully purged' in invoke('cache', 'purge').output
    invoke('cache', 'rebuild')
    assert purged == [True]
    assert rebuilt == [True]


def test_user_create_list_delete(monkeypatch):
    class Users:
        def __init__(self):
            self.users = {}

        async def save(self, user):
            self.users[user.username] = user

        async def filter(self):
            return list(self.users.values())

        async def get(self, username):
            return self.users.get(username)

        async def delete(self, username):
            del self.users[username]

    db = type('Db', (), {'users': Users()})()

    async def active_doc_db():
        return db

    monkeypatch.setattr(cli_user, 'get_active_doc_db', active_doc_db)

    assert 'No users found' in invoke('user', 'list').output
    assert 'Successfully created new user alice' in invoke('user', 'create', 'alice', '--password', 'pw').output
    assert db.users.users['alice'].validate_password('pw')
    assert 'alice' in invoke('user', 'list').output
    missing = invoke('user', 'delete', 'bob')
    assert 'No user found with username bob' in missing.output
    assert missing.exit_code == 1
    assert 'Successfully deleted user alice' in invoke('user', 'delete', 'alice').output
