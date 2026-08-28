from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus
import bioterms.vocabulary.ohdsi as ohdsi


CONCEPT_CSV_HEADER = 'concept_id\tconcept_name\tdomain_id\tvocabulary_id\tconcept_class_id\t' \
                     'standard_concept\tconcept_code\tvalid_start_date\tvalid_end_date\tinvalid_reason'


def _write_concept_csv(data_dir, rows: list[str]):
    ohdsi_dir = data_dir / 'ohdsi'
    ohdsi_dir.mkdir(parents=True, exist_ok=True)
    concept_path = ohdsi_dir / 'CONCEPT.csv'
    concept_path.write_text('\n'.join([CONCEPT_CSV_HEADER, *rows]) + '\n')
    return concept_path


def test_process_concepts_stamps_source_vocabulary_id(monkeypatch, tmp_path):
    data_dir = tmp_path / 'data'
    _write_concept_csv(data_dir, [
        '1\tSNOMED-sourced concept\tCondition\tSNOMED\tClinical Finding\tS\t12345\t20200101\t20991231\t',
        '2\tRead-sourced concept\tCondition\tRead\tRead Code\tS\t67890\t20200101\t20991231\t',
    ])
    monkeypatch.setattr(CONFIG, 'data_dir', str(data_dir))

    concepts = ohdsi._process_concepts()

    assert concepts[1].source_vocabulary_id == 'SNOMED'
    assert concepts[2].source_vocabulary_id == 'Read'
    assert concepts[1].status == ConceptStatus.ACTIVE


def test_process_concepts_handles_missing_vocabulary_id(monkeypatch, tmp_path):
    data_dir = tmp_path / 'data'
    _write_concept_csv(data_dir, [
        '3\tNo vocabulary id\tCondition\t\tClinical Finding\tS\t11111\t20200101\t20991231\t',
    ])
    monkeypatch.setattr(CONFIG, 'data_dir', str(data_dir))

    concepts = ohdsi._process_concepts()

    assert concepts[3].source_vocabulary_id is None
    assert concepts[3].prefix == ConceptPrefix.OHDSI
