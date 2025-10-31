from bioterms.etc.enums import ConceptPrefix, ConceptStatus
from bioterms.model.concept import Concept


class TestConcept:
    def test_initialisation(self):
        concept = Concept(
            conceptTypes=[],
            prefix=ConceptPrefix.HPO,
            conceptId='0000001',
            label='Test Concept',
            synonyms=['Synonym One', 'Synonym Two'],
            definition='Testing concept definition.',
            comment='This is a test concept.',
            status=ConceptStatus.ACTIVE
        )

        assert concept.concept_types == []
        assert concept.prefix == ConceptPrefix.HPO
        assert concept.concept_id == '0000001'
        assert concept.label == 'Test Concept'
        assert concept.synonyms == ['Synonym One', 'Synonym Two']
        assert concept.definition == 'Testing concept definition.'
        assert concept.comment == 'This is a test concept.'
        assert concept.status == ConceptStatus.ACTIVE

    def test_generates_ngrams_from_label_and_synonyms(self):
        concept = Concept(
            conceptTypes=[],
            prefix=ConceptPrefix.HPO,
            conceptId='0000001',
            label='Test Concept',
            synonyms=['Synonym One', 'Synonym Two'],
            status=ConceptStatus.ACTIVE,
        )

        ngrams = concept.n_grams(min_length=3, max_length=5)

        assert 'tes' in ngrams
        assert 'test' in ngrams
        assert 'oncep' in ngrams
        assert 'syn' in ngrams
        assert 'onym' in ngrams
        assert 'one' in ngrams
        assert 'two' in ngrams

    def test_handles_empty_label_and_synonyms(self):
        concept = Concept(
            conceptTypes=[],
            prefix=ConceptPrefix.HPO,
            conceptId='0000001',
            label=None,
            synonyms=None,
            status=ConceptStatus.ACTIVE,
        )

        ngrams = concept.n_grams(min_length=3, max_length=5)

        assert '000' in ngrams
        assert '00001' in ngrams
        assert len(ngrams) > 0

    def test_excludes_ngrams_shorter_than_min_length(self):
        concept = Concept(
            conceptTypes=[],
            prefix=ConceptPrefix.HPO,
            conceptId='0000001',
            label='Short',
            synonyms=None,
            status=ConceptStatus.ACTIVE,
        )

        ngrams = concept.n_grams(min_length=9, max_length=15)

        assert len(ngrams) == 0

    def test_generates_searchable_text_with_label_and_synonyms(self):
        concept = Concept(
            conceptTypes=[],
            prefix=ConceptPrefix.HPO,
            conceptId='0000001',
            label='Test Concept',
            synonyms=['Synonym One', 'Synonym Two'],
            status=ConceptStatus.ACTIVE,
        )

        search_text = concept.search_text()

        assert '0000001' in search_text
        assert 'TestConcept' in search_text
        assert "SynonymOne" in search_text
        assert "SynonymTwo" in search_text

    def test_handles_searchable_text_with_empty_label_and_synonyms(self):
        concept = Concept(
            conceptTypes=[],
            prefix=ConceptPrefix.HPO,
            conceptId='0000001',
            label=None,
            synonyms=None,
            status=ConceptStatus.ACTIVE,
        )

        search_text = concept.search_text()

        assert search_text == '0000001'
