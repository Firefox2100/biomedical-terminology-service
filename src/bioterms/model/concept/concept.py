import re
from typing import Optional
from pydantic import Field, ConfigDict

from bioterms.etc.enums import ConceptType, ConceptPrefix, ConceptStatus
from ..base import JsonModel


_UNWANTED_CHARS_PATTERN = re.compile(r'[()"\'\s]')

# The graph database deliberately stores almost nothing beyond node id/prefix/type-labels --
# full concept detail lives in the document database. This is the explicit allowlist of
# Concept-subclass fields (by their JSON alias) that ARE promoted to real, queryable Neo4j/
# PostgreSQL/offline-dump node properties, because some later phase of graph analysis
# genuinely needs to filter/query on them without a document-database lookup (e.g. scoping
# OHDSI's internal hierarchy to its SNOMED-sourced subset, or scoping UniProt's full-release,
# multi-organism content down to human). Adding a field here, plus its column/type below,
# requires no further changes to Neo4jGraphDatabase.save_vocabulary_graph,
# PostgresGraphDatabase's schema/CRUD, vocabulary.utils.write_graph_to_file, or
# scripts/load_offline_vocabulary.py -- all of them read these same structures generically.
GRAPH_NODE_EXTRA_PROPERTIES: list[str] = [
    'sourceVocabularyId',
    'reviewed',
    'organismTaxId',
    'organismName',
]

# PostgreSQL's graph_node_<prefix> tables are plain relational tables, not Neo4j's schemaless
# property graph -- each extra property needs an explicit snake_case column name and SQL type
# to generate DDL/DML from. Every prefix's table gets every column (most stay NULL for most
# vocabularies, e.g. only UniProt populates organism_tax_id) rather than trying to vary the
# schema per vocabulary, matching how any node in Neo4j could carry any of these properties
# regardless of prefix even though only certain vocabularies populate them in practice.
GRAPH_NODE_EXTRA_PROPERTY_COLUMNS: dict[str, str] = {
    'sourceVocabularyId': 'source_vocabulary_id',
    'reviewed': 'reviewed',
    'organismTaxId': 'organism_tax_id',
    'organismName': 'organism_name',
}
GRAPH_NODE_EXTRA_PROPERTY_SQL_TYPES: dict[str, str] = {
    'sourceVocabularyId': 'TEXT',
    'reviewed': 'BOOLEAN',
    'organismTaxId': 'TEXT',
    'organismName': 'TEXT',
}


class Concept(JsonModel):
    """
    A base model for a concept in any vocabulary.
    """

    model_config = ConfigDict(
        serialize_by_alias=True,
    )

    concept_types: list[ConceptType] = Field(
        default_factory=list,
        description='The types of the concept. Marking the node type in the ontology graph or vocabulary.',
        alias='conceptTypes',
    )
    prefix: ConceptPrefix = Field(
        ...,
        description='The prefix of the concept, marking which vocabulary it belongs to.',
    )
    concept_id: str = Field(
        ...,
        description='The unique identifier of the concept within its vocabulary.',
        alias='conceptId',
    )
    label: Optional[str] = Field(
        None,
        description='The human-readable label or name of the concept.',
    )
    synonyms: Optional[list[str]] = Field(
        None,
        description='A list of synonyms or alternative names for the concept.',
    )
    definition: Optional[str] = Field(
        None,
        description='A textual definition or description of the concept.',
    )
    comment: Optional[str] = Field(
        None,
        description='Additional comments or notes about the concept.',
    )
    status: Optional[ConceptStatus] = Field(
        ConceptStatus.ACTIVE,
        description='The status of the concept, indicating whether it is active or deprecated.',
    )
    vector_id: Optional[str] = Field(
        None,
        description='The identifier of the vector representation of the concept in the vector database.',
        alias='vectorId',
    )
    vector: Optional[list[float]] = Field(
        None,
        description='The embedding vector of the concept, stored directly on the concept document. '
                    'Only populated when using a vector database driver (e.g. MongoDB) that embeds '
                    'vectors alongside the rest of the concept data instead of in a separate store.',
        exclude=True,
    )

    def n_grams(self,
                min_length: int = 3,
                max_length: int = 20,
                ) -> list[str]:
        """
        Generate n-grams for auto-complete search from a concept's label and synonyms.
        :param min_length: Minimum length of n-grams
        :param max_length: Maximum length of n-grams
        :return: A list of n-grams
        """
        targets = set()

        def clean_and_split(text: str) -> list[str]:
            """
            Clean a string and split into words, removing unwanted characters and short words.

            :param text: Input string
            :return: List of cleaned, lowercased words.
            """
            # Remove parentheses, double quotes, and other unwanted characters
            cleaned_text = _UNWANTED_CHARS_PATTERN.sub(' ', text)
            # Split into words, filter by length, and convert to lowercase
            return [word.lower() for word in cleaned_text.split() if len(word) > 2]

        targets.add(self.concept_id.lower())

        if self.label:
            targets.update(clean_and_split(self.label))

        if self.synonyms:
            for synonym in self.synonyms:
                targets.update(clean_and_split(synonym))

        n_grams = set()
        for target in targets:
            target_len = len(target)    # Precompute target length
            for n in range(min_length, max_length + 1):     # n ranges from 3 to 20
                if n > target_len:  # Skip if n-gram size exceeds word length
                    break
                for start in range(target_len - n + 1):
                    n_grams.add(target[start:start + n])

        return list(n_grams)

    def search_text(self) -> str:
        """
        Generate a searchable text, including term ID, label, and synonyms
        for a term. This is used for the scoring with regex match in
        auto-completion searches.

        :return: A string of searchable text.
        """
        search_text = self.concept_id

        if self.label:
            label = _UNWANTED_CHARS_PATTERN.sub('', self.label)
            search_text += ' ' + label

        if self.synonyms:
            for synonym in self.synonyms:
                if isinstance(synonym, str):
                    synonym = _UNWANTED_CHARS_PATTERN.sub('', synonym)
                    search_text += ' ' + synonym

        return search_text

    def canonical_text(self) -> str:
        """
        Generate a canonical text representation of the concept, used for text embedding.
        :return: A string of canonical text.
        """
        concept_str = ''

        if self.label:
            concept_str += self.label + ': '

        if self.definition:
            concept_str += self.definition + ' '

        if self.synonyms:
            concept_str += '(' + ' '.join(self.synonyms) + ')'

        return concept_str.strip(' :')
