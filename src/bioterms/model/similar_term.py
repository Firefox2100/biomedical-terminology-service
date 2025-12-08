from typing import List
from pydantic import Field, ConfigDict

from bioterms.etc.enums import ConceptPrefix
from .base import JsonModel


class SimilarTermWithScores(JsonModel):
    """
    Data model for similar terms with similarity scores.
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    concept_id: str = Field(
        ...,
        description='The concept ID of the similar term.',
        alias='conceptId',
    )
    similarity_scores: dict[str, float] = Field(
        ...,
        description='Dictionary of similarity scores, with their calculation methods'
                    'and corpus (if applicable) as keys.',
    )


class SimilarTermByPrefix(JsonModel):
    """
    Data model for similar terms grouped by concept prefix.
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    prefix: ConceptPrefix = Field(
        ...,
        description='The concept prefix of the similar concepts.',
    )
    similar_concepts: List[SimilarTermWithScores] = Field(
        ...,
        description='List of similar concepts under this prefix.',
        alias='similarConcepts',
    )


class SimilarTerm(JsonModel):
    """
    Data model for a similar term.
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    concept_id: str = Field(
        ...,
        description='The concept ID of the expanded term.',
        alias='conceptId',
    )
    similar_groups: List[SimilarTermByPrefix] = Field(
        ...,
        description='List of similar terms grouped by concept prefix.',
        alias='similarGroups',
    )


class SimilarTermAggregate(JsonModel):
    """
    Data model for a similar term, but in aggregated form for GraphQL responses.
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    concept_id: str = Field(
        ...,
        description='The concept ID of the expanded term.',
        alias='conceptId',
    )
    similar_concepts: List[tuple[str, float]] = Field(
        ...,
        description='List of similar terms as tuples of (concept ID, similarity score).',
        alias='similarConcepts',
    )
