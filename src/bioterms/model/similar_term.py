from typing import List
from pydantic import Field, ConfigDict

from bioterms.etc.enums import ConceptPrefix
from .base import JsonModel


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
    similar_concepts: List[str] = Field(
        ...,
        description='List of similar concepts under this prefix.',
        alias='similarConcepts',
    )


class SimilarTerm(JsonModel):
    """
    Data model for an expanded term in the expand terms response (v2).
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
