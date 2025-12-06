from typing import List
from pydantic import Field, ConfigDict

from .base import JsonModel


class RelatedTerms(JsonModel):
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
    related_concepts: List[str] = Field(
        ...,
        description='List of related concept IDs.',
        alias='relatedConcepts',
    )
