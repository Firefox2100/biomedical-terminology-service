from typing import List
from pydantic import Field, ConfigDict

from .base import JsonModel


class RelatedTerm(JsonModel):
    """
    Data model for a concept with its related concepts. This may be used in various contexts for different
    types of relationships between concepts.
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
