from pydantic import Field, ConfigDict

from bioterms.etc.enums import ConceptPrefix
from .base import JsonModel


class TranslatedTerm(JsonModel):
    """
    Data model for a translated term in the translate terms response.
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    concept_id: str = Field(
        ...,
        description='The concept ID of the translated term.',
        alias='conceptId',
    )
    prefix: ConceptPrefix = Field(
        ...,
        description='The concept prefix of the translated term.',
    )
    score: float = Field(
        ...,
        description='The similarity score of the translated term.',
    )
