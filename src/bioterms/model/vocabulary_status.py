from typing import List
from pydantic import Field, ConfigDict

from bioterms.etc.enums import ConceptPrefix
from .base import JsonModel


class VocabularyStatus(JsonModel):
    """
    A vocabulary status model indicating the current state of a vocabulary.
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    prefix: ConceptPrefix = Field(
        ...,
        description='The prefix of the vocabulary.',
    )
    name: str = Field(
        ...,
        description='The name of the vocabulary.',
    )
    loaded: bool = Field(
        ...,
        description='Indicates whether the vocabulary is loaded in the system.',
    )
    concept_count: int = Field(
        ...,
        description='The number of concepts in the vocabulary.',
        alias='conceptCount',
    )
    annotations: List[ConceptPrefix] = Field(
        default_factory=list,
        description='A list of annotation vocabularies associated with this vocabulary.',
    )
