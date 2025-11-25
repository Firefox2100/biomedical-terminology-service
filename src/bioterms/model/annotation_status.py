from pydantic import Field, ConfigDict

from bioterms.etc.enums import ConceptPrefix
from .base import JsonModel


class AnnotationStatus(JsonModel):
    """
    An annotation status model indicating the current state of an annotation
    loaded in between vocabularies.
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    prefix_source: ConceptPrefix = Field(
        ...,
        description='The prefix of the source vocabulary.',
        alias='prefixSource',
    )
    prefix_target: ConceptPrefix = Field(
        ...,
        description='The prefix of the target vocabulary.',
        alias='prefixTarget',
    )
    name: str = Field(
        ...,
        description='The name of the vocabulary.',
    )
    loaded: bool = Field(
        ...,
        description='Indicates whether the vocabulary is loaded in the system.',
    )
    relationship_count: int = Field(
        ...,
        description='The number of relationships in the annotation.',
        alias='relationshipCount',
    )
