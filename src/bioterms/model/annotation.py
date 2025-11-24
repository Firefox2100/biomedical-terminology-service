from typing import Optional
from pydantic import Field, ConfigDict

from bioterms.etc.enums import AnnotationType, ConceptPrefix
from .base import JsonModel


class Annotation(JsonModel):
    """
    An annotation model representing an annotation mapping from one concept to another.
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    prefix_from: ConceptPrefix = Field(
        ...,
        description='The prefix of the source concept in the annotation.',
        alias='prefixFrom',
    )
    concept_id_from: str = Field(
        ...,
        description='The identifier of the source concept in the annotation.',
        alias='conceptIdFrom',
    )
    prefix_to: ConceptPrefix = Field(
        ...,
        description='The prefix of the target concept in the annotation.',
        alias='prefixTo',
    )
    concept_id_to: str = Field(
        ...,
        description='The identifier of the target concept in the annotation.',
        alias='conceptIdTo',
    )
    annotation_type: Optional[AnnotationType] = Field(
        None,
        description='The type of the annotation relationship.',
        alias='annotationType',
    )
    properties: Optional[dict[str, str]] = Field(
        None,
        description='Additional properties associated with the annotation (i.e. frequency, evidence codes).',
    )
