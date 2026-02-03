from typing import List
from pydantic import Field, ConfigDict

from bioterms.etc.enums import ConceptPrefix
from .base import JsonModel


class NodeInPath(JsonModel):
    """
    A node in a concept path
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    concept_id: str = Field(
        ...,
        description='The concept ID of the node in the path.',
        alias='conceptId',
    )
    prefix: ConceptPrefix = Field(
        ...,
        description='The prefix of the concept ID.',
    )


class ConceptPath(JsonModel):
    """
    A path between to concepts in the system.
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    start_concept_id: str = Field(
        ...,
        description='The concept ID of the start of the path.',
        alias='startConceptId',
    )
    end_concept_id: str = Field(
        ...,
        description='The concept ID of the end of the path.',
        alias='endConceptId',
    )
    start_prefix: ConceptPrefix = Field(
        ...,
        description='The prefix of the start concept ID.',
        alias='startPrefix',
    )
    end_prefix: ConceptPrefix = Field(
        ...,
        description='The prefix of the end concept ID.',
        alias='endPrefix',
    )
    length: int = Field(
        ...,
        description='The length of the path (number of nodes).',
    )

    nodes: List[NodeInPath] = Field(
        ...,
        description='The list of nodes in the path, in order from start to end.',
    )
