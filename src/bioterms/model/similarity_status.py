from typing import List, Optional
from pydantic import Field, ConfigDict

from bioterms.etc.enums import ConceptPrefix, SimilarityMethod
from .base import JsonModel


class SimilarityCount(JsonModel):
    """
    A similarity count model indicating the number of similarity relationships.
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    method: SimilarityMethod = Field(
        ...,
        description='The similarity method used.',
    )
    corpus: Optional[ConceptPrefix] = Field(
        None,
        description='The prefix of the corpus vocabulary used for similarity, if applicable.',
    )
    count: int = Field(
        0,
        description='The number of similarity relationships.',
    )


class SimilarityStatus(JsonModel):
    """
    A similarity status model indicating the current state of a vocabulary.
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    prefix: ConceptPrefix = Field(
        ...,
        description='The prefix of the vocabulary.',
    )
    similarity_counts: List[SimilarityCount] = Field(
        default_factory=list,
        description='A list of similarity counts for different methods and corpora.',
        alias='similarityCounts',
    )
