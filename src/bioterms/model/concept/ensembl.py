from typing import Optional
from pydantic import Field, ConfigDict

from .concept import Concept


class EnsemblConcept(Concept):
    """
    A model for an Ensembl gene concept in a vocabulary.
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    bio_type: Optional[str] = Field(
        None,
        description='The biological type of the gene (e.g., protein-coding, lncRNA).',
        alias='bioType',
    )
    start: Optional[int] = Field(
        None,
        description='The start position of the gene on the chromosome.',
    )
    end: Optional[int] = Field(
        None,
        description='The end position of the gene on the chromosome.',
    )
    sequence: Optional[str] = Field(
        None,
        description='The chromosome or sequence name where the gene is located.',
    )
