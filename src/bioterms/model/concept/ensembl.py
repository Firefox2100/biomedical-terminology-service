from typing import Optional
from pydantic import Field, ConfigDict

from .concept import Concept


class EnsemblConcept(Concept):
    """
    A model for an Ensembl genomic feature concept.
    """

    model_config = ConfigDict(
        serialize_by_alias=True,
    )

    bio_type: Optional[str] = Field(
        None,
        description='The biological type of the gene (e.g., protein-coding, lncRNA).',
        alias='bioType',
    )
    start: Optional[int] = Field(
        None,
        description='The genomic start position of the feature.',
    )
    end: Optional[int] = Field(
        None,
        description='The genomic end position of the feature.',
    )
    sequence: Optional[str] = Field(
        None,
        description='The chromosome or sequence name where the feature is located.',
    )
    version: Optional[str] = Field(
        None,
        description='The release-specific version of the stable Ensembl identifier.',
    )
    strand: Optional[str] = Field(
        None,
        description='The genomic strand on which the feature is located.',
    )
    source: Optional[str] = Field(
        None,
        description='The annotation source that produced the feature.',
    )
    transcript_support_level: Optional[str] = Field(
        None,
        description='Ensembl transcript support level, when supplied by the release.',
        alias='transcriptSupportLevel',
    )
