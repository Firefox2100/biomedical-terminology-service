from typing import Optional
from pydantic import Field, ConfigDict

from .concept import Concept


class HgncConcept(Concept):
    """
    A model for a HGNC gene concept in a vocabulary.
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    location: Optional[str] = Field(
        None,
        description='The chromosomal location of the gene.',
    )
