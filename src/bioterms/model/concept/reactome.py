from typing import Optional
from pydantic import Field, ConfigDict

from .concept import Concept


class ReactomeConcept(Concept):
    """
    A model representing a Reactome concept.
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    inferred: Optional[bool] = Field(
        None,
        description='Indicates if the concept is inferred.',
    )
