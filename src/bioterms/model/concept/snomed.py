from pydantic import Field, ConfigDict

from .concept import Concept


class SnomedConcept(Concept):
    """
    A model representing a SNOMED CT concept.
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    fully_defined: bool = Field(
        ...,
        description='Indicates if the concept is fully defined.',
        alias='fullyDefined',
    )
