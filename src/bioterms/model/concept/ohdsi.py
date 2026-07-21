from typing import Optional
from pydantic import Field, ConfigDict

from ..base import JsonModel
from .concept import Concept


class OhdsiDrugStrength(JsonModel):
    """
    A model representing the strength of a drug in OHDSI vocabulary.
    """

    model_config = ConfigDict(
        serialize_by_alias=True,
    )

    ingredient_id: str = Field(
        ...,
        description='The unique identifier of the ingredient concept.',
        alias='ingredientId',
    )
    amount_value: Optional[float] = Field(
        None,
        description='The numeric value representing the amount of the drug.',
        alias='amountValue',
    )
    amount_unit: Optional[str] = Field(
        None,
        description='The unit of measurement for the amount of the drug.',
        alias='amountUnit',
    )
    numerator_value: Optional[float] = Field(
        None,
        description='The numeric value representing the numerator of the drug strength.',
        alias='numeratorValue',
    )
    numerator_unit: Optional[str] = Field(
        None,
        description='The unit of measurement for the numerator of the drug strength.',
        alias='numeratorUnit',
    )
    denominator_value: Optional[float] = Field(
        None,
        description='The numeric value representing the denominator of the drug strength.',
        alias='denominatorValue',
    )
    denominator_unit: Optional[str] = Field(
        None,
        description='The unit of measurement for the denominator of the drug strength.',
        alias='denominatorUnit',
    )


class OhdsiConcept(Concept):
    """
    A model representing an OHDSI concept.
    """

    model_config = ConfigDict(
        extra='forbid',
        serialize_by_alias=True,
    )

    drug_strengths: Optional[list[OhdsiDrugStrength]] = Field(
        None,
        description='List of drug strengths associated with the concept.',
        alias='drugStrengths',
    )
