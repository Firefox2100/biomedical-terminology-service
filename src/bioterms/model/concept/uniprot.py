from typing import Optional
from pydantic import Field, ConfigDict

from .concept import Concept


class UniProtConcept(Concept):
    """
    A model representing a UniProtKB protein concept.
    """

    model_config = ConfigDict(
        serialize_by_alias=True,
    )

    reviewed: Optional[bool] = Field(
        None,
        description='Whether this entry is UniProtKB/Swiss-Prot (manually reviewed, True) '
                    'or UniProtKB/TrEMBL (automated, unreviewed, False).',
    )
    organism_tax_id: Optional[str] = Field(
        None,
        description='The NCBI taxonomy ID of the source organism. UniProt is loaded as a '
                    'complete, unscoped vocabulary (all organisms) -- this is the field '
                    'downstream consumers filter on (e.g. to a human-only subset) rather '
                    'than the vocabulary being pre-restricted at load time.',
        alias='organismTaxId',
    )
    organism_name: Optional[str] = Field(
        None,
        description="The source organism's scientific name.",
        alias='organismName',
    )
