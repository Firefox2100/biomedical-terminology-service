from typing import Union

from .concept import Concept
from .snomed import SnomedConcept


ConceptUnion = Union[
    Concept,
    SnomedConcept,
]
