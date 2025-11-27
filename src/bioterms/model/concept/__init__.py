from typing import Union

from .concept import Concept
from .gene import GeneConcept
from .snomed import SnomedConcept


ConceptUnion = Union[
    Concept,
    GeneConcept,
    SnomedConcept,
]
