from typing import Union

from .concept import Concept
from .ensembl import EnsemblConcept
from .hgnc import HgncConcept
from .reactome import ReactomeConcept
from .snomed import SnomedConcept


ConceptUnion = Union[
    Concept,
    EnsemblConcept,
    HgncConcept,
    ReactomeConcept,
    SnomedConcept,
]
