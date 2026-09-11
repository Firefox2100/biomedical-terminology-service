from typing import Union

from .concept import Concept, EmbeddingItem, GRAPH_NODE_EXTRA_PROPERTIES, GRAPH_NODE_EXTRA_PROPERTY_COLUMNS, \
    GRAPH_NODE_EXTRA_PROPERTY_SQL_TYPES
from .ensembl import EnsemblConcept
from .hgnc import HgncConcept
from .ohdsi import OhdsiDrugStrength, OhdsiConcept
from .reactome import ReactomeConcept
from .snomed import SnomedConcept
from .uniprot import UniProtConcept


ConceptUnion = Union[
    Concept,
    EnsemblConcept,
    HgncConcept,
    OhdsiConcept,
    ReactomeConcept,
    SnomedConcept,
    UniProtConcept,
]
