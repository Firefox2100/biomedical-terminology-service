"""NLM-published RxNorm concept mappings to SNOMED CT source codes."""

import httpx
from bioterms.annotation.utils import AnnotationSource, assert_pre_requisite
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.etc.enums import AnnotationType, ConceptPrefix
from bioterms.vocabulary.rxnorm import FILE_PATHS, download_vocabulary, iter_rxnorm_atoms

ANNOTATION_NAME = 'RxNorm Mapping to SNOMED CT'
VOCABULARY_PREFIX_1 = ConceptPrefix.RXNORM
VOCABULARY_PREFIX_2 = ConceptPrefix.SNOMED
_SOURCE = AnnotationSource('NLM RxNorm full release', ConceptPrefix.RXNORM, ConceptPrefix.SNOMED)

async def download_annotation(download_client: httpx.AsyncClient = None):
    await download_vocabulary(download_client)

async def load_annotation_from_file(graph_db: GraphDatabase = None):
    graph_db = graph_db or get_active_graph_db()
    await assert_pre_requisite(ANNOTATION_NAME, VOCABULARY_PREFIX_1, VOCABULARY_PREFIX_2, FILE_PATHS, graph_db)
    current_ids = set()
    mappings = set()
    for atoms in iter_rxnorm_atoms():
        current_ids.update(atoms.loc[
            (atoms['SAB'] == 'RXNORM') & (atoms['LAT'] == 'ENG'), 'RXCUI',
        ])
        rows = atoms[
            atoms['SAB'].str.startswith('SNOMEDCT') & (atoms['SUPPRESS'] == 'N')
        ]
        mappings.update(
            (rxcui, code, sab)
            for rxcui, code, sab in rows[['RXCUI', 'CODE', 'SAB']]
            .itertuples(index=False, name=None)
            if code
        )
    annotations = [
        _SOURCE.create(rxcui, code, AnnotationType.EXACT, {'sourceVocabulary': sab})
        for rxcui, code, sab in mappings
        if rxcui in current_ids
    ]
    if annotations:
        await graph_db.save_annotations(annotations)
