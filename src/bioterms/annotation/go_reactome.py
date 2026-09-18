import os

import httpx
import pandas as pd

from bioterms.annotation.utils import AnnotationSource, assert_pre_requisite, \
    load_obo_xref_annotations
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix
from bioterms.etc.utils import verbose_print
from bioterms.vocabulary.go import download_vocabulary as download_go
from bioterms.vocabulary.reactome import download_vocabulary as download_reactome


ANNOTATION_NAME = 'Gene Ontology and Reactome Mappings'
VOCABULARY_PREFIX_1 = ConceptPrefix.GO
VOCABULARY_PREFIX_2 = ConceptPrefix.REACTOME
FILE_PATHS = ['go/go-basic.owl', 'reactome/go_mapping.csv']
_REACTOME = AnnotationSource('Reactome GO assignment', ConceptPrefix.REACTOME, ConceptPrefix.GO)


async def download_annotation(download_client: httpx.AsyncClient = None):
    """Download both publisher products used by this bidirectional annotation."""
    await download_go(download_client=download_client)
    await download_reactome(download_client=download_client)


async def load_annotation_from_file(graph_db: GraphDatabase = None):
    """Load GO-published xrefs and Reactome-published GO assignments independently."""
    graph_db = graph_db or get_active_graph_db()
    await assert_pre_requisite(
        ANNOTATION_NAME, VOCABULARY_PREFIX_1, VOCABULARY_PREFIX_2, FILE_PATHS, graph_db,
    )

    go_annotations = load_obo_xref_annotations(
        FILE_PATHS[0], ConceptPrefix.GO, 'GO', 'Reactome', ConceptPrefix.REACTOME,
        'GO hasDbXref',
    )
    if go_annotations:
        await graph_db.save_annotations(go_annotations)

    frame = pd.read_csv(os.path.join(CONFIG.data_dir, FILE_PATHS[1]), dtype=str).fillna('')
    annotations = [
        _REACTOME.create(
            publisher_concept_id=row['reactome_id'],
            other_concept_id=row['external_id'].removeprefix('GO:'),
            annotation_type=AnnotationType.ANNOTATED_WITH,
            properties={'sourceRelation': row['source_relation']},
        )
        for _, row in frame.drop_duplicates(
            ['reactome_id', 'external_id', 'source_relation'],
        ).iterrows()
    ]
    if annotations:
        await graph_db.save_annotations(annotations)
    verbose_print(
        f'Loaded {len(go_annotations)} GO-published and {len(annotations)} '
        'Reactome-published annotations.',
    )
