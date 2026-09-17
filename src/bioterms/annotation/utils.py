from dataclasses import dataclass
from typing import Mapping

from bioterms.etc.enums import AnnotationType
from bioterms.etc.enums import ConceptPrefix
from bioterms.etc.errors import VocabularyNotLoaded, FilesNotFound
from bioterms.etc.utils import check_files_exist, verbose_print
from bioterms.database import GraphDatabase, get_active_graph_db
from bioterms.model.annotation import Annotation


@dataclass(frozen=True)
class AnnotationSource:
    """A published annotation dataset and its publisher-defined direction."""

    name: str
    publisher_prefix: ConceptPrefix | str
    other_prefix: ConceptPrefix | str

    def create(self,
               publisher_concept_id: str,
               other_concept_id: str,
               annotation_type: AnnotationType = AnnotationType.ANNOTATED_WITH,
               properties: Mapping[str, str] | None = None,
               ) -> Annotation:
        """Create an annotation with immutable provenance and publisher-first direction."""
        annotation_properties = dict(properties or {})
        existing_source = annotation_properties.get('source')
        if existing_source is not None and existing_source != self.name:
            raise ValueError(
                f'Annotation source is managed by AnnotationSource: '
                f'{existing_source!r} != {self.name!r}'
            )
        annotation_properties['source'] = self.name
        return Annotation(
            prefixFrom=self.publisher_prefix,
            conceptIdFrom=publisher_concept_id,
            prefixTo=self.other_prefix,
            conceptIdTo=other_concept_id,
            annotationType=annotation_type,
            properties=annotation_properties,
        )


def is_gene_annotation_prefix(prefix: ConceptPrefix | str) -> bool:
    """Return whether a target belongs to the provenance-excluded gene/symbol boundary."""
    value = prefix.value if isinstance(prefix, ConceptPrefix) else prefix
    return value.lower() in {
        ConceptPrefix.HGNC.value,
        ConceptPrefix.HGNC_SYMBOL.value,
    }


async def assert_vocabulary_loaded(prefix_1: ConceptPrefix,
                                   prefix_2: ConceptPrefix,
                                   graph_db: GraphDatabase = None):
    """
    Check if the annotation vocabulary graph is loaded in the primary graph database.
    :param prefix_1: The first vocabulary prefix to check.
    :param prefix_2: The second vocabulary prefix to check.
    :param graph_db: Optional GraphDatabase instance to use.
    :raises VocabularyNotLoaded: If any of the vocabularies is not loaded.
    """
    if graph_db is None:
        graph_db = get_active_graph_db()

    prefix_1_count = await graph_db.count_terms(prefix_1)
    if prefix_1_count == 0:
        raise VocabularyNotLoaded(f'Vocabulary with prefix {prefix_1} is not loaded in the graph database.')

    prefix_2_count = await graph_db.count_terms(prefix_2)
    if prefix_2_count == 0:
        raise VocabularyNotLoaded(f'Vocabulary with prefix {prefix_2} is not loaded in the graph database.')


async def assert_pre_requisite(annotation_name: str,
                               prefix_1: ConceptPrefix,
                               prefix_2: ConceptPrefix,
                               file_paths: list[str],
                               graph_db: GraphDatabase = None,
                               ):
    verbose_print(f'Loading annotation: {annotation_name}...')

    await assert_vocabulary_loaded(
        prefix_1=prefix_1,
        prefix_2=prefix_2,
        graph_db=graph_db,
    )

    verbose_print(f'Confirmed {prefix_1} and {prefix_2} vocabularies are loaded.')

    annotation_count = await graph_db.count_annotations(
        prefix_1=prefix_1,
        prefix_2=prefix_2,
    )

    if annotation_count > 0:
        verbose_print(f'Annotation {annotation_name} already loaded with {annotation_count} entries, skipping.')
        return  # Annotations already exist, skip loading

    if not check_files_exist(file_paths):
        raise FilesNotFound(f'{annotation_name} file not found. Please download it first.')
