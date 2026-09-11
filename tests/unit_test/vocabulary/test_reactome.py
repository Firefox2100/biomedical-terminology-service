import pandas as pd
import pytest

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import AnnotationType, ConceptPrefix
import bioterms.vocabulary.reactome as reactome


def test_ann_prefix_is_uniprot_not_hgnc_symbol():
    # The whole point of the change: Reactome must no longer resolve straight to
    # HGNC_SYMBOL -- that duplication is what caused the double-counted vote.
    assert ConceptPrefix.UNIPROT in reactome.ANNOTATIONS
    assert ConceptPrefix.HGNC_SYMBOL not in reactome.ANNOTATIONS


@pytest.mark.asyncio
async def test_load_vocabulary_from_file_builds_exact_annotations_to_uniprot(monkeypatch, tmp_path):
    reactome_dir = tmp_path / 'reactome'
    reactome_dir.mkdir()
    (reactome_dir / 'pathway.csv').write_text('st_id,display_name\n')
    (reactome_dir / 'pathway_hierarchy.csv').write_text('sub_pathway_st_id,parent_st_id\n')
    (reactome_dir / 'reaction.csv').write_text('st_id,display_name,synonyms,inferred\n')
    (reactome_dir / 'reaction_order.csv').write_text('reaction_id,preceding_reaction_id\n')
    (reactome_dir / 'reaction_pathway.csv').write_text('reaction_id,pathway_id\n')
    (reactome_dir / 'gene.csv').write_text(
        'st_id,display_name,synonyms\nR-HSA-1,EEF1A1,\n'
    )
    (reactome_dir / 'gene_reaction.csv').write_text('reaction_id,gene_id,relationship\n')
    (reactome_dir / 'gene_mapping.csv').write_text('gene_id,symbol\nR-HSA-1,P68104\n')

    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    await reactome.load_vocabulary_from_file(offline=True)

    annotation_path = tmp_path / 'offline' / 'reactome-uniprot.annotation.dump'
    assert annotation_path.exists()

    rows = pd.read_csv(annotation_path, header=None).values.tolist()
    assert len(rows) == 1
    source_prefix, source_curie, target_prefix, target_curie, annotation_type, _properties = rows[0]
    assert source_prefix == 'reactome'
    assert source_curie == 'reactome:R-HSA-1'
    assert target_prefix == 'uniprot'
    assert target_curie == 'uniprot:P68104'
    assert annotation_type == AnnotationType.EXACT.value
