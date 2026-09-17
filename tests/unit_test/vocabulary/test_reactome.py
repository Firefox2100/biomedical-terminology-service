import pytest

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
import bioterms.vocabulary.reactome as reactome
import bioterms.vocabulary.uniprot as uniprot


def test_ann_prefix_is_uniprot_not_hgnc_symbol():
    # The whole point of the change: Reactome must no longer resolve straight to
    # HGNC_SYMBOL -- that duplication is what caused the double-counted vote.
    assert ConceptPrefix.UNIPROT in reactome.ANNOTATIONS
    assert ConceptPrefix.HGNC_SYMBOL not in reactome.ANNOTATIONS
    assert ConceptPrefix.REACTOME in uniprot.ANNOTATIONS


@pytest.mark.asyncio
async def test_load_vocabulary_from_file_does_not_build_uniprot_annotations(monkeypatch, tmp_path):
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
    monkeypatch.setattr(CONFIG, 'data_dir', str(tmp_path))

    async def ignore_write(**_kwargs):
        pass

    monkeypatch.setattr(reactome, 'write_concepts_to_file', ignore_write)
    monkeypatch.setattr(reactome, 'write_graph_to_file', ignore_write)

    await reactome.load_vocabulary_from_file(offline=True, build_search_index=False)

    assert not (tmp_path / 'offline' / 'reactome-uniprot.annotation.dump').exists()
