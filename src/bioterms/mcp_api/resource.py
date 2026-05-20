from bioterms.etc.enums import ConceptPrefix
from bioterms.vocabulary import get_vocabulary_license
from .app import mcp


@mcp.resource('license://ctv3')
def get_ctv3_license():
    """
    Get the release licence for the CTV3 vocabulary.
    """
    return get_vocabulary_license(ConceptPrefix.CTV3)


@mcp.resource('license://ensembl')
def get_ensembl_license():
    """
    Get the release licence for the Ensembl vocabulary.
    """
    return get_vocabulary_license(ConceptPrefix.ENSEMBL)


@mcp.resource('license://hgnc')
def get_hgnc_license():
    """
    Get the release licence for the HGNC vocabulary.
    """
    return get_vocabulary_license(ConceptPrefix.HGNC)


@mcp.resource('license://gene')
def get_gene_license():
    """
    Get the release licence for the gene vocabulary.
    """
    return get_vocabulary_license(ConceptPrefix.HGNC_SYMBOL)


@mcp.resource('license://hpo')
def get_hpo_license():
    """
    Get the release licence for the HPO vocabulary.
    """
    return get_vocabulary_license(ConceptPrefix.HPO)


@mcp.resource('license://ncit')
def get_ncit_license():
    """
    Get the release licence for the NCIT vocabulary.
    """
    return get_vocabulary_license(ConceptPrefix.NCIT)


@mcp.resource('license://ohdsi')
def get_ohdsi_license():
    """
    Get the release licence for the OHDSI vocabulary.
    """
    return get_vocabulary_license(ConceptPrefix.OHDSI)


@mcp.resource('license://omim')
def get_omim_license():
    """
    Get the release licence for the OMIM vocabulary.
    """
    return get_vocabulary_license(ConceptPrefix.OMIM)


@mcp.resource('license://ordo')
def get_ordo_license():
    """
    Get the release licence for the ORDO vocabulary.
    """
    return get_vocabulary_license(ConceptPrefix.ORDO)


@mcp.resource('license://reactome')
def get_reactome_license():
    """
    Get the release licence for the Reactome vocabulary.
    """
    return get_vocabulary_license(ConceptPrefix.REACTOME)


@mcp.resource('license://snomed')
def get_snomed_license():
    """
    Get the release licence for the SNOMED vocabulary.
    """
    return get_vocabulary_license(ConceptPrefix.SNOMED)
