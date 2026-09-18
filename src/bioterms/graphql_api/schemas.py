"""
Module for loading GraphQL schema files for the BioTerms GraphQL API.
"""

import importlib.resources as pkg_resources
from ariadne import load_schema_from_path


def load_schema_file(file_name: str) -> str:
    """
    Load a GraphQL schema from the specified file name.
    :param file_name: The name of the schema file to load, without the .graphql extension.
    :return: A string containing the GraphQL schema.
    """
    schema_path = pkg_resources.files('bioterms.data.graphql') / f'{file_name}.graphql'
    schema = load_schema_from_path(str(schema_path))

    return schema


CONCEPT_SCHEMA = load_schema_file('concept')

CTV3_SCHEMA = load_schema_file('ctv3')
ENSEMBL_SCHEMA = load_schema_file('ensembl')
GO_SCHEMA = load_schema_file('go')
GENE_SCHEMA = load_schema_file('gene')
HGNC_SCHEMA = load_schema_file('hgnc')
HPO_SCHEMA = load_schema_file('hpo')
LOINC_SCHEMA = load_schema_file('loinc')
MONDO_SCHEMA = load_schema_file('mondo')
NCIT_SCHEMA = load_schema_file('ncit')
OHDSI_SCHEMA = load_schema_file('ohdsi')
OMIM_SCHEMA = load_schema_file('omim')
ORDO_SCHEMA = load_schema_file('ordo')
REACTOME_SCHEMA = load_schema_file('reactome')
RXNORM_SCHEMA = load_schema_file('rxnorm')
SNOMED_SCHEMA = load_schema_file('snomed')
UBERON_SCHEMA = load_schema_file('uberon')
UNIPROT_SCHEMA = load_schema_file('uniprot')

CTV3_SNOMED_SCHEMA = load_schema_file('ctv3_snomed')
ENSEMBL_GENE_SCHEMA = load_schema_file('ensembl_gene')
ENSEMBL_HGNC_SCHEMA = load_schema_file('ensembl_hgnc')
ENSEMBL_OMIM_SCHEMA = load_schema_file('ensembl_omim')
ENSEMBL_REACTOME_SCHEMA = load_schema_file('ensembl_reactome')
ENSEMBL_UNIPROT_SCHEMA = load_schema_file('ensembl_uniprot')
GENE_HPO_SCHEMA = load_schema_file('gene_hpo')
GENE_NCIT_SCHEMA = load_schema_file('gene_ncit')
GENE_OMIM_SCHEMA = load_schema_file('gene_omim')
GENE_ORDO_SCHEMA = load_schema_file('gene_ordo')
GENE_UNIPROT_SCHEMA = load_schema_file('gene_uniprot')
GO_REACTOME_SCHEMA = load_schema_file('go_reactome')
GO_UBERON_SCHEMA = load_schema_file('go_uberon')
GO_UNIPROT_SCHEMA = load_schema_file('go_uniprot')
HGNC_OMIM_SCHEMA = load_schema_file('hgnc_omim')
HGNC_UNIPROT_SCHEMA = load_schema_file('hgnc_uniprot')
HGNC_MONDO_SCHEMA = load_schema_file('hgnc_mondo')
HGNC_REACTOME_SCHEMA = load_schema_file('hgnc_reactome')
HPO_MONDO_SCHEMA = load_schema_file('hpo_mondo')
HPO_OMIM_SCHEMA = load_schema_file('hpo_omim')
HPO_ORDO_SCHEMA = load_schema_file('hpo_ordo')
LOINC_SNOMED_SCHEMA = load_schema_file('loinc_snomed')
LOINC_RXNORM_SCHEMA = load_schema_file('loinc_rxnorm')
OHDSI_RXNORM_SCHEMA = load_schema_file('ohdsi_rxnorm')
MONDO_NCIT_SCHEMA = load_schema_file('mondo_ncit')
MONDO_OMIM_SCHEMA = load_schema_file('mondo_omim')
MONDO_ORDO_SCHEMA = load_schema_file('mondo_ordo')
MONDO_SNOMED_SCHEMA = load_schema_file('mondo_snomed')
NCIT_OHDSI_SCHEMA = load_schema_file('ncit_ohdsi')
NCIT_REACTOME_SCHEMA = load_schema_file('ncit_reactome')
NCIT_UBERON_SCHEMA = load_schema_file('ncit_uberon')
OHDSI_SNOMED_SCHEMA = load_schema_file('ohdsi_snomed')
OMIM_ORDO_SCHEMA = load_schema_file('omim_ordo')
OMIM_REACTOME_SCHEMA = load_schema_file('omim_reactome')
OMIM_UNIPROT_SCHEMA = load_schema_file('omim_uniprot')
ORDO_SNOMED_SCHEMA = load_schema_file('ordo_snomed')
ORDO_UNIPROT_SCHEMA = load_schema_file('ordo_uniprot')
REACTOME_UNIPROT_SCHEMA = load_schema_file('reactome_uniprot')
RXNORM_SNOMED_SCHEMA = load_schema_file('rxnorm_snomed')
SNOMED_UBERON_SCHEMA = load_schema_file('snomed_uberon')
