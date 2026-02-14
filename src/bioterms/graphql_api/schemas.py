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
GENE_SCHEMA = load_schema_file('gene')
HGNC_SCHEMA = load_schema_file('hgnc')
HPO_SCHEMA = load_schema_file('hpo')
NCIT_SCHEMA = load_schema_file('ncit')
OHDSI_SCHEMA = load_schema_file('ohdsi')
OMIM_SCHEMA = load_schema_file('omim')
ORDO_SCHEMA = load_schema_file('ordo')
REACTOME_SCHEMA = load_schema_file('reactome')
SNOMED_SCHEMA = load_schema_file('snomed')

CTV3_SNOMED_SCHEMA = load_schema_file('ctv3_snomed')
GENE_HPO_SCHEMA = load_schema_file('gene_hpo')
GENE_NCIT_SCHEMA = load_schema_file('gene_ncit')
GENE_OMIM_SCHEMA = load_schema_file('gene_omim')
GENE_ORDO_SCHEMA = load_schema_file('gene_ordo')
HPO_ORDO_SCHEMA = load_schema_file('hpo_ordo')
OMIM_ORDO_SCHEMA = load_schema_file('omim_ordo')
ORDO_SNOMED_SCHEMA = load_schema_file('ordo_snomed')
