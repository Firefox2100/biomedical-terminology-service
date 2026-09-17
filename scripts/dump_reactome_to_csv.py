"""
This file is used to load the Reactome database from the Neo4j dump to CSV files.

This is because the official release is on Neo4j 4.X, while we use 5.X. To use this file,
load the Neo4j dump into a 4.X instance, then run this script to export the data to CSV files.
"""

import os
import csv
import json
from neo4j import GraphDatabase, Driver, EagerResult


neo4j_uri = os.getenv('NEO4J_URI', 'bolt://localhost:27687')
neo4j_user = os.getenv('NEO4J_USER', 'neo4j')
neo4j_password = os.getenv('NEO4J_PASSWORD', 'password')
output_dir = os.getenv('OUTPUT_DIR', '../data/reactome')


def normalise_row(row: dict) -> dict:
    out = {}
    for k, v in row.items():
        if isinstance(v, (list, dict)):     # optionally include dicts
            out[k] = json.dumps(v)
        else:
            out[k] = v
    return out


def write_to_csv(file_path: str,
                 field_names: list[str],
                 query_result: EagerResult,
                 ):
    """
    Write query result to CSV file. The result must be in the exact shape of the field names.
    :param file_path: The path to the output CSV file.
    :param field_names: The field names for the CSV file.
    :param query_result: The query result to write.
    """
    print('Writing to', file_path)

    with open(file_path, 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=field_names)
        writer.writeheader()

        for record in query_result.records:
            writer.writerow(normalise_row(record.data()))


def extract_pathway(driver: Driver):
    """
    Extract pathways from Reactome.
    :param driver: The Neo4j driver.
    """
    pathway_result = driver.execute_query(
        """
        MATCH (p:Pathway)
        WHERE p.speciesName = "Homo sapiens"
        RETURN p.dbId AS db_id,
            p.stId AS st_id,
            p.displayName AS display_name;
        """
    )

    write_to_csv(
        file_path=f'{output_dir}/pathway.csv',
        field_names=['db_id', 'st_id', 'display_name'],
        query_result=pathway_result,
    )

    pathway_hierarchy_result = driver.execute_query(
        """
        MATCH (p:Pathway {speciesName: "Homo sapiens"})
            -[:hasEvent]->
            (sp: Pathway {speciesName: "Homo sapiens"})
        return DISTINCT p.stId as parent_st_id,
            sp.stId as sub_pathway_st_id;
        """
    )

    write_to_csv(
        file_path=f'{output_dir}/pathway_hierarchy.csv',
        field_names=['parent_st_id', 'sub_pathway_st_id'],
        query_result=pathway_hierarchy_result,
    )


def extract_reactions(driver: Driver):
    """
    Extract reactions (and other ReactionLikeEvent) from the Reactome database
    :param driver: The Neo4j driver
    """
    reaction_result = driver.execute_query(
        """
        MATCH (n:ReactionLikeEvent)
        WHERE n.speciesName = "Homo sapiens"
        RETURN n.dbId As db_id,
            n.stId AS st_id,
            n.displayName AS display_name,
            n.name AS synonyms,
            n.isInferred as inferred;
        """
    )

    write_to_csv(
        file_path=f'{output_dir}/reaction.csv',
        field_names=['db_id', 'st_id', 'display_name', 'synonyms', 'inferred'],
        query_result=reaction_result,
    )

    reaction_order_result = driver.execute_query(
        """
        MATCH (n:ReactionLikeEvent {speciesName: "Homo sapiens"})
            -[:precedingEvent]->
            (pn:ReactionLikeEvent {speciesName: "Homo sapiens"})
        RETURN DISTINCT n.stId as reaction_id,
            pn.stId as preceding_reaction_id;
        """
    )

    write_to_csv(
        file_path=f'{output_dir}/reaction_order.csv',
        field_names=['reaction_id', 'preceding_reaction_id'],
        query_result=reaction_order_result,
    )

    reaction_pathway_result = driver.execute_query(
        """
        MATCH (p:Pathway {speciesName: "Homo sapiens"})
            -[:hasEvent]->
            (n:ReactionLikeEvent {speciesName: "Homo sapiens"})
        RETURN DISTINCT p.stId as pathway_id,
            n.stId as reaction_id;
        """
    )

    write_to_csv(
        file_path=f'{output_dir}/reaction_pathway.csv',
        field_names=['pathway_id', 'reaction_id'],
        query_result=reaction_pathway_result,
    )


def extract_genes(driver: Driver):
    """
    Extract genes from the Reactome database
    :param driver: The Neo4j driver
    """
    gene_result = driver.execute_query(
        """
        MATCH (g:GenomeEncodedEntity)
        WHERE g.speciesName = "Homo sapiens"
        RETURN g.dbId As db_id,
            g.stId AS st_id,
            g.displayName AS display_name,
            g.name AS synonyms;
        """
    )

    write_to_csv(
        file_path=f'{output_dir}/gene.csv',
        field_names=['db_id', 'st_id', 'display_name', 'synonyms'],
        query_result=gene_result,
    )

    gene_reaction_result = driver.execute_query(
        """
        MATCH (n:ReactionLikeEvent {speciesName: "Homo sapiens"})
            -[r]->
            (g:GenomeEncodedEntity {speciesName: "Homo sapiens"})
        RETURN DISTINCT g.stId as gene_id,
            TYPE(r) as relationship,
            n.stId as reaction_id;
        """
    )

    write_to_csv(
        file_path=f'{output_dir}/gene_reaction.csv',
        field_names=['gene_id', 'relationship', 'reaction_id'],
        query_result=gene_reaction_result,
    )

    uniprot_mapping_result = driver.execute_query(
        """
        MATCH (g:GenomeEncodedEntity {speciesName: "Homo sapiens"})
            -[:referenceEntity]->
            (rg:ReferenceGeneProduct)
            -[:referenceDatabase]->
            (db:ReferenceDatabase {displayName: "UniProt"})
        RETURN DISTINCT g.stId as reactome_id,
            rg.identifier as external_id;
        """
    )

    write_to_csv(
        file_path=f'{output_dir}/uniprot_mapping.csv',
        field_names=['reactome_id', 'external_id'],
        query_result=uniprot_mapping_result,
    )


def extract_physical_entities(driver: Driver):
    """Extract stable non-genome physical entities used by the human Reactome graph."""
    entity_result = driver.execute_query(
        """
        MATCH (entity:PhysicalEntity)
        WHERE NOT entity:GenomeEncodedEntity
            AND (entity.stId STARTS WITH "R-HSA-" OR entity.stId STARTS WITH "R-ALL-")
        RETURN entity.dbId AS db_id,
            entity.stId AS st_id,
            entity.displayName AS display_name,
            entity.name AS synonyms,
            entity.schemaClass AS schema_class
        """
    )
    write_to_csv(
        file_path=f'{output_dir}/physical_entity.csv',
        field_names=['db_id', 'st_id', 'display_name', 'synonyms', 'schema_class'],
        query_result=entity_result,
    )

    relationship_result = driver.execute_query(
        """
        MATCH (reaction:ReactionLikeEvent {speciesName: "Homo sapiens"})
            -[relationship]->(entity:PhysicalEntity)
        WHERE type(relationship) IN ["input", "output"]
        RETURN DISTINCT entity.stId AS entity_id,
            type(relationship) AS relationship,
            reaction.stId AS reaction_id
        """
    )
    write_to_csv(
        file_path=f'{output_dir}/physical_entity_reaction.csv',
        field_names=['entity_id', 'relationship', 'reaction_id'],
        query_result=relationship_result,
    )


def extract_external_reference_annotations(driver: Driver):
    """Extract supported mappings published in Reactome ReferenceEntity records."""
    ensembl_result = driver.execute_query(
        """
        MATCH (g:GenomeEncodedEntity {speciesName: "Homo sapiens"})
            -[:referenceEntity]->(direct:ReferenceEntity)
            -[:referenceDatabase]->(:ReferenceDatabase {displayName: "ENSEMBL"})
        RETURN g.stId AS reactome_id, direct.identifier AS external_id
        UNION
        MATCH (g:GenomeEncodedEntity {speciesName: "Homo sapiens"})
            -[:referenceEntity]->(:ReferenceGeneProduct)
            -[:referenceGene|referenceTranscript]->(reference:ReferenceEntity)
            -[:referenceDatabase]->(:ReferenceDatabase {displayName: "ENSEMBL"})
        RETURN g.stId AS reactome_id, reference.identifier AS external_id
        """
    )
    write_to_csv(
        file_path=f'{output_dir}/ensembl_mapping.csv',
        field_names=['reactome_id', 'external_id'],
        query_result=ensembl_result,
    )

    for database_name, file_name in (
        ('HGNC', 'hgnc_mapping.csv'),
        ('OMIM', 'omim_mapping.csv'),
    ):
        result = driver.execute_query(
            """
            MATCH (g:GenomeEncodedEntity {speciesName: "Homo sapiens"})
                -[:referenceEntity]->(:ReferenceGeneProduct)
                -[:referenceGene]->(reference:ReferenceEntity)
                -[:referenceDatabase]->(:ReferenceDatabase {displayName: $database_name})
            RETURN DISTINCT g.stId AS reactome_id,
                reference.identifier AS external_id
            """,
            database_name=database_name,
        )
        write_to_csv(
            file_path=f'{output_dir}/{file_name}',
            field_names=['reactome_id', 'external_id'],
            query_result=result,
        )

    for database_name, file_name in (
        ('ChEBI', 'chebi_mapping.csv'),
        ('NCIthesaurus', 'ncit_mapping.csv'),
    ):
        result = driver.execute_query(
            """
            MATCH (entity:PhysicalEntity)-[:referenceEntity]->(reference:ReferenceEntity)
                -[:referenceDatabase]->(:ReferenceDatabase {displayName: $database_name})
            WHERE entity.stId STARTS WITH "R-HSA-" OR entity.stId STARTS WITH "R-ALL-"
            RETURN DISTINCT entity.stId AS reactome_id,
                reference.identifier AS external_id
            """,
            database_name=database_name,
        )
        write_to_csv(
            file_path=f'{output_dir}/{file_name}',
            field_names=['reactome_id', 'external_id'],
            query_result=result,
        )


def extract_reactome_data():
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        extract_pathway(driver)
        extract_reactions(driver)
        extract_genes(driver)
        extract_physical_entities(driver)
        extract_external_reference_annotations(driver)
    finally:
        driver.close()


if __name__ == '__main__':
    extract_reactome_data()
