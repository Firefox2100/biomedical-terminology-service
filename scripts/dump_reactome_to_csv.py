"""
This file is used to load the Reactome database from the Neo4j dump to CSV files.

This is because the official release is on Neo4j 4.X, while we use 5.X. To use this file,
load the Neo4j dump into a 4.X instance, then run this script to export the data to CSV files.
"""

import os
import csv
import json
from neo4j import GraphDatabase, Driver, EagerResult


neo4j_uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
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

    gene_symbol_result = driver.execute_query(
        """
        MATCH (g:GenomeEncodedEntity {speciesName: "Homo sapiens"})
            -[:referenceEntity]->
            (rg:ReferenceGeneProduct)
        RETURN DISTINCT g.stId as gene_id,
            rg.identifier as symbol;
        """
    )

    write_to_csv(
        file_path=f'{output_dir}/gene_mapping.csv',
        field_names=['gene_id', 'symbol'],
        query_result=gene_symbol_result,
    )


def extract_reactome_data():
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    extract_pathway(driver)
    extract_reactions(driver)
    extract_genes(driver)


if __name__ == '__main__':
    extract_reactome_data()
