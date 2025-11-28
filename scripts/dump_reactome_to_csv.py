"""
This file is used to load the Reactome database from the Neo4j dump to CSV files.

This is because the official release is on Neo4j 4.X, while we use 5.X. To use this file,
load the Neo4j dump into a 4.X instance, then run this script to export the data to CSV files.
"""

import os
from typing import LiteralString, cast
import pandas as pd
from neo4j import GraphDatabase, Driver


neo4j_uri = "bolt://localhost:7687"
neo4j_user = "neo4j"
neo4j_password = "password"

output_dir = "../data"


def _load_entity(driver: Driver,
                 entity_type: str,
                 ):
    with driver.session() as session:
        query = cast(
            LiteralString,
            f"""
            MATCH (n:{entity_type})
            WHERE n.speciesName = "Homo sapiens"
            RETURN n;
            """,
        )

        result = session.run(
            query
        )

        data = []

        for record in result:
            record = record[0]
            display_name = record['displayName']
            synonyms = record['name']

            if display_name in synonyms:
                synonyms.remove(display_name)

            data.append({
                'id': record['dbId'],
                'display_name': display_name,
                'synonyms': '|'.join(synonyms),
                'st_id': record['stId'],
            })

    return pd.DataFrame(data)


def _load_relationship(driver: Driver,
                       source_type: str,
                       target_type: str,
                       source_name: str,
                       target_name: str,
                       ):
    with driver.session() as session:
        query = cast(
            LiteralString,
            f"""
            MATCH (s:{source_type})-[]-(t:{target_type})
            WHERE s.speciesName = "Homo sapiens"
            RETURN s.stId AS source_st_id, t.stId AS target_st_id;
            """,
        )

        result = session.run(
            query
        )

        relationships = []

        for record in result:
            relationships.append({
                source_name: record['source_st_id'],
                target_name: record['target_st_id'],
            })

    return pd.DataFrame(relationships)


def main():
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    # Generate the entities dataframes
    pathway_df = _load_entity(driver, 'Pathway')
    pathway_df.to_csv(f'{output_dir}/reactome/pathway.csv', index=False)
    del pathway_df

    reaction_df = _load_entity(driver, 'ReactionLikeEvent')
    reaction_df.to_csv(f'{output_dir}/reactome/reaction.csv', index=False)
    del reaction_df

    gene_df = _load_entity(driver, 'GenomeEncodedEntity')
    gene_df.to_csv(f'{output_dir}/gene.csv', index=False)
    del gene_df

    # Generate the relationships
    pathway_to_reaction_df = _load_relationship(
        driver=driver,
        source_type='Pathway',
        target_type='ReactionLikeEvent',
        source_name='pathway',
        target_name='reaction',
    )
    pathway_to_reaction_df.to_csv(f'{output_dir}/reactome/pathway_to_reaction.csv', index=False)
    del pathway_to_reaction_df

    reaction_to_gene_df = _load_relationship(
        driver=driver,
        source_type='ReactionLikeEvent',
        target_type='GenomeEncodedEntity',
        source_name='reaction',
        target_name=r'gene',
    )
    reaction_to_gene_df.to_csv(f'{output_dir}/reactome/reaction_to_gene.csv', index=False)
    del reaction_to_gene_df


if __name__ == '__main__':
    neo4j_uri = os.getenv('NEO4J_URI', neo4j_uri)
    neo4j_user = os.getenv('NEO4J_USER', neo4j_user)
    neo4j_password = os.getenv('NEO4J_PASSWORD', neo4j_password)
    output_dir = os.getenv('OUTPUT_DIR', output_dir)

    main()
