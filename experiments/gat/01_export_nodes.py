"""
Export nodes from Neo4j to a Parquet file with a mapping from Concept IDs to integer node IDs.
"""

import json
from pathlib import Path
import pandas as pd
from neo4j import GraphDatabase
from config import cfg


# Skipping the isolated nodes
CYPHER_QUERY = """
MATCH (n:Concept)-[]-()
RETURN n.id as id, n.prefix as prefix
"""


def export_nodes():
    cfg.out_dir.mkdir(parents=True, exist_ok=True)

    driver = GraphDatabase.driver(
        uri=cfg.neo4j_uri,
        auth=(cfg.neo4j_user, cfg.neo4j_pass),
    )
    rows = []

    with driver.session() as session:
        result = session.run(CYPHER_QUERY)

        for record in result:
            rows.append(f'{record["prefix"]}:{record["id"]}')

    driver.close()

    df = pd.DataFrame({'cid': rows}).drop_duplicates()
    df = df.sort_values('cid').reset_index(drop=True)
    df['nid'] = df.index.astype('int64')

    out_map = cfg.out_dir / 'node_id_map.parquet'
    df.to_parquet(out_map, index=False)

    meta = {
        'num_nodes': int(df.shape[0]),
        'cid_field': 'cid',
        'nid_field': 'nid',
    }
    (cfg.out_dir / 'meta.json').write_text(json.dumps(meta, indent=2))
    print(f'Exported {meta["num_nodes"]} nodes to {out_map}')


if __name__ == '__main__':
    export_nodes()
