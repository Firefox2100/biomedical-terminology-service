"""
Export edges from Neo4j to .npy files in chunks. Each chunk contains edge_index and etype arrays.
The edge_index array has shape (2, num_edges_in_chunk) and contains source and destination node IDs.
The etype array has shape (num_edges_in_chunk,) and contains integer relation type IDs
corresponding to the edges in the chunk. The mapping from relation type strings to integer IDs is
saved in the meta.json file.
"""

import json
from pathlib import Path
import numpy as np
import pandas as pd
from neo4j import GraphDatabase
from config import cfg


CYPHER_QUERY = """
MATCH (a:Concept)-[r]->(b:Concept)
WHERE type(r) <> 'similar_to
RETURN
    a.id as src,
    a.prefix as src_prefix,
    b.id as dst,
    b.prefix as dst_prefix,
    type(r) as rel,
    r.label as label
"""


def load_map(map_path: Path) -> dict:
    df = pd.read_parquet(map_path, columns=['cid', 'nid'])
    return dict(zip(df['cid'].tolist(), df['nid'].tolist()))


def export_edges():
    cfg.out_dir.mkdir(parents=True, exist_ok=True)

    meta_path = cfg.out_dir / 'meta.json'
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}

    nid_map = load_map(cfg.out_dir / 'node_id_map.parquet')

    driver = GraphDatabase.driver(
        uri=cfg.neo4j_uri,
        auth=(cfg.neo4j_user, cfg.neo4j_pass),
    )

    rel_to_id: dict[str, int] = {}

    edge_dir = cfg.out_dir / 'edges_chunks'
    edge_dir.mkdir(exist_ok=True)

    src_buf: list[int] = []
    dst_buf: list[int] = []
    rel_buf: list[int] = []
    chunk_idx = 0
    total_edges = 0

    def flush_chunk():
        nonlocal chunk_idx, total_edges

        src = np.array(src_buf, dtype=np.int64)
        dst = np.array(dst_buf, dtype=np.int64)
        rel = np.array(rel_buf, dtype=np.int16)

        edge_index = np.stack([src, dst])

        np.save(edge_dir / f'edge_index_{chunk_idx:05d}.npy', edge_index)
        np.save(edge_dir / f'etype_{chunk_idx:05d}.npy', rel)

        total_edges += edge_index.shape[1]
        chunk_idx += 1

        src_buf.clear()
        dst_buf.clear()
        rel_buf.clear()

        print(
            f'Exported chunk {chunk_idx} with {edge_index.shape[1]} edges, '
            f'total so far: {total_edges}'
        )

    with driver.session(fetch_size=50_000) as session:
        result = session.run(CYPHER_QUERY)

        for record in result:
            s = f'{record["src_prefix"]}:{record["src"]}'
            d = f'{record["dst_prefix"]}:{record["dst"]}'

            if s not in nid_map or d not in nid_map:
                continue

            rel_type = record['rel']
            label_val = record.get('label')

            rs = rel_type if rel_type != 'ohdsi_relationship' else label_val
            if rs is None:
                continue

            if not isinstance(rs, (list, tuple, set)):
                rs = [rs]

            src_nid = nid_map[s]
            dst_nid = nid_map[d]

            for r in rs:
                if r is None:
                    continue
                r = str(r)

                if r not in rel_to_id:
                    rel_to_id[r] = len(rel_to_id)

                src_buf.append(src_nid)
                dst_buf.append(dst_nid)
                rel_buf.append(rel_to_id[r])

            while len(src_buf) >= cfg.edge_chunk_rows:
                flush_chunk()

    driver.close()

    if src_buf:
        # flush final
        src = np.array(src_buf, dtype=np.int64)
        dst = np.array(dst_buf, dtype=np.int64)
        rel = np.array(rel_buf, dtype=np.int16)

        edge_index = np.stack([src, dst])

        np.save(edge_dir / f'edge_index_{chunk_idx:05d}.npy', edge_index)
        np.save(edge_dir / f'etype_{chunk_idx:05d}.npy', rel)

        total_edges += edge_index.shape[1]
        chunk_idx += 1
        print(
            f'Exported final chunk {chunk_idx} with {edge_index.shape[1]} edges, '
            f'total edges: {total_edges}'
        )

    meta['num_edges'] = int(total_edges)
    meta['rel_to_id'] = rel_to_id
    meta['num_rels'] = len(rel_to_id)
    meta_path.write_text(json.dumps(meta, indent=2))
    print(f'Exported total {total_edges} edges with {len(rel_to_id)} relation types')


if __name__ == '__main__':
    export_edges()
