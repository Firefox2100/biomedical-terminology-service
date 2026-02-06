import json
from pathlib import Path
import numpy as np
import pandas as pd
from pymongo import MongoClient

from config import cfg


EDGE_CHUNK = 50_000_000
COPY_CHUNK = 1_000_000


def get_cid_with_embedding() -> set[str]:
    """
    Get a set of CIDs for which we have embeddings in Qdrant. Qdrant data is from the MongoDB
    concepts, so scan MongoDB for all concepts and get their CIDs.
    :return: A set of strings for CIDs
    """
    mongo_client = MongoClient(cfg.mongo_uri)
    mongo_database = mongo_client[cfg.mongo_db]

    collection_names = mongo_database.list_collection_names()
    collection_names = [name for name in collection_names if name != 'users']

    cids = set()
    for collection in collection_names:
        coll = mongo_database[collection]
        cursor = coll.find({'vectorId': {'$exists': True}}, {'_id': 0, 'conceptId': 1}).batch_size(50_000)

        for doc in cursor:
            concept_id = doc['conceptId']
            cid = f'{collection}:{concept_id}'
            cids.add(cid)

    return cids


def build_has_emb(N: int, cids_with_emb: set[str], df_node_map: pd.DataFrame) -> np.ndarray:
    has_emb = np.zeros(N, dtype=np.uint8)
    mask = df_node_map['cid'].isin(cids_with_emb).to_numpy()
    nids = df_node_map.loc[mask, 'nid'].to_numpy(dtype=np.int64)
    has_emb[nids] = 1

    return has_emb


def build_has_edge(N: int) -> np.ndarray:
    has_edge = np.zeros(N, dtype=np.uint8)
    edge_index_path = cfg.out_dir / 'edge_index.i64.npy'
    edge_index = np.load(edge_index_path, mmap_mode='r')

    E = edge_index.shape[1]
    for start in range(0, E, EDGE_CHUNK):
        end = min(start + EDGE_CHUNK, E)
        src = edge_index[0, start:end]
        dst = edge_index[1, start:end]
        has_edge[src] = 1
        has_edge[dst] = 1
        if start and start % (EDGE_CHUNK * 5) == 0:
            print(f"edge scan: {start:,}/{E:,}")

    return has_edge


def build_old_to_new(keep: np.ndarray) -> tuple[np.ndarray, int]:
    old_to_new = np.full(keep.shape[0], -1, dtype=np.int64)
    kept_old = np.nonzero(keep)[0].astype(np.int64)
    old_to_new[kept_old] = np.arange(kept_old.shape[0], dtype=np.int64)

    return old_to_new, int(kept_old.shape[0])


def copy_kept_x(keep: np.ndarray):
    x_old = np.load(cfg.out_dir / 'x.emb.f16.npy', mmap_mode='r')
    N, D = x_old.shape
    kept_idx = np.nonzero(keep)[0].astype(np.int64)
    N2 = kept_idx.shape[0]

    x2 = np.lib.format.open_memmap(cfg.out_dir / 'x.emb.final.f16.npy', mode='w+', dtype=np.float16, shape=(N2, D))

    for start in range(0, N2, COPY_CHUNK):
        end = min(start + COPY_CHUNK, N2)
        idx = kept_idx[start:end]
        x2[start:end] = x_old[idx]
        if start and start % (COPY_CHUNK * 10) == 0:
            x2.flush()
            print(f"Copied embeddings for {start:,}/{N2:,} kept nodes...")

    x2.flush()
    return int(N2), int(D)


def filter_and_remap_edges(old_to_new: np.ndarray):
    edge = np.load(cfg.out_dir / 'edge_index.i64.npy', mmap_mode='r')
    etype = np.load(cfg.out_dir / 'etype.u16.npy', mmap_mode='r')
    E = edge.shape[1]

    kept = 0
    for start in range(0, E, EDGE_CHUNK):
        end = min(start + EDGE_CHUNK, E)
        src = edge[0, start:end]
        dst = edge[1, start:end]
        src_new = old_to_new[src]
        dst_new = old_to_new[dst]

        kept += int(((src_new >= 0) & (dst_new >= 0)).sum())
        if start and start % (EDGE_CHUNK * 5) == 0:
            print(f"Filtering and remapping edges: {start:,}/{E:,}, kept so far: {kept:,}")

    out_edge = np.lib.format.open_memmap(
        cfg.out_dir / 'edge_index.final.i64.npy',
        mode='w+',
        dtype=np.int64,
        shape=(2, kept),
    )
    out_type = np.lib.format.open_memmap(
        cfg.out_dir / 'etype.final.u16.npy',
        mode='w+',
        dtype=np.uint16,
        shape=(kept,),
    )

    pos = 0
    for start in range(0, E, EDGE_CHUNK):
        end = min(start + EDGE_CHUNK, E)
        src = edge[0, start:end]
        dst = edge[1, start:end]
        et = etype[start:end]

        src_new = old_to_new[src]
        dst_new = old_to_new[dst]

        mask = (src_new >= 0) & (dst_new >= 0)
        n_kept = int(mask.sum())

        out_edge[:, pos:pos+n_kept] = np.stack([src_new[mask], dst_new[mask]])
        out_type[pos:pos+n_kept] = et[mask]

        pos += n_kept
        if start and start % (EDGE_CHUNK * 5) == 0:
            out_edge.flush()
            out_type.flush()
            print(f"Filtering and remapping edges: {start:,}/{E:,}, kept so far: {pos:,}")

    out_edge.flush()
    out_type.flush()
    print(f"Finished filtering and remapping edges. Kept {pos:,} edges out of {E:,} total.")
    assert pos == kept, f"Expected to keep {kept} edges, but actually kept {pos}"
    return int(kept)


def rewrite_node_map(df_node_map: pd.DataFrame, old_to_new: np.ndarray):
    df = df_node_map.copy()
    keep_mask = df['nid'].map(lambda n: old_to_new[int(n)] >= 0).to_numpy()
    df = df.loc[keep_mask].copy()
    df['nid_old'] = df['nid'].astype(np.int64)
    df['nid'] = df['nid_old'].map(lambda n: int(old_to_new[int(n)])).astype(np.int64)
    df = df.sort_values('nid').reset_index(drop=True)

    df.to_parquet(cfg.out_dir / 'node_id_map.final.parquet', index=False)


def clean_up_data():
    meta = json.loads((cfg.out_dir / 'meta.json').read_text())
    N = int(meta['num_nodes'])

    df_node_map = pd.read_parquet(cfg.out_dir / 'node_id_map.parquet', columns=['cid', 'nid'])

    print('Loading CIDs with embeddings from MongoDB...')
    cids_with_emb = get_cid_with_embedding()
    print(f'Found {len(cids_with_emb):,} CIDs with embeddings in MongoDB.')

    has_emb = build_has_emb(N, cids_with_emb, df_node_map)
    print(f'{int(has_emb.sum()):,} out of {N} nodes have embeddings according to MongoDB.')
    del cids_with_emb  # free memory

    has_edge = build_has_edge(N)
    print(f'{int(has_edge.sum()):,} out of {N} nodes have at least one edge.')

    keep = (has_emb == 1) & (has_edge == 1)
    kept_nodes = int(keep.sum())
    print(f'Keeping {kept_nodes:,} nodes that have both embeddings and edges.')

    # Reindex the nodes to be contiguous after filtering
    old_to_new, N2 = build_old_to_new(keep)
    np.save(cfg.out_dir / 'old_to_new.i64.npy', old_to_new)
    print(f'Saved old_to_new mapping and determined new node count: {N2:,}')

    N2_check, D = copy_kept_x(keep)
    assert N2_check == N2, f"Expected {N2} nodes after copying embeddings, but got {N2_check}"
    print(f'Copied embeddings for kept nodes to new file. Final node count: {N2:,}, embedding dimension: {D}.')

    E2 = filter_and_remap_edges(old_to_new)
    rewrite_node_map(df_node_map, old_to_new)
    print(f'Finished filtering and remapping edges. Final edge count: {E2:,}. Updated node_id_map.parquet accordingly.')

    # Write updated meta
    out_meta = dict(meta)
    out_meta['num_nodes'] = N2
    out_meta['num_edges'] = E2
    out_meta['edge_index_path'] = str(cfg.out_dir / 'edge_index.final.i64.npy')
    out_meta['etype_path'] = str(cfg.out_dir / 'etype.final.u16.npy')
    out_meta['x_path'] = str(cfg.out_dir / 'x.emb.final.f16.npy')
    (cfg.out_dir / 'meta.json').write_text(json.dumps(out_meta, indent=2))
    print(f'Wrote updated meta to meta.json')


if __name__ == '__main__':
    clean_up_data()
