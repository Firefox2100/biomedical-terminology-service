import json
import numpy as np
import pandas as pd
from qdrant_client import QdrantClient

from config import cfg


def export_embeddings():
    meta = json.loads((cfg.out_dir / 'meta.json').read_text())
    N = meta['num_nodes']
    D = cfg.emb_dim

    df = pd.read_parquet(cfg.out_dir / 'node_id_map.parquet', columns=['cid', 'nid'])
    cid_to_nid = dict(zip(df['cid'].tolist(), df['nid'].tolist()))

    cfg.out_dir.mkdir(parents=True, exist_ok=True)

    out_path = cfg.out_dir / 'x.emb.f16.npy'
    x = np.lib.format.open_memmap(
        out_path,
        mode='w+',
        dtype=np.float16,
        shape=(N, D),
    )

    client = QdrantClient(url=cfg.qdrant_url)
    collections = [c.name for c in client.get_collections().collections]

    batch_size = 10_000
    for collection in collections:
        scroll_offset = None
        written = 0

        while True:
            points, scroll_offset = client.scroll(
                collection_name=collection,
                limit=batch_size,
                offset=scroll_offset,
                with_payload=True,
                with_vectors=True,
            )

            if not points:
                break

            for point in points:
                concept_id = point.payload['conceptId']
                cid = f'{collection}:{concept_id}'
                if cid not in cid_to_nid:
                    continue

                nid = cid_to_nid[cid]
                vector = np.asarray(point.vector, dtype=np.float16)
                if vector.shape[0] != D:
                    raise ValueError(f'Unexpected embedding dimension for {cid}: {vector.shape[0]} != {D}')
                x[nid] = vector
                written += 1

            if scroll_offset is None:
                break

            if written % 200_000 == 0:
                x.flush()
                print(f'Written {written} embeddings so far...')

        x.flush()
        print(f'Finished exporting embeddings for collection {collection}. Total written: {written}')


if __name__ == '__main__':
    export_embeddings()
