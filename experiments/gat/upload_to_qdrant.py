import json
import numpy as np
import pandas as pd
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue
from qdrant_client.http.models import HnswConfigDiff
from pymongo import MongoClient

from config import cfg


def create_collection(client: QdrantClient):
    collection_name = 'gat'

    # Delete it if it already exists
    if collection_name in [c.name for c in client.get_collections().collections]:
        client.delete_collection(collection_name=collection_name)

    client.create_collection(
        collection_name=collection_name,
        vectors_config=VectorParams(
            size=128,
            distance=Distance.COSINE,
        )
    )


def upload_points():
    client = QdrantClient(url=cfg.qdrant_url)
    create_collection(client)

    mongo_client = MongoClient(cfg.mongo_uri)
    mongo_database = mongo_client[cfg.mongo_db]

    meta = json.loads((cfg.out_dir / 'meta.json').read_text())
    num_nodes = int(meta['num_nodes'])

    df_node_map = pd.read_parquet(cfg.out_dir / 'node_id_map.parquet', columns=['cid', 'nid'])
    embedding = np.lib.format.open_memmap(
        cfg.out_dir / 'concept_emb.gat.out128.f16.npy',
        mode='r',
        dtype=np.float16,
        shape=(num_nodes, 128),
    )

    # Disable HNSW indexing for faster upload
    client.update_collection(
        collection_name='gat',
        hnsw_config=HnswConfigDiff(
            m=0,
        )
    )

    node_map: dict[str, dict[str, int]] = {}

    # Iterate the dataframe in batches
    for _, row in df_node_map.iterrows():
        cid = row['cid']
        nid = int(row['nid'])

        prefix, concept_id = cid.split(':', 1)

        if prefix not in node_map:
            node_map[prefix] = {}

        node_map[prefix][concept_id] = nid

    for prefix, concept_map in node_map.items():
        coll = mongo_database[prefix]

        points: list[PointStruct] = []

        cursor = coll.find({}, {'_id': 0, 'conceptId': 1, 'vectorId': 1}).batch_size(10_000)
        for doc in cursor:
            concept_id = doc['conceptId']
            vector_id = doc['vectorId']

            if concept_id not in concept_map:
                continue

            nid = concept_map[concept_id]
            vector = embedding[nid].tolist()

            point = PointStruct(
                id=vector_id,
                vector=vector,
                payload={
                    'conceptId': concept_id,
                    'prefix': prefix,
                }
            )
            points.append(point)

            if len(points) >= 10_000:
                client.upsert(
                    collection_name='gat',
                    points=points,
                )
                print(f'Uploaded {len(points)} points for prefix {prefix}...')
                points.clear()

        if points:
            client.upsert(
                collection_name='gat',
                points=points,
            )
            print(f'Uploaded {len(points)} points for prefix {prefix}...')

    # Turn the HNSW index back on
    client.update_collection(
        collection_name='gat',
        hnsw_config=HnswConfigDiff(
            m=16,
        )
    )


if __name__ == '__main__':
    upload_points()
