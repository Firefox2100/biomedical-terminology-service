import json
from pathlib import Path
import pandas as pd
import numpy as np


CONFIG_PATH = Path('../config.json')


def merge_node_parquets(parquet_files: dict[str, str]):
    config = json.loads(CONFIG_PATH.read_text())

    merged_df = pd.DataFrame(columns=['prefix', 'cid', 'type'])
    for prefix, path in parquet_files.items():
        df = pd.read_parquet(path)
        df['prefix'] = prefix
        merged_df = pd.concat([merged_df, df], ignore_index=True)

    merged_df = merged_df.drop_duplicates(subset=['prefix', 'cid'], keep='last')
    merged_df = merged_df.sort_values(['prefix', 'cid']).reset_index(drop=True)
    merged_df['nid'] = merged_df.index.astype('int64')

    out_path = 'data/merged_node_id_map.parquet'
    merged_df.to_parquet(out_path, index=False)
    print(f'Merged {merged_df.shape[0]} nodes to {out_path}')

    nodes = np.lib.format.open_memmap('data/node.npy', mode='w+', dtype=np.int64, shape=(3, merged_df.shape[0]))
    nodes[0, :] = merged_df['nid'].values
    nodes[1, :] = merged_df['prefix'].apply(lambda x: config['prefixes'].index(x)).values
    nodes[2, :] = merged_df['type'].apply(lambda x: config['conceptTypes'].index(x)).values
    nodes.flush()
    print('Exported node attributes to data/node.npy')


if __name__ == '__main__':
    parquet_files = {
        'ctv3': 'data/ctv3_node.parquet',
    }

    merge_node_parquets(parquet_files)
