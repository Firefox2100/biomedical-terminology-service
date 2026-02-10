import os
import json
import numpy as np
import pandas as pd
from typing import Set, Tuple

from config import cfg


def read_undirected_edge_types() -> Set[int]:
    meta_path = cfg.gat_path / 'graph' / 'meta.json'
    meta = json.loads(meta_path.read_text())

    undirected_types = set()
    for edge in cfg.undirected_edges:
        if edge in meta['rel_to_id']:
            undirected_types.add(meta['rel_to_id'][edge])
        else:
            print(f'Warning: undirected edge type "{edge}" not found in meta, skipping.')

    return undirected_types


def load_n_nodes_from_parquet(node_id_map_parquet: str, nid_col: str = "nid") -> int:
    df = pd.read_parquet(node_id_map_parquet, columns=[nid_col])
    n = int(df[nid_col].max()) + 1
    return n


def open_memmap_npy(path: str, mode: str = "r") -> np.memmap:
    return np.load(path, mmap_mode=mode)


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def save_memmap_npy(path: str, shape: Tuple[int, ...], dtype: np.dtype) -> np.memmap:
    return np.lib.format.open_memmap(path, mode="w+", dtype=dtype, shape=shape)
