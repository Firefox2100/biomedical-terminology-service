from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Cfg:
    gat_path = Path('../gat')
    chunk_edges = 2_000_000
    add_inverses = True
    out_dir = Path('graph')
    checkpoint_dir: Path = Path('checkpoints')
    undirected_edges = [
        "annotated_with",
        "exact",
    ]


cfg = Cfg()
