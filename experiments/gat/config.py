from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Cfg:
    out_dir: Path = Path('graph')
    checkpoints_dir: Path = Path('checkpoints')

    # Neo4j
    neo4j_uri: str = 'bolt://localhost:17687'
    neo4j_user: str = 'neo4j'
    neo4j_pass: str = 'password'

    # Qdrant
    qdrant_url: str = 'http://localhost:16333'

    # Embeddings
    emb_dim: int = 768
    emb_dtype: str = 'float16'

    # Graph export
    edge_chunk_rows: int = 5_000_000
    node_chunk_rows: int = 1_000_000


cfg = Cfg()
