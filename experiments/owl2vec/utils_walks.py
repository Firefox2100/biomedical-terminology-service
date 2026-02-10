import numpy as np
from typing import Tuple


class CSRGraph:
    """
    CSR graph with per-edge relation id.
    indptr: (N+1) int64
    indices: (E) int32 (dst)
    rel: (E) uint16/uint32 (relation id in [0, R_rel_tokens))
    """
    def __init__(self, indptr: np.memmap, indices: np.memmap, rel: np.memmap, n_nodes: int, n_rel: int):
        self.indptr = indptr
        self.indices = indices
        self.rel = rel
        self.n_nodes = n_nodes
        self.n_rel = n_rel

    def neighbors(self, u: int) -> Tuple[np.ndarray, np.ndarray]:
        s = int(self.indptr[u])
        e = int(self.indptr[u + 1])
        return self.indices[s:e], self.rel[s:e]


def random_walk_interleaved(
    g: CSRGraph,
    start: int,
    walk_len_nodes: int,
    rng: np.random.Generator,
    rel_token_offset: int,
) -> np.ndarray:
    """
    Returns an interleaved token sequence:
      [node0, relTok0, node1, relTok1, node2, ...]
    Total nodes visited = walk_len_nodes (including start).
    Total tokens length = 2*walk_len_nodes - 1.
    relTok = rel_token_offset + rel_id
    """
    u = int(start)
    tokens = np.empty((2 * walk_len_nodes - 1,), dtype=np.int64)
    tokens[0] = u

    for i in range(1, walk_len_nodes):
        nbrs, rels = g.neighbors(u)
        if nbrs.size == 0:
            # restart to a random node if dead-end
            u = int(rng.integers(0, g.n_nodes))
            tokens[2*i - 1] = rel_token_offset  # dummy relation token 0
            tokens[2*i] = u
            continue

        j = int(rng.integers(0, nbrs.size))
        v = int(nbrs[j])
        r = int(rels[j])
        tokens[2*i - 1] = rel_token_offset + r
        tokens[2*i] = v
        u = v

    return tokens


def iter_skipgram_pairs(tokens: np.ndarray, window: int):
    """
    Yield (center, context) pairs for skip-gram, given a token sequence.
    """
    L = tokens.shape[0]
    for i in range(L):
        c = int(tokens[i])
        lo = max(0, i - window)
        hi = min(L, i + window + 1)
        for j in range(lo, hi):
            if j == i:
                continue
            yield c, int(tokens[j])
