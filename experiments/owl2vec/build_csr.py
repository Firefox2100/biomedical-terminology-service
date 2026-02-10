import json
import numpy as np

from utils_io import open_memmap_npy, read_undirected_edge_types, save_memmap_npy, ensure_dir
from config import cfg


def main():
    meta_path = cfg.gat_path / 'graph' / 'meta.json'
    meta = json.loads(meta_path.read_text())

    n_nodes = meta['num_nodes']
    n_rels = meta['num_rels']
    edge_index_path = str(cfg.gat_path / meta['edge_index_path'])
    etype_path = str(cfg.gat_path / meta['etype_path'])

    ensure_dir(str(cfg.out_dir))

    edge_index = open_memmap_npy(edge_index_path)
    etype = open_memmap_npy(etype_path)

    assert edge_index.shape[0] == 2, f'edge_index first dim must be 2, got {edge_index.shape}'
    E = edge_index.shape[1]
    assert etype.shape[0] == E

    undirected = read_undirected_edge_types()

    # relation id space:
    # 0..n_rels-1 original
    # n_rels..2*n_rels-1 inverses (if add_inverses)
    rel_vocab = 2 * n_rels if cfg.add_inverses else n_rels

    # Pass 1: compute degrees for CSR (out-degree)
    deg = np.zeros((n_nodes,), dtype=np.int64)

    for s0 in range(0, E, cfg.chunk_edges):
        s1 = min(E, s0 + cfg.chunk_edges)
        src = edge_index[0, s0:s1]
        dst = edge_index[1, s0:s1]
        r = etype[s0:s1].astype(np.int64)

        # original edges contribute +1 to src
        np.add.at(deg, src, 1)

        # undirected: add reciprocal edge (dst -> src) with same relation
        if undirected:
            mask_u = np.isin(r, list(undirected))
            if mask_u.any():
                np.add.at(deg, dst[mask_u], 1)

        # add inverse edges for directed types:
        if cfg.add_inverses:
            # For undirected, we already added reciprocal with same relation; also adding inverse is redundant.
            # For directed types, add inverse edge (dst -> src) with rel_id+n_rels
            if undirected:
                mask_d = ~np.isin(r, list(undirected))
            else:
                mask_d = np.ones_like(r, dtype=bool)
            if mask_d.any():
                np.add.at(deg, dst[mask_d], 1)

    indptr = np.empty((n_nodes + 1,), dtype=np.int64)
    indptr[0] = 0
    np.cumsum(deg, out=indptr[1:])

    E2 = int(indptr[-1])
    print(f'[build_csr] E original={E:,} -> E expanded={E2:,}  rel_vocab={rel_vocab}')

    indices = save_memmap_npy(f'{cfg.out_dir}/csr_indices.i32.npy', shape=(E2,), dtype=np.int32)
    rel = save_memmap_npy(f'{cfg.out_dir}/csr_rel.u32.npy', shape=(E2,), dtype=np.uint32)
    indptr_mm = save_memmap_npy(f'{cfg.out_dir}/csr_indptr.i64.npy', shape=(n_nodes + 1,), dtype=np.int64)
    indptr_mm[:] = indptr[:]
    indptr_mm.flush()

    # Pass 2: fill CSR
    cursor = indptr.copy()

    def add_edge(u_arr, v_arr, r_arr):
        nonlocal cursor, indices, rel
        # u_arr, v_arr, r_arr are numpy arrays
        for u, v, rr in zip(u_arr, v_arr, r_arr):
            u = int(u); v = int(v); rr = int(rr)
            p = int(cursor[u])
            indices[p] = v
            rel[p] = rr
            cursor[u] += 1

    for s0 in range(0, E, cfg.chunk_edges):
        s1 = min(E, s0 + cfg.chunk_edges)
        src = edge_index[0, s0:s1].astype(np.int64)
        dst = edge_index[1, s0:s1].astype(np.int64)
        r0 = etype[s0:s1].astype(np.int64)

        # original edges
        add_edge(src, dst, r0)

        if undirected:
            mask_u = np.isin(r0, list(undirected))
            if mask_u.any():
                add_edge(dst[mask_u], src[mask_u], r0[mask_u])

        if cfg.add_inverses:
            if undirected:
                mask_d = ~np.isin(r0, list(undirected))
            else:
                mask_d = np.ones_like(r0, dtype=bool)
            if mask_d.any():
                add_edge(dst[mask_d], src[mask_d], r0[mask_d] + n_rels)

        if (s0 // cfg.chunk_edges) % 10 == 0:
            print(f'[build_csr] processed edges {s1:,}/{E:,}')

    indices.flush()
    rel.flush()
    indptr_mm.flush()
    print('[build_csr] done.')


if __name__ == '__main__':
    main()
