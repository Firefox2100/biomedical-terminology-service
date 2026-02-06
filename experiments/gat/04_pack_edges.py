import json
import numpy as np
from config import cfg


def pack_edges():
    meta = json.loads((cfg.out_dir / 'meta.json').read_text())
    edge_dir = cfg.out_dir / 'edges_chunks'

    edge_files = sorted(edge_dir.glob('edge_index_*.npy'))
    etype_files = sorted(edge_dir.glob('etype_*.npy'))
    assert len(edge_files) == len(etype_files)

    total = 0
    sizes = []

    for f in edge_files:
        e = np.load(f, mmap_mode='r')
        sizes.append(e.shape[1])
        total += e.shape[1]

    E = total
    out_edge = cfg.out_dir / 'edge_index.i64.npy'
    out_type = cfg.out_dir / 'etype.u16.npy'

    edge_index = np.lib.format.open_memmap(out_edge, mode='w+', dtype=np.int64, shape=(2, E))
    etype = np.lib.format.open_memmap(out_type, mode='w+', dtype=np.uint16, shape=(E,))

    pos = 0
    for edge_file, etype_file, size in zip(edge_files, etype_files, sizes):
        e = np.load(edge_file, mmap_mode='r')
        t = np.load(etype_file, mmap_mode='r')

        edge_index[:, pos:pos+size] = e
        etype[pos:pos+size] = t

        pos += size
        if pos % 10_000_000 == 0:
            edge_index.flush()
            etype.flush()
            print(f'Packed {pos} edges so far...')

    edge_index.flush()
    etype.flush()
    print(f'Packed total {total} edges to {out_edge} and {out_type}')

    meta['edge_index_path'] = str(out_edge)
    meta['etype_path'] = str(out_type)
    (cfg.out_dir / 'meta.json').write_text(json.dumps(meta, indent=2))
    print(f'Finished. Packed {total} edges.')


if __name__ == '__main__':
    pack_edges()
