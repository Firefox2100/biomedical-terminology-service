import argparse
import os
import numpy as np
import torch

from utils_io import ensure_dir, save_memmap_npy
from config import cfg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ckpt", type=str, required=True)
    ap.add_argument("--out_dir", type=str, required=True)
    ap.add_argument("--dtype", type=str, default="float16", choices=["float16", "float32"])
    ap.add_argument("--chunk", type=int, default=1_000_000, help="Rows per chunk to write")
    args = ap.parse_args()

    ensure_dir(args.out_dir)

    ckpt = torch.load(args.ckpt, map_location="cpu")
    n_nodes = int(ckpt["n_nodes"])
    dim = int(ckpt["dim"])
    vocab_size = int(ckpt["vocab_size"])

    state = ckpt["emb_state"]
    weight = state["weight"]  # (vocab_size, dim)

    assert weight.shape[0] == vocab_size
    assert weight.shape[1] == dim
    assert n_nodes <= vocab_size

    out_path = os.path.join(args.out_dir, f"node_embeddings.{ 'f16' if args.dtype=='float16' else 'f32' }.npy")
    out_dtype = np.float16 if args.dtype == "float16" else np.float32
    mm = save_memmap_npy(out_path, shape=(n_nodes, dim), dtype=out_dtype)

    # Write in chunks
    w = weight[:n_nodes].detach().cpu()
    for s0 in range(0, n_nodes, args.chunk):
        s1 = min(n_nodes, s0 + args.chunk)
        mm[s0:s1] = w[s0:s1].numpy().astype(out_dtype, copy=False)
        mm.flush()
        print(f"[export] wrote {s1:,}/{n_nodes:,}")

    mm.flush()
    print(f"[export] done: {out_path}")


if __name__ == "__main__":
    main()
