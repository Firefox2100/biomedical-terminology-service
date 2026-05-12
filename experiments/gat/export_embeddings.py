import json
from pathlib import Path

import numpy as np
import torch
from torch_geometric.data import Data
from torch_geometric.loader import NeighborLoader

from config import cfg
from train_gat import GATEncoder


def main():
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print("device:", device)

    meta = json.loads((cfg.out_dir / "meta.json").read_text())
    N = int(meta["num_nodes"])
    E = int(meta["num_edges"])

    x_path = Path(meta["x_path"])
    edge_path = Path(meta["edge_index_path"])

    # Load graph (memmap-backed, no RAM copy)
    x_np = np.load(x_path, mmap_mode="r")        # (N, D_in) float16
    e_np = np.load(edge_path, mmap_mode="r")     # (2, E) int64

    assert x_np.shape[0] == N, (x_np.shape, N)
    assert e_np.shape == (2, E), (e_np.shape, (2, E))

    x = torch.from_numpy(x_np)          # float16 memmap tensor
    edge_index = torch.from_numpy(e_np) # int64 memmap tensor

    data = Data(x=x, edge_index=edge_index)
    data.num_nodes = N

    # Load checkpoint
    ckpt_path = cfg.checkpoints_dir / "final.pt"
    if not ckpt_path.exists():
        ckpt_path = cfg.checkpoints_dir / "latest.pt"
    ckpt = torch.load(ckpt_path, map_location="cpu")

    train_cfg = ckpt["train_cfg"]
    in_dim = int(x_np.shape[1])

    # Rebuild model with exact training hyperparams
    model = GATEncoder(
        in_dim=in_dim,
        proj_dim=int(train_cfg["proj_dim"]),
        hidden_dim=int(train_cfg["hidden_dim"]),
        out_dim=int(train_cfg["out_dim"]),
        heads=int(train_cfg["heads"]),
        layers=int(train_cfg["layers"]),
        dropout=float(train_cfg["dropout"]),
    ).to(device)

    model.load_state_dict(ckpt["model"])
    model.eval()

    D_out = int(train_cfg["out_dim"])
    out_path = cfg.out_dir / f"concept_emb.gat.out{D_out}.f16.npy"

    # Create output .npy with header (real npy, stream-write)
    out = np.lib.format.open_memmap(
        out_path,
        mode="w+",
        dtype=np.float16,
        shape=(N, D_out),
    )

    # Inference loader over all nodes
    # Use same depth as training (len(num_neighbors) == layers) or bigger if you want.
    # Keeping it equal to training is usually the most stable.
    num_neighbors = list(train_cfg["num_neighbors"]) if "num_neighbors" in train_cfg else [15, 10]

    # batch_size: tune based on GPU memory
    batch_size = int(train_cfg.get("batch_size", 2048))
    num_workers = int(train_cfg.get("num_workers", 8))

    all_nodes = torch.arange(N, dtype=torch.int64)

    loader = NeighborLoader(
        data,
        input_nodes=all_nodes,
        num_neighbors=num_neighbors,
        batch_size=batch_size,
        shuffle=False,
        num_workers=num_workers,
        persistent_workers=(num_workers > 0),
    )

    use_amp = bool(train_cfg.get("amp", True) and device.type == "cuda")

    written = 0
    with torch.no_grad():
        for step, batch in enumerate(loader, start=1):
            batch = batch.to(device, non_blocking=True)

            # IMPORTANT: x is float16 from disk. Cast to float32 (same as training).
            batch.x = batch.x.float()

            with torch.cuda.amp.autocast(enabled=use_amp):
                z = model(batch.x, batch.edge_index)

            # Only the first batch.batch_size rows correspond to the seed nodes
            seed_global = batch.n_id[: batch.batch_size].detach().cpu().numpy()
            z_seed = z[: batch.batch_size].detach().cpu().to(torch.float16).numpy()

            out[seed_global] = z_seed
            written += seed_global.shape[0]

            if step % 200 == 0:
                out.flush()
                print(f"step={step} written={written:,}/{N:,}")

    out.flush()
    print(f"Wrote embeddings: {out_path}  shape=({N},{D_out}) dtype=float16  ckpt={ckpt_path}")


if __name__ == "__main__":
    main()
