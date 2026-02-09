import json
import os
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import torch
import torch.distributed as dist
from torch.nn import functional as F
from torch.nn.parallel import DistributedDataParallel as DDP

from torch_geometric.data import Data
from torch_geometric.loader import NeighborLoader
from torch_geometric.nn import GATv2Conv

from config import cfg


@dataclass
class TrainCfg:
    # Model
    in_dim: int = 768
    proj_dim: int = 256          # reduce 768 -> proj_dim before GAT (much faster)
    hidden_dim: int = 128
    out_dim: int = 128
    heads: int = 4
    layers: int = 2
    dropout: float = 0.1

    # Sampling (per GNN layer)
    num_neighbors: tuple[int, ...] = (15, 10)
    batch_size: int = 2048
    num_workers: int = 8

    # Optimisation
    lr: float = 1e-3
    weight_decay: float = 1e-5
    epochs: int = 5
    amp: bool = True

    # Checkpointing
    save_latest_every_epochs: int = 1  # overwrite latest.pt
    save_final: bool = True


tcfg = TrainCfg()


class GATEncoder(torch.nn.Module):
    """
    Projection + GAT stack.
    Note: batch.x is float16 from disk; we cast to float32 on GPU for stability,
    then projection reduces compute cost, then AMP can speed up.
    """
    def __init__(self, in_dim: int, proj_dim: int, hidden_dim: int, out_dim: int, heads: int, layers: int, dropout: float):
        super().__init__()
        assert layers >= 2

        self.dropout = dropout
        self.proj = torch.nn.Linear(in_dim, proj_dim, bias=False)

        self.convs = torch.nn.ModuleList()
        self.convs.append(GATv2Conv(proj_dim, hidden_dim, heads=heads, dropout=dropout, add_self_loops=True))

        for _ in range(layers - 2):
            self.convs.append(GATv2Conv(hidden_dim * heads, hidden_dim, heads=heads, dropout=dropout, add_self_loops=True))

        self.convs.append(GATv2Conv(hidden_dim * heads, out_dim, heads=1, dropout=dropout, add_self_loops=True))

    def forward(self, x, edge_index):
        # x expected float32 on GPU here
        x = self.proj(x)
        for i, conv in enumerate(self.convs):
            x = conv(x, edge_index)
            if i != len(self.convs) - 1:
                x = F.elu(x)
                x = F.dropout(x, p=self.dropout, training=self.training)
        return x


def init_distributed() -> bool:
    # torchrun sets RANK/WORLD_SIZE/LOCAL_RANK
    if "RANK" in os.environ and "WORLD_SIZE" in os.environ:
        dist.init_process_group(backend="nccl")
        torch.cuda.set_device(int(os.environ["LOCAL_RANK"]))
        return True
    return False


def is_main() -> bool:
    return (not dist.is_initialized()) or dist.get_rank() == 0


def load_graph():
    """
    Uses your current meta.json fields:
      - x_path
      - edge_index_path
      - num_nodes
      - num_edges
    """
    meta = json.loads((cfg.out_dir / "meta.json").read_text())

    N = int(meta["num_nodes"])
    E = int(meta["num_edges"])

    x_path = Path(meta["x_path"])
    edge_path = Path(meta["edge_index_path"])

    # Must be true .npy (open_memmap) => np.load(mmap_mode="r") is correct.
    x_np = np.load(x_path, mmap_mode="r")          # shape (N, D_in), float16
    e_np = np.load(edge_path, mmap_mode="r")       # shape (2, E), int64

    # Zero-copy views backed by memmap:
    x = torch.from_numpy(x_np)
    edge_index = torch.from_numpy(e_np)

    data = Data(x=x, edge_index=edge_index)
    data.num_nodes = N
    return data, meta


def sample_negative_edges(src: torch.Tensor, num_nodes: int) -> torch.Tensor:
    # Uniform negatives; for better training later, you can add hard negatives.
    dst = torch.randint(0, num_nodes, (src.size(0),), device=src.device, dtype=src.dtype)
    return torch.stack([src, dst], dim=0)


def save_checkpoint(path: Path, state: dict):
    # Minimal: overwrite single file; no extra copies.
    # Write directly to target path (simple). If you want atomic semantics, write to temp then replace.
    torch.save(state, path)


def main():
    distributed = init_distributed()
    rank = dist.get_rank() if distributed else 0
    world = dist.get_world_size() if distributed else 1
    local_rank = int(os.environ.get("LOCAL_RANK", "0"))

    cfg.checkpoints_dir.mkdir(parents=True, exist_ok=True)
    latest_path = cfg.checkpoints_dir / "latest.pt"
    final_path = cfg.checkpoints_dir / "final.pt"

    data, meta = load_graph()
    N = int(data.num_nodes)
    D_in = int(data.x.size(1))
    if D_in != tcfg.in_dim and is_main():
        print(f"[warn] x dim is {D_in}, but TrainCfg.in_dim is {tcfg.in_dim}. Using {D_in}.")
        tcfg.in_dim = D_in

    device = torch.device("cuda", local_rank) if torch.cuda.is_available() else torch.device("cpu")
    use_amp = bool(tcfg.amp and device.type == "cuda")

    model = GATEncoder(
        in_dim=tcfg.in_dim,
        proj_dim=tcfg.proj_dim,
        hidden_dim=tcfg.hidden_dim,
        out_dim=tcfg.out_dim,
        heads=tcfg.heads,
        layers=tcfg.layers,
        dropout=tcfg.dropout,
    ).to(device)

    if distributed:
        model = DDP(model, device_ids=[local_rank], output_device=local_rank)

    opt = torch.optim.AdamW(model.parameters(), lr=tcfg.lr, weight_decay=tcfg.weight_decay)
    scaler = torch.cuda.amp.GradScaler(enabled=use_amp)

    # Seed nodes sharded across ranks for DDP
    all_nodes = torch.arange(N, dtype=torch.int64)
    train_nodes = all_nodes[rank::world]

    loader = NeighborLoader(
        data,
        input_nodes=train_nodes,
        num_neighbors=list(tcfg.num_neighbors),
        batch_size=tcfg.batch_size,
        shuffle=True,
        num_workers=tcfg.num_workers,
        persistent_workers=(tcfg.num_workers > 0),
    )

    start_epoch = 0
    if latest_path.exists():
        ckpt = torch.load(latest_path, map_location="cpu")
        (model.module if distributed else model).load_state_dict(ckpt["model"])
        opt.load_state_dict(ckpt["opt"])
        scaler.load_state_dict(ckpt["scaler"])
        start_epoch = int(ckpt["epoch"]) + 1
        if is_main():
            print(f"Resumed from epoch {start_epoch}")

    for epoch in range(start_epoch, tcfg.epochs):
        model.train()
        total_loss = 0.0
        steps = 0

        for batch in loader:
            batch = batch.to(device, non_blocking=True)

            # Fix dtype mismatch: x from disk is float16; cast to float32 for linear/proj stability.
            # This also prevents "Half vs Float" matmul errors.
            batch.x = batch.x.float()

            opt.zero_grad(set_to_none=True)

            with torch.cuda.amp.autocast(enabled=use_amp):
                z = model(batch.x, batch.edge_index)

                # Positive edges inside sampled subgraph.
                pos_edge = batch.edge_index

                # Negatives: same number of edges, random dst
                neg = sample_negative_edges(pos_edge[0], batch.num_nodes)

                pos_score = (z[pos_edge[0]] * z[pos_edge[1]]).sum(dim=-1)
                neg_score = (z[neg[0]] * z[neg[1]]).sum(dim=-1)

                loss = (
                    F.binary_cross_entropy_with_logits(pos_score, torch.ones_like(pos_score)) +
                    F.binary_cross_entropy_with_logits(neg_score, torch.zeros_like(neg_score))
                )

            scaler.scale(loss).backward()
            scaler.step(opt)
            scaler.update()

            total_loss += float(loss.detach().cpu())
            steps += 1

        if distributed:
            dist.barrier()

        avg = total_loss / max(1, steps)
        if is_main():
            print(f"epoch={epoch} loss={avg:.4f}")

        # Minimal checkpoint: overwrite single file
        if is_main() and (epoch + 1) % tcfg.save_latest_every_epochs == 0:
            state = {
                "epoch": epoch,
                "model": (model.module if distributed else model).state_dict(),
                "opt": opt.state_dict(),
                "scaler": scaler.state_dict(),
                "train_cfg": tcfg.__dict__,
                "meta": meta,
            }
            save_checkpoint(latest_path, state)

    # Final checkpoint (written once)
    if is_main() and tcfg.save_final:
        final_state = {
            "epoch": tcfg.epochs - 1,
            "model": (model.module if distributed else model).state_dict(),
            "train_cfg": tcfg.__dict__,
            "meta": meta,
        }
        save_checkpoint(final_path, final_state)
        print(f"Saved final checkpoint: {final_path}")

    if distributed:
        dist.destroy_process_group()


if __name__ == "__main__":
    main()
