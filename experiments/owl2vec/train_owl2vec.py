import os
import time
import json
from dataclasses import dataclass
import numpy as np
import torch
from torch.utils.data import IterableDataset, DataLoader

from utils_io import open_memmap_npy, ensure_dir
from utils_walks import CSRGraph, random_walk_interleaved, iter_skipgram_pairs
from config import cfg


@dataclass(frozen=True)
class TrainCfg:
    dim: int = 128
    walk_len_nodes: int = 40
    window: int = 5
    neg_k: int = 10
    batch_size: int = 8192
    num_workers: int = 8
    steps: int = 200_000
    lr: float = 0.05
    seed: int = 42
    log_every: int = 200
    ckpt_every: int = 5_000
    fp16: bool = True


class WalkPairStream(IterableDataset):
    """
    Streams skip-gram pairs generated from random walks.
    Each sample yields (center_token, context_token).
    """
    def __init__(
        self,
        indptr_path: str,
        indices_path: str,
        rel_path: str,
        n_nodes: int,
        n_rel: int,
        walk_len_nodes: int,
        window: int,
        rel_token_offset: int,
        seed: int = 1234,
    ):
        super().__init__()
        self.indptr_path = indptr_path
        self.indices_path = indices_path
        self.rel_path = rel_path
        self.n_nodes = n_nodes
        self.n_rel = n_rel
        self.walk_len_nodes = walk_len_nodes
        self.window = window
        self.rel_token_offset = rel_token_offset
        self.seed = seed

    def __iter__(self):
        worker_info = torch.utils.data.get_worker_info()
        if worker_info is None:
            wid = 0
        else:
            wid = worker_info.id

        # Per-worker RNG
        rng = np.random.default_rng(self.seed + 10_000 * wid)

        indptr = open_memmap_npy(self.indptr_path, mode="r")
        indices = open_memmap_npy(self.indices_path, mode="r")
        rel = open_memmap_npy(self.rel_path, mode="r")

        g = CSRGraph(indptr=indptr, indices=indices, rel=rel, n_nodes=self.n_nodes, n_rel=self.n_rel)

        # Stride starts across workers to reduce overlap (still random, but helps)
        while True:
            # Sample start node uniformly. You can bias later if you want.
            start = int(rng.integers(0, self.n_nodes))
            tokens = random_walk_interleaved(
                g=g,
                start=start,
                walk_len_nodes=self.walk_len_nodes,
                rng=rng,
                rel_token_offset=self.rel_token_offset,
            )
            for c, o in iter_skipgram_pairs(tokens, self.window):
                yield c, o


tcfg = TrainCfg()


def sgns_loss(emb: torch.Tensor, centers: torch.Tensor, contexts: torch.Tensor, negs: torch.Tensor) -> torch.Tensor:
    """
    emb: (V, D) embedding matrix
    centers: (B,) token ids
    contexts: (B,) token ids
    negs: (B, K) token ids
    """
    v_c = emb[centers]          # (B, D)
    v_o = emb[contexts]         # (B, D)
    pos_logits = (v_c * v_o).sum(dim=1)  # (B,)
    pos_loss = torch.nn.functional.binary_cross_entropy_with_logits(
        pos_logits, torch.ones_like(pos_logits), reduction='mean'
    )

    v_n = emb[negs]             # (B, K, D)
    neg_logits = (v_c.unsqueeze(1) * v_n).sum(dim=2)  # (B, K)
    neg_loss = torch.nn.functional.binary_cross_entropy_with_logits(
        neg_logits, torch.zeros_like(neg_logits), reduction='mean'
    )

    return pos_loss + neg_loss


def main():
    ensure_dir(str(cfg.checkpoint_dir))
    meta_path = cfg.gat_path / 'graph' / 'meta.json'
    meta = json.loads(meta_path.read_text())

    indptr_path = str(cfg.out_dir / 'csr_indptr.i64.npy')
    indices_path = str(cfg.out_dir / 'csr_indices.i32.npy')
    rel_path = str(cfg.out_dir / 'csr_rel.u32.npy')

    n_rel = (2 * meta['num_rels']) if cfg.add_inverses else meta['num_rels']
    rel_token_offset = meta['num_nodes']
    vocab_size = meta['num_nodes'] + n_rel

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    torch.manual_seed(tcfg.seed)
    np.random.seed(tcfg.seed)

    # Dataset/Dataloader streaming pairs
    ds = WalkPairStream(
        indptr_path=indptr_path,
        indices_path=indices_path,
        rel_path=rel_path,
        n_nodes=meta['num_nodes'],
        n_rel=n_rel,
        walk_len_nodes=tcfg.walk_len_nodes,
        window=tcfg.window,
        rel_token_offset=rel_token_offset,
        seed=tcfg.seed,
    )
    dl = DataLoader(
        ds,
        batch_size=tcfg.batch_size,
        num_workers=tcfg.num_workers,
        pin_memory=True,
        persistent_workers=(tcfg.num_workers > 0),
        prefetch_factor=2 if tcfg.num_workers > 0 else None,
    )

    # Embedding matrix
    emb_dtype = torch.float16 if tcfg.fp16 else torch.float32
    emb = torch.nn.Embedding(vocab_size, tcfg.dim, sparse=False, device=device, dtype=emb_dtype)
    torch.nn.init.uniform_(emb.weight, a=-0.5 / tcfg.dim, b=0.5 / tcfg.dim)

    # Optimizer: SGD (no state -> memory friendly)
    opt = torch.optim.SGD(emb.parameters(), lr=tcfg.lr)

    # Negative sampler on GPU (uniform)
    # If you later want unigram-based negatives, we can add it, but uniform scales best.
    def sample_negs(batch_size: int, k: int) -> torch.Tensor:
        # Avoid generating on CPU; do it on GPU
        return torch.randint(low=0, high=vocab_size, size=(batch_size, k), device=device, dtype=torch.int64)

    step = 0
    t0 = time.time()
    it = iter(dl)

    while step < tcfg.steps:
        centers, contexts = next(it)
        centers = centers.to(device, non_blocking=True, dtype=torch.int64)
        contexts = contexts.to(device, non_blocking=True, dtype=torch.int64)
        negs = sample_negs(centers.shape[0], tcfg.neg_k)

        opt.zero_grad(set_to_none=True)

        # Mixed precision here is optional; dot products are stable enough in fp16 typically,
        # but if you see instability, disable --fp16 or add autocast.
        loss = sgns_loss(emb.weight, centers, contexts, negs)
        loss.backward()
        opt.step()

        step += 1

        if step % tcfg.log_every == 0:
            dt = time.time() - t0
            pairs_per_s = (tcfg.batch_size * tcfg.log_every) / max(dt, 1e-9)
            print(f"[train] step={step:,} loss={float(loss):.4f} pairs/s={pairs_per_s:,.0f}")
            t0 = time.time()

        if step % tcfg.ckpt_every == 0:
            ckpt_path = os.path.join(cfg.checkpoint_dir, f"ckpt_step_{step}.pt")
            torch.save(
                {
                    "step": step,
                    "vocab_size": vocab_size,
                    "n_nodes": meta['num_nodes'],
                    "n_rel": n_rel,
                    "dim": tcfg.dim,
                    "add_inverses": cfg.add_inverses,
                    "emb_state": emb.state_dict(),
                },
                ckpt_path,
            )
            print(f"[train] checkpoint saved: {ckpt_path}")

    final_path = os.path.join(cfg.checkpoint_dir, "ckpt_final.pt")
    torch.save(
        {
            "step": step,
            "vocab_size": vocab_size,
            "n_nodes": meta['num_nodes'],
            "n_rel": n_rel,
            "dim": tcfg.dim,
            "add_inverses": cfg.add_inverses,
            "emb_state": emb.state_dict(),
        },
        final_path,
    )
    print(f"[train] done. final checkpoint: {final_path}")


if __name__ == "__main__":
    main()
