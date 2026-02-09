import json
import numpy as np
import torch
import torch.nn.functional as F
from pathlib import Path
from torch_geometric.data import Data
from torch_geometric.loader import NeighborLoader
from torch_geometric.nn import GATv2Conv


META = Path("graph/meta.json")


class TinyGAT(torch.nn.Module):
    def __init__(self, in_dim=768, hid=64, out=64, heads=2):
        super().__init__()
        self.c1 = GATv2Conv(in_dim, hid, heads=heads, dropout=0.1, add_self_loops=True)
        self.c2 = GATv2Conv(hid*heads, out, heads=1, dropout=0.1, add_self_loops=True)

    def forward(self, x, edge_index):
        x = F.elu(self.c1(x, edge_index))
        x = self.c2(x, edge_index)
        return x


def neg_sample(src, num_nodes):
    dst = torch.randint(0, num_nodes, (src.size(0),), device=src.device)
    return torch.stack([src, dst], dim=0)


def main():
    meta = json.loads(META.read_text())
    N = meta["num_nodes"]
    E = meta["num_edges"]

    x = torch.from_numpy(np.load(meta["x_path"], mmap_mode="r"))  # memmap-backed
    edge_index = torch.from_numpy(np.load(meta["edge_index_path"], mmap_mode="r"))  # memmap-backed

    data = Data(x=x, edge_index=edge_index)
    data.num_nodes = N

    # Take a small random set of seed nodes
    rng = np.random.default_rng(0)
    seed = torch.from_numpy(rng.integers(0, N, size=4096, dtype=np.int64))

    loader = NeighborLoader(
        data,
        input_nodes=seed,
        num_neighbors=[10, 10],
        batch_size=512,
        shuffle=True,
        num_workers=0,  # keep 0 for smoke test
    )

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = TinyGAT(in_dim=x.size(1)).to(device)
    opt = torch.optim.AdamW(model.parameters(), lr=1e-3)

    model.train()
    steps = 0
    for batch in loader:
        batch = batch.to(device)
        opt.zero_grad()

        batch.x = batch.x.float()
        z = model(batch.x, batch.edge_index)

        pos = batch.edge_index
        neg = neg_sample(pos[0], batch.num_nodes)

        pos_score = (z[pos[0]] * z[pos[1]]).sum(dim=-1)
        neg_score = (z[neg[0]] * z[neg[1]]).sum(dim=-1)

        loss = F.binary_cross_entropy_with_logits(pos_score, torch.ones_like(pos_score)) + \
               F.binary_cross_entropy_with_logits(neg_score, torch.zeros_like(neg_score))

        loss.backward()
        opt.step()

        steps += 1
        print(f"step={steps} loss={loss.item():.4f} nodes={batch.num_nodes} edges={batch.edge_index.size(1)}")

        if steps >= 10:
            break

    print("OK: smoke training run completed.")


if __name__ == "__main__":
    main()
