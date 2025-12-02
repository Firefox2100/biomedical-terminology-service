import itertools
import networkx as nx
import numpy as np
import pandas as pd
import torch
from torch import nn
import torch.nn.functional as F
from torch_geometric.data import Data
from torch_geometric.nn import GCNConv

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix
from bioterms.database import get_active_vector_db


METHOD_NAME = 'Hybrid Graph Neural Network with Text Embeddings'
DEFAULT_SIMILARITY_THRESHOLD = 0.2


class ConceptGCN(nn.Module):
    """
    GCN-based encoder that refines initial text embeddings using the ontology graph structure.
    """

    def __init__(self,
                 input_dim: int,
                 hidden_dim: int = 256,
                 output_dim: int = 256,
                 num_layers: int = 2,
                 dropout: float = 0.5,
                 ):
        super().__init__()

        if num_layers < 1:
            raise ValueError('num_layers must be at least 1')

        layers = []

        layers.append(GCNConv(input_dim, hidden_dim))

        for _ in range(num_layers - 2):
            layers.append(GCNConv(hidden_dim, hidden_dim))

        if num_layers > 1:
            layers.append(GCNConv(hidden_dim, output_dim))

        self.convs = nn.ModuleList(layers)

        self.dropout = dropout
        self.num_layers = num_layers
        self.input_dim = input_dim
        self.output_dim = output_dim if num_layers > 1 else hidden_dim
        self.hidden_dim = hidden_dim

    def forward(self,
                x: torch.Tensor,
                edge_index: torch.Tensor,
                ) -> torch.Tensor:
        """
        Forward pass of the GCN model.
        :param x: Input node features tensor [N, F]
        :param edge_index: Edge index tensor [2, E]
        :return: Node embeddings tensor [N, D]
        """
        num_layers = len(self.convs)

        for i, conv in enumerate(self.convs):
            x = conv(x, edge_index)

            if i != num_layers - 1:
                x = F.relu(x)
                x = F.dropout(x, p=self.dropout, training=self.training)

        return x


class IdentityGCN(nn.Module):
    """
    Identity GCN that returns input features unchanged.
    
    This is a fallback model used when there are no edges in the graph.
    """
    def forward(self, x, edge_index):
        return x


async def _build_pyg_from_nx(target_graph: nx.DiGraph,
                             prefix: ConceptPrefix,
                             ) -> tuple[Data, list[str]]:
    """
    Build a PyTorch Geometric Data object from a NetworkX graph.
    :param target_graph: The directed graph.
    :param prefix: The vocabulary prefix.
    :return: A tuple containing the PyG Data object and a list of node IDs.
    """
    vector_db = get_active_vector_db()
    vector_dict = await vector_db.get_vectors_for_prefix(prefix)

    nodes = list(target_graph.nodes)
    if not nodes:
        raise ValueError('The target graph has no nodes.')

    first_embedding = next(iter(vector_dict.values()))
    embedding_dim = len(first_embedding)

    kept_nodes: list[str] = []
    emb_list: list[np.ndarray] = []

    for n in nodes:
        embedding = vector_dict.get(n)
        if embedding is None:
            # Stale concept without document, skip this node entirely
            continue

        embedding = np.asarray(embedding, dtype=np.float32)
        if embedding.shape[0] != embedding_dim:
            raise ValueError(
                f'Inconsistent embedding dimension for node {n}: '
                f'expected {embedding_dim}, got {embedding.shape[0]}'
            )

        kept_nodes.append(n)
        emb_list.append(embedding)

    if not kept_nodes:
        raise ValueError('No nodes with embeddings exist for this prefix/graph.')

    X = np.vstack(emb_list).astype(np.float32)  # [N, F]
    node_to_index = {node_id: idx for idx, node_id in enumerate(kept_nodes)}

    edge_list: list[tuple[int, int]] = []
    for u, v in target_graph.edges:
        if u in node_to_index and v in node_to_index:
            iu = node_to_index[u]
            iv = node_to_index[v]
            edge_list.append((iu, iv))
            edge_list.append((iv, iu))

    if not edge_list:
        edge_index = torch.empty((2, 0), dtype=torch.long)
    else:
        edge_index_np = np.array(edge_list, dtype=np.int64).T  # [2, E]
        edge_index = torch.from_numpy(edge_index_np)

    x = torch.from_numpy(X)  # [N, F]
    data = Data(x=x, edge_index=edge_index)

    return data, kept_nodes


def _sample_edge_pairs_for_training(edge_index: torch.Tensor,
                                    num_nodes: int,
                                    num_samples: int = None,
                                    ) -> tuple[torch.Tensor, torch.Tensor]:
    """
    Sample positive and negative edge pairs for training the GNN.
    :param edge_index: Edge index tensor [2, E]
    :param num_nodes: Number of nodes in the graph.
    :param num_samples: Number of samples to draw. If None, use all edges.
    :return: A tuple of tensors (pos_edge_index, neg_edge_index) with shape [K, 2]
    """
    if edge_index.numel() == 0:
        # No edges, fall back to identity-like mapping
        return torch.empty((0, 2), dtype=torch.long), torch.empty((0, 2), dtype=torch.long)

    edges = edge_index.t()  # Shape [E, 2]
    num_edges = edges.size(0)

    if num_samples is None or num_samples > num_edges:
        num_samples = num_edges

    perm = torch.randperm(num_edges)[:num_samples]
    pos_pairs = edges[perm]  # Shape [K, 2]

    neg_pairs = []
    existing = set(map(tuple, edges.tolist()))
    while len(neg_pairs) < num_samples:
        i = torch.randint(0, num_nodes, (1,)).item()
        j = torch.randint(0, num_nodes, (1,)).item()
        if i == j:
            continue
        if (i, j) in existing or (j, i) in existing:
            continue
        neg_pairs.append((i, j))
    neg_pairs = torch.tensor(neg_pairs, dtype=torch.long)

    return pos_pairs, neg_pairs


def _train_gnn_model(data: Data,
                     epochs: int = 100,
                     hidden_dim: int = 256,
                     output_dim: int = 256,
                     learning_rate: float = 1e-3,
                     device: str | torch.device = 'cpu',
                     ) -> ConceptGCN | IdentityGCN:
    """
    Train the GCN model on the given graph data.
    :param data: PyTorch Geometric Data object.
    :param epochs: Number of training epochs.
    :param hidden_dim: The hidden dimension size.
    :param output_dim: The output dimension size.
    :param learning_rate: Learning rate for the optimiser.
    :param device: Device to run the training on. Default is 'cpu'.
    :return: The trained GCN model.
    """
    x: torch.Tensor = data.x.to(device)
    edge_index: torch.Tensor = data.edge_index.to(device)
    num_nodes = x.size(0)
    input_dim = x.size(1)

    model = ConceptGCN(
        input_dim=input_dim,
        hidden_dim=hidden_dim,
        output_dim=output_dim,
        num_layers=2,
        dropout=0.5,
    ).to(device)

    optimiser = torch.optim.Adam(model.parameters(), lr=learning_rate, weight_decay=1e-4)

    pos_pairs, neg_pairs = _sample_edge_pairs_for_training(
        edge_index=edge_index,
        num_nodes=num_nodes,
    )
    pos_pairs = pos_pairs.to(device)
    neg_pairs = neg_pairs.to(device)

    if pos_pairs.numel() == 0:
        return IdentityGCN().to(device)

    for _ in range(epochs):
        model.train()
        optimiser.zero_grad()

        z = model(x, edge_index)  # Shape [N, D]
        z = F.normalize(z, p=2, dim=1)

        def pair_scores(pairs: torch.Tensor) -> torch.Tensor:
            return (z[pairs[:, 0]] * z[pairs[:, 1]]).sum(dim=1)

        pos_score = pair_scores(pos_pairs)
        neg_score = pair_scores(neg_pairs)

        margin = 0.5
        loss = F.relu(margin - pos_score + neg_score).mean()

        loss.backward()
        optimiser.step()

    return model


def _compute_similarity_matrix(embeddings: np.ndarray,
                               nodes: list[str],
                               ) -> pd.DataFrame:
    """
    Compute cosine similarity matrix between node embeddings.
    :param embeddings: np.ndarray: Node embeddings array of shape [N, D]
    :param nodes: list[str]: List of node IDs corresponding to the embeddings.
    :return: A pandas DataFrame with similarity scores.
    """
    sim_matrix = embeddings @ embeddings.T  # Shape [N, N]

    n = len(nodes)
    records = []
    for i, j in itertools.combinations(range(n), 2):
        sim = float(sim_matrix[i, j])
        records.append({
            'concept_from': nodes[i],
            'concept_to': nodes[j],
            'similarity': sim,
        })

    df = pd.DataFrame.from_records(records)
    return df


async def calculate_similarity(target_graph: nx.DiGraph,
                               target_prefix: ConceptPrefix,
                               corpus_graph: nx.DiGraph = None,
                               corpus_prefix: ConceptPrefix = None,
                               annotation_graph: nx.DiGraph = None,
                               ) -> pd.DataFrame:
    """
    Calculate semantic similarity scores between terms in the target graph using
    a GNN that refines text embeddings with graph structure.
    :param target_graph: The directed graph of the target vocabulary.
    :param target_prefix: The prefix of the target vocabulary.
    :param corpus_graph: The directed graph of the corpus vocabulary.
    :param corpus_prefix: The prefix of the corpus vocabulary.
    :param annotation_graph: The directed graph of the annotation between target and corpus.
    :return: A pandas DataFrame with similarity scores.
    """
    data, node_ids = await _build_pyg_from_nx(target_graph, target_prefix)
    device = torch.device(CONFIG.torch_device)

    model = _train_gnn_model(
        data=data,
        epochs=CONFIG.gnn_epochs,
        hidden_dim=CONFIG.gnn_hidden_dim,
        output_dim=CONFIG.gnn_output_dim,
        learning_rate=CONFIG.gnn_learning_rate,
        device=device,
    )

    model.eval()
    with torch.no_grad():
        x = data.x.to(device)
        edge_index = data.edge_index.to(device)
        z = model(x, edge_index)  # Shape [N, D]
        z = F.normalize(z, p=2, dim=1)
        embeddings = z.cpu().numpy()  # Shape [N, D]

    similarity_df = _compute_similarity_matrix(embeddings, node_ids)

    return similarity_df
