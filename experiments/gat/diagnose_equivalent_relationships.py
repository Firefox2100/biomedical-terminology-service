"""
Code to examine if the strictly equivalent relationships selected is actually equivalent.
"""

import json
import random
import numpy as np
import pandas as pd

from config import cfg


EQUIVALENT_TYPES = [
    "exact",
    "RxNorm - Source eq",
    "SNOMED - ATC eq",
    "VA Class to ATC eq",
    "NDFRT to ATC eq",
    "DRG - MS-DRG eq",
    "OncoTree to ICDO eq",
    "RxNorm - NDFRT eq",
    "SNOMED - NDFRT eq",
    "VA Class to NDFRT eq",
    "Chem to Prep eq",
    "Concept same_as to",
    "VAProd - RxNorm eq",
    "SNOMED - RxNorm eq",
    "SNOMED - VA Class eq",
]


def select_baseline_pairs(k: int, total_edges: int) -> set[int]:
    """
    Randomly select k edges to form a baseline for comparison.
    :param k: The number of edges to select.
    :param total_edges: The total number of edges in the graph.
    :return: A set of edge IDs representing the baseline.
    """
    return set(random.sample(range(total_edges), k))


def calculate_cosine_similarity(vec1: np.ndarray, vec2: np.ndarray) -> float:
    """
    Calculate the cosine similarity between two vectors.
    :param vec1: The first vector.
    :param vec2: The second vector.
    :return: The cosine similarity score.
    """
    dot_product = np.dot(vec1, vec2)
    norm_vec1 = np.linalg.norm(vec1)
    norm_vec2 = np.linalg.norm(vec2)

    if norm_vec1 == 0 or norm_vec2 == 0:
        return 0.0  # Avoid division by zero

    return dot_product / (norm_vec1 * norm_vec2)


def diagnose_equivalent_relationships():
    meta = json.loads((cfg.out_dir / 'meta.json').read_text())
    total_edges = meta['num_edges']

    baseline_edge_ids = select_baseline_pairs(k=5000, total_edges=total_edges)
    edge = np.load(meta['edge_index_path'], mmap_mode='r')
    etype = np.load(meta['etype_path'], mmap_mode='r')
    embedding = np.load(meta['x_path'], mmap_mode='r')

    equivalent_edge_map = {}
    for et in EQUIVALENT_TYPES:
        etype_id = meta['rel_to_id'][et]
        equivalent_edge_map[et] = etype_id

    edge_count = {}
    equivalent_pairs: set[tuple[int, int]] = set()
    equivalent_nodes = set()

    for i in range(total_edges):
        # Check the type of the edge
        if etype[i] in equivalent_edge_map.values():
            src, dst = edge[:, i]
            equivalent_pairs.add((src, dst))
            edge_count[etype[i]] = edge_count.get(etype[i], 0) + 1
            equivalent_nodes.add(src)
            equivalent_nodes.add(dst)

            if i in baseline_edge_ids:
                # This edge is part of the baseline, re-select another edge for baseline
                new_edge_id = random.choice(range(total_edges))
                while new_edge_id in baseline_edge_ids:
                    new_edge_id = random.choice(range(total_edges))
                baseline_edge_ids.add(new_edge_id)
                baseline_edge_ids.remove(i)

    print("Equivalent edge counts by type:")
    for etype_id, count in edge_count.items():
        etype_name = [k for k, v in equivalent_edge_map.items() if v == etype_id][0]
        print(f"{etype_name}: {count}")

    print(f'{len(equivalent_nodes)} equivalent nodes')

    # Cosine similarity analysis
    selected_equivalent_pairs = random.sample(list(equivalent_pairs), min(5000, len(equivalent_pairs)))
    baseline_pais = [edge[:, i] for i in baseline_edge_ids]

    # Get average cosine similarity for equivalent pairs
    equivalent_similarities = []
    for src, dst in selected_equivalent_pairs:
        vec_src = embedding[src]
        vec_dst = embedding[dst]
        similarity = calculate_cosine_similarity(vec_src, vec_dst)
        equivalent_similarities.append(similarity)

    # Get average cosine similarity for baseline pairs
    baseline_similarities = []
    for edge_id in baseline_edge_ids:
        src, dst = edge[:, edge_id]
        vec_src = embedding[src]
        vec_dst = embedding[dst]
        similarity = calculate_cosine_similarity(vec_src, vec_dst)
        baseline_similarities.append(similarity)

    print(f"Average cosine similarity for equivalent pairs: {np.mean(equivalent_similarities):.4f}")
    print(f"Average cosine similarity for baseline pairs: {np.mean(baseline_similarities):.4f}")


if __name__ == '__main__':
    diagnose_equivalent_relationships()
