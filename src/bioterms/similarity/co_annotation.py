import math
from copy import deepcopy
from typing import AsyncIterator, Iterator

import networkx as nx
import numpy as np
from numba import njit, prange, set_num_threads, get_num_threads
from pyroaring import BitMap

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType
from bioterms.etc.utils import verbose_print
from .utils import count_annotation_for_graph, filter_edges_by_relationship

METHOD_NAME = 'Co-Annotation Vector Method'
DEFAULT_SIMILARITY_THRESHOLD = 0.2
CORPUS_REQUIRED = True
CORPUS_GRAPH_REQUIRED = False

_CPU_PAIR_BATCH_SIZE = 1 << 20
_GPU_PAIR_BATCH_SIZE = 1 << 22

try:
    import cupy as cp
except ImportError:
    cp = None


@njit(cache=True, nogil=True, parallel=True)
def _score_pairs_cpu(row_ptr, annotation_ids, lhs, rhs, total_annotation_count, threshold):
    output = np.empty(lhs.shape[0], dtype=np.float64)
    for k in prange(lhs.shape[0]):
        arow, brow = lhs[k], rhs[k]
        p1, e1 = row_ptr[arow], row_ptr[arow + 1]
        p2, e2 = row_ptr[brow], row_ptr[brow + 1]
        len_1, len_2 = e1 - p1, e2 - p2
        inter = 0
        while p1 < e1 and p2 < e2:
            a, b = annotation_ids[p1], annotation_ids[p2]
            if a < b:
                p1 += 1
            elif a > b:
                p2 += 1
            else:
                inter += 1
                p1 += 1
                p2 += 1
        if inter == 0:
            output[k] = np.nan
            continue
        union = len_1 + len_2 - inter
        jaccard = inter / union
        if jaccard < threshold:
            output[k] = np.nan
            continue
        if inter == total_annotation_count:
            npmi = 1.0
        else:
            numerator = (inter * total_annotation_count) / (len_1 * len_2)
            if numerator <= 0.0:
                output[k] = np.nan
                continue
            denom = math.log(total_annotation_count / inter)
            npmi = 1.0 if math.isclose(denom, 0.0) else (1.0 + math.log(numerator) / denom) / 2.0
        similarity = npmi * jaccard
        output[k] = similarity if similarity >= threshold and similarity >= 0.0 else np.nan
    return output


_CUDA_KERNEL = r'''
extern "C" __global__
void score(const long long* row_ptr, const unsigned int* ids,
           const int* lhs, const int* rhs, const long long total,
           const double threshold, double* out, const long long n) {
    long long k = (long long)blockDim.x * blockIdx.x + threadIdx.x;
    if (k >= n) return;
    int arow = lhs[k], brow = rhs[k];
    long long p1=row_ptr[arow], e1=row_ptr[arow+1], p2=row_ptr[brow], e2=row_ptr[brow+1];
    long long l1=e1-p1, l2=e2-p2, inter=0;
    while (p1<e1 && p2<e2) {
        unsigned int a=ids[p1], b=ids[p2];
        if (a<b) ++p1; else if (a>b) ++p2; else { ++inter; ++p1; ++p2; }
    }
    if (!inter) { out[k]=NAN; return; }
    long long uni=l1+l2-inter;
    double j=(double)inter/(double)uni;
    if (j < threshold) { out[k]=NAN; return; }
    double npmi;
    if (inter == total) npmi=1.0;
    else {
        double numerator=((double)inter*(double)total)/((double)l1*(double)l2);
        if (!(numerator>0.0) || !isfinite(numerator)) { out[k]=NAN; return; }
        double denom=log((double)total/(double)inter);
        npmi = fabs(denom) <= 1e-12 ? 1.0 : (1.0 + log(numerator)/denom)/2.0;
    }
    double s=npmi*j;
    out[k]=(s>=threshold && s>=0.0) ? s : NAN;
}
'''


class _CudaScorer:
    def __init__(self, row_ptr, annotation_ids, total_annotation_count, threshold):
        self.row_ptr = cp.asarray(row_ptr)
        self.annotation_ids = cp.asarray(annotation_ids)
        self.total = total_annotation_count
        self.threshold = threshold
        self.kernel = cp.RawKernel(_CUDA_KERNEL, 'score')

    def score(self, lhs, rhs):
        gl, gr = cp.asarray(lhs), cp.asarray(rhs)
        out = cp.empty(lhs.shape[0], dtype=cp.float64)
        threads = 256
        blocks = (lhs.shape[0] + threads - 1) // threads
        self.kernel((blocks,), (threads,), (self.row_ptr, self.annotation_ids, gl, gr,
                    np.int64(self.total), np.float64(self.threshold), out, np.int64(lhs.shape[0])))
        return cp.asnumpy(out)


def _cuda_available():
    if cp is None:
        return False
    try:
        return cp.cuda.runtime.getDeviceCount() > 0
    except Exception:
        return False


def _fits_cuda(row_ptr, annotation_ids, batch_size):
    if not _cuda_available():
        return False
    try:
        free, _ = cp.cuda.runtime.memGetInfo()
    except Exception:
        return False
    required = row_ptr.nbytes + annotation_ids.nbytes + batch_size * 16
    return required <= int(free * 0.8)


def _build_annotation_sets(target_graph, target_prefix, corpus_prefix, annotation_graph, nodes):
    if not nx.is_directed_acyclic_graph(target_graph):
        raise ValueError('Filtered target ontology must be a DAG.')
    node_to_index = {n: i for i, n in enumerate(nodes)}
    corpus_prefix_string = f'{corpus_prefix.value}:'
    corpus_to_index = {
        n: i for i, n in enumerate(
            n for n in annotation_graph.nodes
            if isinstance(n, str) and n.startswith(corpus_prefix_string)
        )
    }
    if len(corpus_to_index) >= (1 << 32):
        raise ValueError('Too many corpus concepts for uint32 IDs.')
    sets = [BitMap() for _ in nodes]
    for i, node in enumerate(nodes):
        ann_node = f'{target_prefix.value}:{node}'
        if ann_node not in annotation_graph:
            continue
        for neighbour in annotation_graph.neighbors(ann_node):
            idx = corpus_to_index.get(neighbour)
            if idx is not None:
                sets[i].add(idx)
    # Existing implementation used nx.ancestors(node): predecessors propagate to successors.
    for source in nx.topological_sort(target_graph):
        src = sets[node_to_index[source]]
        if not src:
            continue
        for target in target_graph.successors(source):
            sets[node_to_index[target]] |= src
    return sets, len(corpus_to_index)


def _bitmaps_to_csr(bitmaps):
    ptr = np.empty(len(bitmaps) + 1, dtype=np.int64)
    sizes = np.empty(len(bitmaps), dtype=np.int64)
    ptr[0] = 0
    for i, bm in enumerate(bitmaps):
        sizes[i] = len(bm)
        ptr[i + 1] = ptr[i] + sizes[i]
    ids = np.empty(int(ptr[-1]), dtype=np.uint32)
    for i, bm in enumerate(bitmaps):
        s, e = int(ptr[i]), int(ptr[i + 1])
        if s != e:
            ids[s:e] = np.fromiter(bm, dtype=np.uint32, count=e - s)
    return ptr, ids, sizes


def _build_postings(bitmaps, annotation_count):
    postings = [BitMap() for _ in range(annotation_count)]
    for concept, bm in enumerate(bitmaps):
        for annotation in bm:
            postings[annotation].add(concept)
    return postings


def _candidate_batches(ptr, ids, sizes, postings, threshold, batch_size):
    lhs = np.empty(batch_size, dtype=np.int32)
    rhs = np.empty(batch_size, dtype=np.int32)
    used = 0
    for i in range(len(sizes) - 1):
        a = int(sizes[i])
        if a == 0:
            continue
        min_size = math.ceil(threshold * a)
        max_size = math.floor(a / threshold)
        candidates = BitMap()
        for off in range(int(ptr[i]), int(ptr[i + 1])):
            candidates |= postings[int(ids[off])]
        for j in candidates:
            if j <= i:
                continue
            b = int(sizes[j])
            if b < min_size or b > max_size:
                continue
            lhs[used], rhs[used] = i, j
            used += 1
            if used == batch_size:
                yield lhs.copy(), rhs.copy()
                used = 0
    if used:
        yield lhs[:used].copy(), rhs[:used].copy()


async def calculate_similarity(target_graph: nx.MultiDiGraph,
                               target_prefix: ConceptPrefix,
                               corpus_graph: nx.MultiDiGraph = None,
                               corpus_prefix: ConceptPrefix = None,
                               annotation_graph: nx.DiGraph = None,
                               ) -> AsyncIterator[tuple[str, str, float]]:
    threshold = DEFAULT_SIMILARITY_THRESHOLD
    target_graph = deepcopy(target_graph)
    filter_edges_by_relationship(target_graph, {ConceptRelationshipType.IS_A, ConceptRelationshipType.PART_OF})
    verbose_print(f'Relationship filtered down to {len(target_graph.edges):,} edges in target graph.')
    target_graph = nx.DiGraph(target_graph)
    annotation_graph = annotation_graph.to_undirected(as_view=True)
    count_annotation_for_graph(target_graph=target_graph, annotation_graph=annotation_graph, target_prefix=target_prefix)
    remove = [n for n in target_graph if target_graph.nodes[n].get('annotation_count', 0) == 0]
    verbose_print(f'Pruning {len(remove):,} nodes with zero annotations.')
    target_graph.remove_nodes_from(remove)
    nodes = list(target_graph.nodes)
    if len(nodes) < 2:
        return
    bitmaps, total_annotations = _build_annotation_sets(
        target_graph, target_prefix, corpus_prefix, annotation_graph, nodes
    )
    ptr, ids, sizes = _bitmaps_to_csr(bitmaps)
    postings = _build_postings(bitmaps, total_annotations)
    del bitmaps
    use_cuda = _fits_cuda(ptr, ids, _GPU_PAIR_BATCH_SIZE)
    if use_cuda:
        scorer = _CudaScorer(ptr, ids, total_annotations, threshold)
        batches = _candidate_batches(ptr, ids, sizes, postings, threshold, _GPU_PAIR_BATCH_SIZE)
    else:
        try:
            if CONFIG.process_limit is not None:
                set_num_threads(min(int(CONFIG.process_limit), get_num_threads()))
        except (TypeError, ValueError):
            pass
        _score_pairs_cpu(ptr, ids, np.array([0], np.int32), np.array([1], np.int32), total_annotations, threshold)
        batches = _candidate_batches(ptr, ids, sizes, postings, threshold, _CPU_PAIR_BATCH_SIZE)
    for lhs, rhs in batches:
        scores = scorer.score(lhs, rhs) if use_cuda else _score_pairs_cpu(
            ptr, ids, lhs, rhs, total_annotations, threshold
        )
        for k in np.nonzero(~np.isnan(scores))[0]:
            yield nodes[int(lhs[k])], nodes[int(rhs[k])], float(scores[k])
