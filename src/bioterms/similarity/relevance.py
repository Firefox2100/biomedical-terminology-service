import math
from copy import deepcopy
from typing import AsyncIterator

import networkx as nx
import numpy as np
from numba import njit, prange, set_num_threads, get_num_threads
from pyroaring import BitMap

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptRelationshipType
from bioterms.etc.utils import verbose_print
from .utils import count_annotation_for_graph, filter_edges_by_relationship

METHOD_NAME = 'Relevance Method'
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
def _score_cpu(ptr, ids, ic, relevance, lhs, rhs, threshold):
    out = np.empty(lhs.shape[0], dtype=np.float64)
    for k in prange(lhs.shape[0]):
        i, j = lhs[k], rhs[k]
        denom = ic[i] + ic[j]
        if denom <= 0.0:
            out[k] = np.nan
            continue
        if 2.0 * min(ic[i], ic[j]) / denom < threshold:
            out[k] = np.nan
            continue
        p1, e1, p2, e2 = ptr[i], ptr[i + 1], ptr[j], ptr[j + 1]
        mica = -1
        mica_ic = -1.0
        while p1 < e1 and p2 < e2:
            a, b = ids[p1], ids[p2]
            if a < b:
                p1 += 1
            elif a > b:
                p2 += 1
            else:
                if ic[a] > mica_ic:
                    mica, mica_ic = a, ic[a]
                p1 += 1
                p2 += 1
        if mica < 0:
            out[k] = np.nan
            continue
        s = (2.0 * mica_ic / denom) * relevance[mica]
        out[k] = min(1.0, s) if s >= threshold and s >= 0.0 else np.nan
    return out


_CUDA_KERNEL = r'''
extern "C" __global__
void score(const long long* ptr, const unsigned int* ids, const double* ic,
           const double* rel, const int* lhs, const int* rhs, const double threshold,
           double* out, const long long n) {
    long long k=(long long)blockDim.x*blockIdx.x+threadIdx.x;
    if (k>=n) return;
    int i=lhs[k], j=rhs[k];
    double denom=ic[i]+ic[j];
    if (!(denom>0.0)) { out[k]=NAN; return; }
    double min_ic=ic[i]<ic[j]?ic[i]:ic[j];
    if (2.0*min_ic/denom < threshold) { out[k]=NAN; return; }
    long long p1=ptr[i], e1=ptr[i+1], p2=ptr[j], e2=ptr[j+1];
    int mica=-1; double mica_ic=-1.0;
    while (p1<e1 && p2<e2) {
        unsigned int a=ids[p1], b=ids[p2];
        if (a<b) ++p1; else if (a>b) ++p2; else {
            if (ic[a]>mica_ic) { mica=(int)a; mica_ic=ic[a]; }
            ++p1; ++p2;
        }
    }
    if (mica<0) { out[k]=NAN; return; }
    double s=(2.0*mica_ic/denom)*rel[mica];
    if (s>1.0) s=1.0;
    out[k]=(s>=threshold && s>=0.0)?s:NAN;
}
'''


class _CudaScorer:
    def __init__(self, ptr, ids, ic, relevance, threshold):
        self.ptr, self.ids = cp.asarray(ptr), cp.asarray(ids)
        self.ic, self.relevance = cp.asarray(ic), cp.asarray(relevance)
        self.threshold = threshold
        self.kernel = cp.RawKernel(_CUDA_KERNEL, 'score')

    def score(self, lhs, rhs):
        gl, gr = cp.asarray(lhs), cp.asarray(rhs)
        out = cp.empty(lhs.shape[0], dtype=cp.float64)
        threads = 256
        blocks = (lhs.shape[0] + threads - 1) // threads
        self.kernel((blocks,), (threads,), (self.ptr, self.ids, self.ic, self.relevance,
                    gl, gr, np.float64(self.threshold), out, np.int64(lhs.shape[0])))
        return cp.asnumpy(out)


def _cuda_available():
    if cp is None:
        return False
    try:
        return cp.cuda.runtime.getDeviceCount() > 0
    except Exception:
        return False


def _fits_cuda(ptr, ids, ic, relevance, batch_size):
    if not _cuda_available():
        return False
    try:
        free, _ = cp.cuda.runtime.memGetInfo()
    except Exception:
        return False
    required = ptr.nbytes + ids.nbytes + ic.nbytes + relevance.nbytes + batch_size * 16
    return required <= int(free * 0.8)


def _build_informative_ancestor_sets(graph, node_to_index, relevance, valid, threshold):
    sets = [BitMap() for _ in range(len(node_to_index))]
    for node in reversed(list(nx.topological_sort(graph))):
        i = node_to_index[node]
        bm = sets[i]
        if valid[i] and relevance[i] >= threshold:
            bm.add(i)
        # Standard simRel: unique semantic ancestor set, preserving existing graph orientation.
        for successor in graph.successors(node):
            bm |= sets[node_to_index[successor]]
    return sets


def _bitmaps_to_csr(bitmaps):
    ptr = np.empty(len(bitmaps) + 1, np.int64)
    ptr[0] = 0
    for i, bm in enumerate(bitmaps):
        ptr[i + 1] = ptr[i] + len(bm)
    ids = np.empty(int(ptr[-1]), np.uint32)
    for i, bm in enumerate(bitmaps):
        s, e = int(ptr[i]), int(ptr[i + 1])
        if s != e:
            ids[s:e] = np.fromiter(bm, dtype=np.uint32, count=e - s)
    return ptr, ids


def _build_postings(bitmaps):
    postings = {}
    for concept, bm in enumerate(bitmaps):
        for ancestor in bm:
            postings.setdefault(ancestor, BitMap()).add(concept)
    return postings


def _candidate_batches(ptr, ids, postings, ic, valid, threshold, batch_size):
    lhs = np.empty(batch_size, np.int32)
    rhs = np.empty(batch_size, np.int32)
    used = 0
    for i in range(len(valid) - 1):
        if not valid[i] or ptr[i] == ptr[i + 1]:
            continue
        candidates = BitMap()
        for off in range(int(ptr[i]), int(ptr[i + 1])):
            candidates |= postings[int(ids[off])]
        for j in candidates:
            if j <= i or not valid[j]:
                continue
            denom = ic[i] + ic[j]
            if denom <= 0.0 or 2.0 * min(ic[i], ic[j]) / denom < threshold:
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
    if not nx.is_directed_acyclic_graph(target_graph):
        raise ValueError('Filtered target ontology must be a DAG.')
    annotation_graph = annotation_graph.to_undirected(as_view=True)
    count_annotation_for_graph(target_graph=target_graph, annotation_graph=annotation_graph, target_prefix=target_prefix)
    max_count = max((target_graph.nodes[n].get('annotation_count', 0) for n in target_graph), default=0)
    if max_count <= 0:
        return
    nodes = list(target_graph.nodes)
    node_to_index = {n: i for i, n in enumerate(nodes)}
    ic = np.zeros(len(nodes), np.float64)
    relevance = np.zeros(len(nodes), np.float64)
    valid = np.zeros(len(nodes), np.bool_)
    for node, i in node_to_index.items():
        count = target_graph.nodes[node].get('annotation_count', 0)
        if count > 0:
            valid[i] = True
            ic[i] = -math.log(count / max_count)
            relevance[i] = 1.0 - count / max_count
    if valid.sum() < 2:
        return
    bitmaps = _build_informative_ancestor_sets(target_graph, node_to_index, relevance, valid, threshold)
    ptr, ids = _bitmaps_to_csr(bitmaps)
    postings = _build_postings(bitmaps)
    del bitmaps
    use_cuda = _fits_cuda(ptr, ids, ic, relevance, _GPU_PAIR_BATCH_SIZE)
    if use_cuda:
        scorer = _CudaScorer(ptr, ids, ic, relevance, threshold)
        batches = _candidate_batches(ptr, ids, postings, ic, valid, threshold, _GPU_PAIR_BATCH_SIZE)
    else:
        try:
            if CONFIG.process_limit is not None:
                set_num_threads(min(int(CONFIG.process_limit), get_num_threads()))
        except (TypeError, ValueError):
            pass
        _score_cpu(ptr, ids, ic, relevance, np.array([0], np.int32), np.array([1], np.int32), threshold)
        batches = _candidate_batches(ptr, ids, postings, ic, valid, threshold, _CPU_PAIR_BATCH_SIZE)
    for lhs, rhs in batches:
        scores = scorer.score(lhs, rhs) if use_cuda else _score_cpu(ptr, ids, ic, relevance, lhs, rhs, threshold)
        for k in np.nonzero(~np.isnan(scores))[0]:
            yield nodes[int(lhs[k])], nodes[int(rhs[k])], float(scores[k])
