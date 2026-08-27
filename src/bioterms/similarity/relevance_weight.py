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
from .utils import filter_edges_by_relationship

METHOD_NAME = 'Weighed Relevance Method'
DEFAULT_SIMILARITY_THRESHOLD = 0.2
CORPUS_REQUIRED = True
CORPUS_GRAPH_REQUIRED = True

_tune_factor = 0.5
_convergence_threshold = 1e-3
_relaxation_factor = 1.0
_path_attenuation_factor = 0.5
_MAX_ITERATIONS = 1000
_CPU_PAIR_BATCH_SIZE = 1 << 20
_GPU_PAIR_BATCH_SIZE = 1 << 22

try:
    import cupy as cp
except ImportError:
    cp = None


@njit(cache=True, nogil=True, inline='always')
def _logaddexp(a, b):
    if a == -np.inf:
        return b
    if b == -np.inf:
        return a
    if a > b:
        return a + math.log1p(math.exp(b - a))
    return b + math.log1p(math.exp(a - b))


@njit(cache=True, nogil=True)
def _direct_log_sums(annotation_ptr, annotation_ids, source_ic, tune_factor, initial):
    result = np.full(annotation_ptr.shape[0] - 1, -np.inf, np.float64)
    for node in range(result.shape[0]):
        value = -np.inf
        for off in range(annotation_ptr[node], annotation_ptr[node + 1]):
            other = annotation_ids[off]
            if initial:
                lw = 0.0
            else:
                other_ic = source_ic[other]
                if not np.isfinite(other_ic):
                    continue
                lw = tune_factor * other_ic
            value = _logaddexp(value, lw)
        result[node] = value
    return result


@njit(cache=True, nogil=True)
def _raw_path_log_sums(direct_log, pred_ptr, pred_ids, topo):
    result = direct_log.copy()
    for pos in range(topo.shape[0]):
        node = topo[pos]
        value = result[node]
        for off in range(pred_ptr[node], pred_ptr[node + 1]):
            value = _logaddexp(value, result[pred_ids[off]])
        result[node] = value
    return result


@njit(cache=True, nogil=True)
def _unique_log_sums(unique_ptr, unique_ids, source_ic, tune_factor, initial):
    result = np.full(unique_ptr.shape[0] - 1, -np.inf, np.float64)
    for node in range(result.shape[0]):
        value = -np.inf
        for off in range(unique_ptr[node], unique_ptr[node + 1]):
            other = unique_ids[off]
            if initial:
                lw = 0.0
            else:
                other_ic = source_ic[other]
                if not np.isfinite(other_ic):
                    continue
                lw = tune_factor * other_ic
            value = _logaddexp(value, lw)
        result[node] = value
    return result


@njit(cache=True, nogil=True)
def _attenuate_path_multiplicity(unique_log, raw_path_log, attenuation):
    out = unique_log.copy()
    for i in range(out.shape[0]):
        u = unique_log[i]
        p = raw_path_log[i]
        if not np.isfinite(u):
            out[i] = -np.inf
            continue
        # Floating error can make P infinitesimally below U; clamp at r=1.
        log_ratio = p - u if np.isfinite(p) and p > u else 0.0
        out[i] = u + math.log1p(attenuation * log_ratio)
    return out


@njit(cache=True, nogil=True)
def _raw_ic(log_sums):
    maximum = -np.inf
    for x in log_sums:
        if np.isfinite(x) and x > maximum:
            maximum = x
    result = np.full(log_sums.shape[0], np.nan, np.float64)
    if not np.isfinite(maximum):
        return result
    for i in range(log_sums.shape[0]):
        if np.isfinite(log_sums[i]):
            result[i] = maximum - log_sums[i]
    return result


@njit(cache=True, nogil=True)
def _relax(old, raw, alpha):
    out = raw.copy()
    max_delta = 0.0
    for i in range(raw.shape[0]):
        if not np.isfinite(raw[i]):
            continue
        if np.isfinite(old[i]):
            out[i] = old[i] + alpha * (raw[i] - old[i])
            d = abs(out[i] - old[i])
            if d > max_delta:
                max_delta = d
    return out, max_delta


def _build_predecessor_csr(graph, nodes, index):
    ptr = np.empty(len(nodes) + 1, np.int64)
    ptr[0] = 0
    for i, node in enumerate(nodes):
        ptr[i + 1] = ptr[i] + graph.in_degree(node)
    ids = np.empty(int(ptr[-1]), np.int32)
    for i, node in enumerate(nodes):
        off = int(ptr[i])
        for predecessor in graph.predecessors(node):
            ids[off] = index[predecessor]
            off += 1
    topo = np.fromiter((index[n] for n in nx.topological_sort(graph)), np.int32, count=len(nodes))
    return ptr, ids, topo


def _build_direct_annotation_csr(source_nodes, source_prefix, dest_prefix, dest_index, annotation_graph):
    ptr = np.empty(len(source_nodes) + 1, np.int64)
    ptr[0] = 0
    prefix = f'{dest_prefix.value}:'
    rows = []
    for i, source in enumerate(source_nodes):
        row = []
        name = f'{source_prefix.value}:{source}'
        if name in annotation_graph:
            for neighbour in annotation_graph.neighbors(name):
                if neighbour.startswith(prefix):
                    dest = neighbour.split(':', 1)[1]
                    idx = dest_index.get(dest)
                    if idx is not None:
                        row.append(idx)
        rows.append(row)
        ptr[i + 1] = ptr[i] + len(row)
    ids = np.empty(int(ptr[-1]), np.int32)
    for i, row in enumerate(rows):
        s = int(ptr[i])
        ids[s:s + len(row)] = row
    return ptr, ids


def _build_unique_annotation_csr(graph, nodes, index, direct_ptr, direct_ids):
    """Unique annotation concepts reachable through node + descendants.

    With child->parent edges, predecessors are descendants when accumulating
    from leaves to parents in topological order. Roaring union removes repeated
    annotations and repeated inheritance paths.
    """
    bitmaps = [BitMap() for _ in nodes]
    for i in range(len(nodes)):
        for off in range(int(direct_ptr[i]), int(direct_ptr[i + 1])):
            bitmaps[i].add(int(direct_ids[off]))
    for node in nx.topological_sort(graph):
        i = index[node]
        if not bitmaps[i]:
            continue
        for parent in graph.successors(node):
            bitmaps[index[parent]] |= bitmaps[i]
    ptr = np.empty(len(nodes) + 1, np.int64)
    ptr[0] = 0
    for i, bm in enumerate(bitmaps):
        ptr[i + 1] = ptr[i] + len(bm)
    ids = np.empty(int(ptr[-1]), np.int32)
    for i, bm in enumerate(bitmaps):
        s, e = int(ptr[i]), int(ptr[i + 1])
        if s != e:
            ids[s:e] = np.fromiter(bm, dtype=np.int32, count=e - s)
    return ptr, ids


def _update_ic(direct_ptr, direct_ids, unique_ptr, unique_ids, pred_ptr, pred_ids, topo,
               source_ic, current_ic, initial):
    direct = _direct_log_sums(direct_ptr, direct_ids, source_ic, _tune_factor, initial)
    raw_path = _raw_path_log_sums(direct, pred_ptr, pred_ids, topo)
    unique = _unique_log_sums(unique_ptr, unique_ids, source_ic, _tune_factor, initial)
    attenuated = _attenuate_path_multiplicity(unique, raw_path, _path_attenuation_factor)
    raw = _raw_ic(attenuated)
    return _relax(current_ic, raw, _relaxation_factor)


def _coupled_ic(target_struct, corpus_struct, target_count, corpus_count):
    target_ic = np.full(target_count, np.nan, np.float64)
    corpus_ic = np.full(corpus_count, np.nan, np.float64)
    for iteration in range(_MAX_ITERATIONS):
        target_ic, td = _update_ic(*target_struct, corpus_ic, target_ic, iteration == 0)
        corpus_ic, cd = _update_ic(*corpus_struct, target_ic, corpus_ic, False)
        verbose_print(
            f'Iteration {iteration + 1}: max target IC delta={td:.6f}, max corpus IC delta={cd:.6f}'
        )
        if iteration > 0 and max(td, cd) < _convergence_threshold:
            verbose_print(f'Convergence reached after {iteration + 1} iterations.')
            return target_ic, corpus_ic
    raise RuntimeError(
        f'Weighted relevance did not converge within {_MAX_ITERATIONS} iterations. '
        'Consider reducing _relaxation_factor.'
    )


@njit(cache=True, nogil=True, parallel=True)
def _score_cpu(ptr, ids, ic, relevance, lhs, rhs, threshold):
    out = np.empty(lhs.shape[0], np.float64)
    for k in prange(lhs.shape[0]):
        i, j = lhs[k], rhs[k]
        denom = ic[i] + ic[j]
        if denom <= 0.0 or 2.0 * min(ic[i], ic[j]) / denom < threshold:
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
        out = cp.empty(lhs.shape[0], cp.float64)
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


def _build_informative_ancestor_sets(graph, index, relevance, valid, threshold):
    sets = [BitMap() for _ in range(len(index))]
    for node in reversed(list(nx.topological_sort(graph))):
        i = index[node]
        bm = sets[i]
        if valid[i] and relevance[i] >= threshold:
            bm.add(i)
        for successor in graph.successors(node):
            bm |= sets[index[successor]]
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
            ids[s:e] = np.fromiter(bm, np.uint32, count=e - s)
    return ptr, ids


def _build_postings(bitmaps):
    postings = {}
    for concept, bm in enumerate(bitmaps):
        for ancestor in bm:
            postings.setdefault(ancestor, BitMap()).add(concept)
    return postings


def _candidate_batches(ptr, ids, postings, ic, valid, threshold, batch_size):
    lhs, rhs = np.empty(batch_size, np.int32), np.empty(batch_size, np.int32)
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

    corpus_graph = deepcopy(corpus_graph)
    filter_edges_by_relationship(corpus_graph, {ConceptRelationshipType.IS_A, ConceptRelationshipType.PART_OF})
    verbose_print(f'Relationship filtered down to {len(corpus_graph.edges):,} edges in corpus graph.')

    target_graph, corpus_graph = nx.DiGraph(target_graph), nx.DiGraph(corpus_graph)
    if not nx.is_directed_acyclic_graph(target_graph) or not nx.is_directed_acyclic_graph(corpus_graph):
        raise ValueError('Both filtered ontologies must be DAGs.')
    annotation_graph = annotation_graph.to_undirected(as_view=True)

    target_nodes, corpus_nodes = list(target_graph), list(corpus_graph)
    if len(target_nodes) >= (1 << 31) or len(corpus_nodes) >= (1 << 31):
        raise ValueError('Ontology too large for int32 node IDs.')
    target_index = {n: i for i, n in enumerate(target_nodes)}
    corpus_index = {n: i for i, n in enumerate(corpus_nodes)}

    t_pred = _build_predecessor_csr(target_graph, target_nodes, target_index)
    c_pred = _build_predecessor_csr(corpus_graph, corpus_nodes, corpus_index)
    t_direct = _build_direct_annotation_csr(
        target_nodes, target_prefix, corpus_prefix, corpus_index, annotation_graph
    )
    c_direct = _build_direct_annotation_csr(
        corpus_nodes, corpus_prefix, target_prefix, target_index, annotation_graph
    )
    t_unique = _build_unique_annotation_csr(target_graph, target_nodes, target_index, *t_direct)
    c_unique = _build_unique_annotation_csr(corpus_graph, corpus_nodes, corpus_index, *c_direct)

    # _update_ic expects: direct_ptr, direct_ids, unique_ptr, unique_ids, pred_ptr, pred_ids, topo
    target_struct = (*t_direct, *t_unique, *t_pred)
    corpus_struct = (*c_direct, *c_unique, *c_pred)
    target_ic, _ = _coupled_ic(target_struct, corpus_struct, len(target_nodes), len(corpus_nodes))

    valid = np.isfinite(target_ic)
    if valid.sum() < 2:
        return
    relevance = np.zeros(len(target_nodes), np.float64)
    relevance[valid] = 1.0 - np.exp(-target_ic[valid])

    ancestor_sets = _build_informative_ancestor_sets(
        target_graph, target_index, relevance, valid, threshold
    )
    ptr, ids = _bitmaps_to_csr(ancestor_sets)
    postings = _build_postings(ancestor_sets)
    del ancestor_sets

    use_cuda = _fits_cuda(ptr, ids, target_ic, relevance, _GPU_PAIR_BATCH_SIZE)
    if use_cuda:
        scorer = _CudaScorer(ptr, ids, target_ic, relevance, threshold)
        batches = _candidate_batches(ptr, ids, postings, target_ic, valid, threshold, _GPU_PAIR_BATCH_SIZE)
    else:
        try:
            if CONFIG.process_limit is not None:
                set_num_threads(min(int(CONFIG.process_limit), get_num_threads()))
        except (TypeError, ValueError):
            pass
        _score_cpu(ptr, ids, target_ic, relevance, np.array([0], np.int32), np.array([1], np.int32), threshold)
        batches = _candidate_batches(ptr, ids, postings, target_ic, valid, threshold, _CPU_PAIR_BATCH_SIZE)

    for lhs, rhs in batches:
        scores = scorer.score(lhs, rhs) if use_cuda else _score_cpu(
            ptr, ids, target_ic, relevance, lhs, rhs, threshold
        )
        for k in np.nonzero(~np.isnan(scores))[0]:
            yield target_nodes[int(lhs[k])], target_nodes[int(rhs[k])], float(scores[k])
