import math

import numpy as np
import pytest

from bioterms.similarity.co_annotation import (
    _pair_similarity,
    _score_pairs_cpu,
    _sorted_intersection_size,
)


def _csr(rows):
    row_ptr = np.zeros(len(rows) + 1, dtype=np.int64)
    row_ptr[1:] = np.cumsum([len(row) for row in rows])
    annotation_ids = np.array([value for row in rows for value in sorted(row)], dtype=np.uint32)
    return row_ptr, annotation_ids


def test_sorted_intersection_size():
    ids = np.array([1, 2, 3, 5, 2, 3, 4, 5, 9], dtype=np.uint32)

    assert _sorted_intersection_size(ids, 0, 4, 4, 9) == 3   # {1,2,3,5} & {2,3,4,5,9}
    assert _sorted_intersection_size(ids, 0, 0, 4, 9) == 0   # empty row


def test_pair_similarity_is_npmi_times_jaccard():
    # {1,2,3} vs {2,3,4} out of 10 annotated items: 2 shared, 4 in the union.
    jaccard = 2 / 4
    npmi = (1 + math.log((2 * 10) / (3 * 3)) / math.log(10 / 2)) / 2

    assert _pair_similarity(2, 3, 3, 10, 0.0) == pytest.approx(npmi * jaccard)


@pytest.mark.parametrize(('inter', 'len_1', 'len_2', 'total', 'threshold'), [
    (0, 3, 3, 10, 0.0),    # nothing shared
    (1, 5, 5, 10, 0.2),    # Jaccard 1/9 below the threshold
    (2, 3, 3, 10, 0.5),    # passes Jaccard (0.5) but the combined score does not
])
def test_pair_similarity_rejects_to_nan(inter, len_1, len_2, total, threshold):
    assert math.isnan(_pair_similarity(inter, len_1, len_2, total, threshold))


def test_pair_similarity_full_overlap_of_whole_corpus_scores_one():
    assert _pair_similarity(4, 4, 4, 4, 0.9) == 1.0


def test_score_pairs_cpu_scores_each_pair():
    row_ptr, annotation_ids = _csr([{1, 2, 3}, {2, 3, 4}, set(), {7}])
    lhs = np.array([0, 0, 2, 3], dtype=np.int32)
    rhs = np.array([1, 0, 1, 3], dtype=np.int32)

    scores = _score_pairs_cpu(row_ptr, annotation_ids, lhs, rhs, 10, 0.0)

    assert scores[0] == pytest.approx(_pair_similarity(2, 3, 3, 10, 0.0))
    assert scores[1] == pytest.approx(_pair_similarity(3, 3, 3, 10, 0.0))
    assert math.isnan(scores[2])
    assert scores[3] == pytest.approx(_pair_similarity(1, 1, 1, 10, 0.0))
