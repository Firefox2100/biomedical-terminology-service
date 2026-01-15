"""
Setup Pytheus metrics backend based on configuration.
"""

from pytheus.backends import load_backend
from pytheus.backends.redis import MultiProcessRedisBackend
from pytheus.metrics import Counter, Histogram

from .consts import CONFIG
from .enums import CacheDriverType


class NoOpMetrics:
    """
    No-op metrics for when metrics are disabled.
    """
    def __getattr__(self, item):
        return self

    def labels(self, *args, **kwargs):
        return self

    def observe(self, value):
        pass

    def inc(self, amount=1):
        pass


if CONFIG.enable_metrics:
    if CONFIG.cache_driver == CacheDriverType.REDIS:
        # Enable Redis backend for multiprocess support
        load_backend(
            backend_class=MultiProcessRedisBackend,
            backend_config={
                'host': CONFIG.redis_host,
                'port': CONFIG.redis_port,
                'db': 15,
            },
        )
    else:
        # Use the default backend (in-memory)
        load_backend()


DOCDB_OP_DURATION: Histogram = NoOpMetrics()
DOCDB_OP_TTFI: Histogram = NoOpMetrics()
DOCDB_OP_ERRORS: Counter = NoOpMetrics()

GRAPHDB_OP_DURATION: Histogram = NoOpMetrics()
GRAPHDB_OP_TTFR: Histogram = NoOpMetrics()
GRAPHDB_OP_ERRORS: Counter = NoOpMetrics()
GRAPHDB_OP_RETRYS: Counter = NoOpMetrics()

EMBED_LOCK_WAIT: Histogram = NoOpMetrics()
EMBED_DURATION: Histogram = NoOpMetrics()
EMBED_TEXTS: Histogram = NoOpMetrics()
EMBED_CHARS: Histogram = NoOpMetrics()
EMBED_ERRORS: Counter = NoOpMetrics()

VDB_DUR: Histogram = NoOpMetrics()
VDB_ERR: Counter = NoOpMetrics()

AUTOCOMPLETE_ITEMS: Histogram = NoOpMetrics()
AUTOCOMPLETE_LIMIT: Histogram = NoOpMetrics()
AUTOCOMPLETE_QUERY_LEN: Histogram = NoOpMetrics()
AUTOCOMPLETE_STREAM_ERRORS: Counter = NoOpMetrics()

EXPAND_ROOTS: Histogram = NoOpMetrics()
EXPAND_DEPTH: Histogram = NoOpMetrics()
EXPAND_LIMIT: Histogram = NoOpMetrics()
EXPAND_REQS: Counter = NoOpMetrics()
EXPAND_DESC_COUNT: Histogram = NoOpMetrics()

MAP_REQS: Counter = NoOpMetrics()
MAP_ROOTS: Histogram = NoOpMetrics()
MAP_HOPS: Histogram = NoOpMetrics()
MAP_LIMIT: Histogram = NoOpMetrics()
MAP_COUNT: Histogram = NoOpMetrics()

SEARCH_ITEMS: Histogram = NoOpMetrics()
SEARCH_LIMIT: Histogram = NoOpMetrics()
SEARCH_QUERY_LEN: Histogram = NoOpMetrics()

SIM_REQS: Counter = NoOpMetrics()
SIM_ROOTS: Histogram = NoOpMetrics()
SIM_THRESHOLD: Histogram = NoOpMetrics()
SIM_LIMIT: Histogram = NoOpMetrics()
SIM_GROUPS: Histogram = NoOpMetrics()
SIM_PER_GROUP: Histogram = NoOpMetrics()
SIM_TOTAL: Histogram = NoOpMetrics()


if CONFIG.enable_metrics:
    DOCDB_OP_DURATION = Histogram(
        'docdb_op_duration_seconds',
        'Document DB operation duration.',
        required_labels=['backend', 'op', 'prefix', 'result'],
    )
    DOCDB_OP_TTFI = Histogram(
        "docdb_op_time_to_first_item_seconds",
        "Time to first yielded item for streamed DB ops.",
        required_labels=['backend', 'op', 'prefix', 'result'],
    )
    DOCDB_OP_ERRORS = Counter(
        "docdb_op_errors_total",
        "Document DB operation errors.",
        required_labels=['backend', 'op', 'prefix', 'result'],
    )

    GRAPHDB_OP_DURATION = Histogram(
        'graphdb_op_duration_seconds',
        'Graph DB operation duration.',
        required_labels=['backend', 'op', 'prefix', 'mode', 'result'],
    )
    GRAPHDB_OP_TTFR = Histogram(
        'graphdb_op_time_to_first_result_seconds',
        'Time to first result for streamed Graph DB ops.',
        required_labels=['backend', 'op', 'prefix', 'mode', 'result'],
    )
    GRAPHDB_OP_ERRORS = Counter(
        'graphdb_op_errors_total',
        'Graph DB operation errors.',
        required_labels = ['backend', 'op', 'prefix', 'error_type'],
    )
    GRAPHDB_OP_RETRYS = Counter(
        'graphdb_op_retrys_total',
        'Graph DB operation retrys.',
        required_labels = ['backend', 'op', 'reason'],
    )

    EMBED_LOCK_WAIT = Histogram(
        'embed_lock_wait_seconds',
        'Time spent waiting for embed lock.',
        required_labels=['model'],
    )
    EMBED_DURATION = Histogram(
        'embed_duration_seconds',
        'Embedding operation duration.',
        required_labels=['model', 'result'],
    )
    EMBED_TEXTS = Histogram(
        'embed_texts_count',
        'Number of texts embedded per request.',
        required_labels=['model'],
        buckets=[1, 2, 3, 4, 5, 6, 7, 10, 16, 32, 64],
    )
    EMBED_CHARS = Histogram(
        'embed_chars_total',
        'Number of characters embedded per request.',
        required_labels=['model'],
        buckets=[4, 8, 16, 32, 64, 128, 256, 512],
    )
    EMBED_ERRORS = Counter(
        'embed_errors_total',
        'Embedding operation errors.',
        required_labels=['model', 'error_type']
    )

    VDB_DUR = Histogram(
        'vectordb_op_duration_seconds',
        'Vector DB operation duration.',
        required_labels=['backend', 'op', 'prefix', 'result'],
    )
    VDB_ERR = Counter(
        'vectordb_op_errors_total',
        'Vector DB operation errors.',
        required_labels=['backend', 'op', 'prefix', 'error_type'],
    )

    AUTOCOMPLETE_ITEMS = Histogram(
        'autocomplete_items_returned',
        'Number of items returned by autocomplete.',
        required_labels=['prefix'],
        buckets=[0, 1, 5, 10, 15, 20, 25, 50, 100, 150, 200, 250, 300, 400, 500],
    )
    AUTOCOMPLETE_LIMIT = Histogram(
        'autocomplete_limit',
        'Requested autocomplete limit.',
        required_labels=['prefix'],
        buckets=[25, 50, 100, 150, 200, 250],
    )
    AUTOCOMPLETE_QUERY_LEN = Histogram(
        'autocomplete_query_length',
        'Length of autocomplete query string.',
        required_labels=['prefix'],
        buckets=[3, 5, 10, 15, 20, 25, 30, 40, 50],
    )
    AUTOCOMPLETE_STREAM_ERRORS = Counter(
        'autocomplete_stream_errors_total',
        'Errors during autocomplete streaming.',
        required_labels=['prefix', 'stage'],
    )

    EXPAND_ROOTS = Histogram(
        'expand_roots_count',
        'Number of root terms requested for expansion.',
        required_labels=['prefix'],
        buckets=[1, 5, 10, 20, 50, 100, 200, 500, 1000],
    )
    EXPAND_DEPTH = Histogram(
        'expand_depth_requested',
        'Requested expansion depth.',
        required_labels=['prefix'],
        buckets=[0, 1, 2, 3, 5, 10, 20, 50, 100],
    )
    EXPAND_LIMIT = Histogram(
        'expand_limit_requested',
        'Requested expansion limit.',
        required_labels=['prefix', 'has_limit'],
        buckets=[0, 10, 50, 100, 200, 500, 1000, 5000, 10000],
    )
    EXPAND_REQS = Counter(
        'expand_requests_total',
        'Total number of expand requests.',
        required_labels=['prefix', 'mode'],
    )
    EXPAND_DESC_COUNT = Histogram(
        'expand_descendants_count',
        'Number of descendants returned in expansion.',
         required_labels = ['prefix', 'mode'],
        buckets=[0, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000],
    )

    MAP_REQS = Counter(
        'map_requests_total',
        'Total number of map requests.',
        required_labels=['prefix', 'target_prefix', 'mode'],
    )
    MAP_ROOTS = Histogram(
        'map_roots_count',
        'Number of root terms requested for mapping.',
        required_labels=['prefix', 'target_prefix'],
        buckets=[1, 5, 10, 20, 50, 100, 200, 500, 1000],
    )
    MAP_HOPS = Histogram(
        'map_hops_requested',
        'Requested number of hops for mapping.',
        required_labels=['prefix', 'target_prefix'],
        buckets=[1, 2, 3, 5, 10, 20],
    )
    MAP_LIMIT = Histogram(
        'map_limit_requested',
        'Requested mapping limit.',
        required_labels=['prefix', 'target_prefix', 'has_limit'],
        buckets=[0, 10, 50, 100, 200, 500, 1000, 5000, 10000],
    )
    MAP_COUNT = Histogram(
        'map_mapped_terms_count',
        'Number of terms returned in mapping.',
        required_labels=['prefix', 'target_prefix'],
        buckets=[0, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000],
    )

    SEARCH_ITEMS = Histogram(
        'search_items_returned',
        'Number of items returned by search.',
        required_labels=['prefix'],
        buckets=[0, 1, 5, 10, 15, 20, 25, 50, 100, 150, 200, 250, 300, 400, 500],
    )
    SEARCH_LIMIT = Histogram(
        'search_limit',
        'Requested search limit.',
        required_labels=['prefix'],
        buckets=[10, 25, 50, 100, 150, 200, 250],
    )
    SEARCH_QUERY_LEN = Histogram(
        'search_query_length',
        'Length of search query string.',
        required_labels=['prefix'],
        buckets=[3, 5, 10, 15, 20, 25, 30, 40, 50],
    )

    SIM_REQS = Counter(
        'similarity_requests_total',
        'Total number of similarity search requests.',
        required_labels=['prefix', 'variant', 'filter', 'method', 'corpus', 'has_limit'],
    )
    SIM_ROOTS = Histogram(
        'similarity_roots_count',
        'Number of root terms requested for similarity search.',
        required_labels=['prefix'],
        buckets=[1, 5, 10, 20, 50, 100, 200, 500, 1000],
    )
    SIM_THRESHOLD = Histogram(
        'similarity_threshold_requested',
        'Requested similarity threshold.',
        required_labels=['prefix'],
        buckets=[0.2, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99],
    )
    SIM_LIMIT = Histogram(
        'similarity_limit_requested',
        'Requested similarity search limit.',
        required_labels=['prefix', 'has_limit'],
        buckets=[0, 10, 50, 100, 200, 500, 1000, 5000, 10000],
    )
    SIM_GROUPS = Histogram(
        'similarity_groups_count',
        'Number of groups returned in similarity search.',
        required_labels=['prefix', 'variant'],
        buckets=[0, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000],
    )
    SIM_PER_GROUP = Histogram(
        'similarity_per_group_count',
        'Number of items per group returned in similarity search.',
        required_labels=['prefix', 'variant'],
        buckets=[0, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000],
    )
    SIM_TOTAL = Histogram(
        'similarity_total_items_count',
        'Total number of items returned in similarity search.',
        required_labels=['prefix', 'variant'],
        buckets=[0, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000],
    )
