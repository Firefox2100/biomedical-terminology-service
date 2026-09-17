"""Metrics definitions and explicit backend initialization."""

from pytheus.backends import load_backend
from pytheus.backends.redis import MultiProcessRedisBackend
from pytheus.metrics import Counter, Histogram

from .consts import CONFIG
from .enums import CacheDriverType


class MetricProxy:
    """Stable metric reference that is inert until a backend is initialized."""

    def __init__(self):
        self._target = None

    def bind(self, target):
        self._target = target

    def __getattr__(self, item):
        if self._target is None:
            return self
        return getattr(self._target, item)

    def labels(self, *args, **kwargs):
        if self._target is None:
            return self
        return self._target.labels(*args, **kwargs)

    def observe(self, value):
        if self._target is not None:
            self._target.observe(value)

    def inc(self, amount=1):
        if self._target is not None:
            self._target.inc(amount)


DOCDB_OP_DURATION: Histogram = MetricProxy()
DOCDB_OP_TTFI: Histogram = MetricProxy()
DOCDB_OP_ERRORS: Counter = MetricProxy()

GRAPHDB_OP_DURATION: Histogram = MetricProxy()
GRAPHDB_OP_TTFR: Histogram = MetricProxy()
GRAPHDB_OP_ERRORS: Counter = MetricProxy()
GRAPHDB_OP_RETRYS: Counter = MetricProxy()

EMBED_LOCK_WAIT: Histogram = MetricProxy()
EMBED_DURATION: Histogram = MetricProxy()
EMBED_TEXTS: Histogram = MetricProxy()
EMBED_CHARS: Histogram = MetricProxy()
EMBED_ERRORS: Counter = MetricProxy()

VDB_DUR: Histogram = MetricProxy()
VDB_ERR: Counter = MetricProxy()

AUTOCOMPLETE_ITEMS: Histogram = MetricProxy()
AUTOCOMPLETE_LIMIT: Histogram = MetricProxy()
AUTOCOMPLETE_QUERY_LEN: Histogram = MetricProxy()
AUTOCOMPLETE_STREAM_ERRORS: Counter = MetricProxy()

EXPAND_ROOTS: Histogram = MetricProxy()
EXPAND_DEPTH: Histogram = MetricProxy()
EXPAND_LIMIT: Histogram = MetricProxy()
EXPAND_REQS: Counter = MetricProxy()
EXPAND_DESC_COUNT: Histogram = MetricProxy()

MAP_REQS: Counter = MetricProxy()
MAP_ROOTS: Histogram = MetricProxy()
MAP_HOPS: Histogram = MetricProxy()
MAP_LIMIT: Histogram = MetricProxy()
MAP_COUNT: Histogram = MetricProxy()

SEARCH_ITEMS: Histogram = MetricProxy()
SEARCH_LIMIT: Histogram = MetricProxy()
SEARCH_QUERY_LEN: Histogram = MetricProxy()

SIM_REQS: Counter = MetricProxy()
SIM_ROOTS: Histogram = MetricProxy()
SIM_THRESHOLD: Histogram = MetricProxy()
SIM_LIMIT: Histogram = MetricProxy()
SIM_GROUPS: Histogram = MetricProxy()
SIM_PER_GROUP: Histogram = MetricProxy()
SIM_TOTAL: Histogram = MetricProxy()


_metrics_initialized = False


def initialize_metrics() -> None:
    """Initialize and bind the configured metrics backend once."""
    global _metrics_initialized
    if _metrics_initialized or not CONFIG.enable_metrics:
        return

    if CONFIG.cache_driver == CacheDriverType.REDIS:
        load_backend(
            backend_class=MultiProcessRedisBackend,
            backend_config={
                'host': CONFIG.redis_host,
                'port': CONFIG.redis_port,
                'db': 15,
            },
        )
    else:
        load_backend()

    proxies = {
        name: value for name, value in globals().items()
        if isinstance(value, MetricProxy)
    }

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

    created_metrics = locals()
    for name, proxy in proxies.items():
        proxy.bind(created_metrics[name])
    _metrics_initialized = True
