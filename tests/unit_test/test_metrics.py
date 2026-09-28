"""Tests for the lazily bound Prometheus metric proxies."""
import pytest

from bioterms.etc import metrics
from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import CacheDriverType


@pytest.fixture
def fresh_metrics(monkeypatch):
    """Reset initialisation state and unbind every proxy afterwards, so no test leaks metrics."""
    proxies = [value for value in vars(metrics).values() if isinstance(value, metrics.MetricProxy)]
    monkeypatch.setattr(metrics, '_metrics_initialized', False)
    backends = []
    monkeypatch.setattr(metrics, 'load_backend', lambda **kwargs: backends.append(kwargs))
    yield backends
    for proxy in proxies:
        proxy.bind(None)


def test_unbound_proxies_are_inert():
    proxy = metrics.MetricProxy()

    proxy.labels(op='x').observe(1.0)
    proxy.inc()
    assert proxy.anything is proxy


def test_disabled_metrics_bind_nothing(fresh_metrics, monkeypatch):
    monkeypatch.setattr(CONFIG, 'enable_metrics', False)

    metrics.initialize_metrics()

    assert fresh_metrics == []
    assert metrics.DOCDB_OP_DURATION._target is None


@pytest.mark.parametrize(('driver', 'expects_redis'), [
    (CacheDriverType.REDIS, True),
    (None, False),  # no shared cache: in-process metrics backend
])
def test_initialisation_binds_every_proxy_once(fresh_metrics, monkeypatch, driver, expects_redis):
    monkeypatch.setattr(CONFIG, 'enable_metrics', True)
    monkeypatch.setattr(CONFIG, 'cache_driver', driver)

    metrics.initialize_metrics()
    metrics.initialize_metrics()  # idempotent

    assert len(fresh_metrics) == 1
    assert ('backend_class' in fresh_metrics[0]) is expects_redis
    proxies = [value for value in vars(metrics).values() if isinstance(value, metrics.MetricProxy)]
    assert proxies and all(proxy._target is not None for proxy in proxies)
    metrics.DOCDB_OP_DURATION.labels(backend='sql', op='get', prefix='hpo', result='ok').observe(0.1)
    metrics.DOCDB_OP_ERRORS.labels(backend='sql', op='get', prefix='hpo', error_type='X').inc()
