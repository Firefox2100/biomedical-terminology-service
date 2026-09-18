import os
import subprocess
import sys


def test_importing_metrics_does_not_connect_to_configured_redis_backend():
    environment = os.environ.copy()
    environment.update({
        'BTS_ENABLE_METRICS': 'true',
        'BTS_CACHE_DRIVER': 'redis',
        'BTS_REDIS_HOST': '203.0.113.1',
        'BTS_REDIS_PORT': '1',
    })

    result = subprocess.run(
        [sys.executable, '-c', 'import bioterms.etc.metrics'],
        env=environment,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )

    assert result.returncode == 0, result.stderr
