"""Shared adaptive, per-prefix Locust benchmark runner."""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import time
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Callable, Sequence

import gevent
from locust import task
from locust.contrib.fasthttp import FastHttpUser
from locust.env import Environment

from bioterms.etc.enums import ConceptPrefix


@dataclass(frozen=True)
class RequestSpec:
    method: str
    path: str
    params: dict[str, object] | None = None
    json_body: dict[str, object] | None = None


RequestBuilder = Callable[[FastHttpUser, str, random.Random], RequestSpec]


@dataclass(frozen=True)
class Endpoint:
    purpose: str
    version: str
    build_request: RequestBuilder
    needs_concept_ids: bool = False

    @property
    def label(self) -> str:
        return f"{self.purpose}-{self.version}"


@dataclass
class StageResult:
    purpose: str
    version: str
    endpoint: str
    prefix: str
    users: int
    duration_seconds: float
    requests: int
    failures: int
    failure_rate_pct: float
    rps: float
    mean_ms: float
    median_ms: float
    p90_ms: float
    p95_ms: float
    p99_ms: float
    minimum_ms: float
    maximum_ms: float
    saturated: bool = False
    saturation_reasons: str = ""


def all_prefixes() -> tuple[str, ...]:
    return tuple(prefix.value for prefix in ConceptPrefix)


def concept_ids(user: FastHttpUser, prefix: str, count: int) -> list[str]:
    cached = user._benchmark_concept_ids  # type: ignore[attr-defined]
    if prefix not in cached:
        with user.client.get(
            f"/api/vocabularies/{prefix}/random",
            params={"count": "100"},
            name="benchmark setup: random concepts",
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(f"setup returned {response.status_code}")
                cached[prefix] = []
            else:
                cached[prefix] = list(response.json())
                response.success()
    values = cached[prefix]
    if not values:
        return []
    return user._benchmark_rng.sample(  # type: ignore[attr-defined]
        values, k=min(count, len(values))
    )


def make_user_class(
    host: str,
    endpoint: Endpoint,
    prefix: str,
    interval: float,
    timeout: float,
    headers: dict[str, str],
    seed: int,
) -> type[FastHttpUser]:
    class BenchmarkUser(FastHttpUser):
        abstract = False
        wait_time = staticmethod(lambda: interval)
        connection_timeout = timeout
        network_timeout = timeout

        def on_start(self) -> None:
            self._benchmark_rng = random.Random(seed + id(self))
            self._benchmark_concept_ids: dict[str, list[str]] = {}
            if endpoint.needs_concept_ids:
                concept_ids(self, prefix, 1)

        @task
        def benchmark_request(self) -> None:
            spec = endpoint.build_request(self, prefix, self._benchmark_rng)
            if spec.method == "GET":
                context = self.client.get(
                    spec.path, params=spec.params, headers=headers,
                    name=endpoint.label, catch_response=True
                )
            else:
                context = self.client.post(
                    spec.path, params=spec.params, json=spec.json_body,
                    headers=headers, name=endpoint.label, catch_response=True
                )
            with context as response:
                if response.status_code != 200:
                    response.failure(f"unexpected status {response.status_code}")
                else:
                    response.success()

    BenchmarkUser.host = host
    BenchmarkUser.__name__ = (
        f"{endpoint.purpose.title()}{endpoint.version.upper()}{prefix.title()}User"
    )
    return BenchmarkUser


def _percentile(total: object, value: float) -> float:
    if not getattr(total, "num_requests", 0):
        return 0.0
    return float(total.get_response_time_percentile(value) or 0.0)


def capture(
    environment: Environment,
    endpoint: Endpoint,
    prefix: str,
    users: int,
    duration: float,
) -> StageResult:
    total = environment.stats.total
    requests = int(total.num_requests)
    failures = int(total.num_failures)
    return StageResult(
        purpose=endpoint.purpose,
        version=endpoint.version,
        endpoint=endpoint.label,
        prefix=prefix,
        users=users,
        duration_seconds=duration,
        requests=requests,
        failures=failures,
        failure_rate_pct=(failures / requests * 100.0) if requests else 0.0,
        rps=float(total.total_rps or 0),
        mean_ms=float(total.avg_response_time or 0),
        median_ms=float(total.median_response_time or 0),
        p90_ms=_percentile(total, .90),
        p95_ms=_percentile(total, .95),
        p99_ms=_percentile(total, .99),
        minimum_ms=float(total.min_response_time or 0),
        maximum_ms=float(total.max_response_time or 0),
    )


def saturation_reasons(
    current: StageResult,
    previous: StageResult | None,
    args: argparse.Namespace,
) -> list[str]:
    reasons: list[str] = []
    if current.requests == 0:
        reasons.append("no completed requests")
    if current.failure_rate_pct > args.max_failure_rate:
        reasons.append(
            f"failure rate {current.failure_rate_pct:.2f}% > "
            f"{args.max_failure_rate:.2f}%"
        )
    if args.max_p95_ms and current.p95_ms > args.max_p95_ms:
        reasons.append(f"p95 {current.p95_ms:.1f}ms > {args.max_p95_ms:.1f}ms")
    if previous and previous.rps > 0:
        rps_gain = (current.rps - previous.rps) / previous.rps
        latency_ratio = (
            current.p95_ms / previous.p95_ms if previous.p95_ms else math.inf
        )
        if (
            rps_gain < args.plateau_rps_gain
            and latency_ratio >= args.latency_jump_ratio
        ):
            reasons.append(
                f"RPS gain {rps_gain:.1%} with p95 change {latency_ratio:.2f}x"
            )
        if current.rps < previous.rps * .95 and current.p95_ms >= previous.p95_ms:
            reasons.append("RPS fell while concurrency and latency increased")
    return reasons


def progress(message: str) -> None:
    print(f"[load-test] {message}", flush=True)


def sleep_with_progress(seconds: float, activity: str) -> None:
    if seconds <= 0:
        return
    deadline = time.monotonic() + seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        progress(f"{activity}; approximately {remaining:.0f}s remaining")
        gevent.sleep(min(10.0, remaining))


def run_one(
    endpoint: Endpoint,
    prefix: str,
    args: argparse.Namespace,
    headers: dict[str, str],
) -> list[StageResult]:
    user_class = make_user_class(
        args.host.rstrip("/"), endpoint, prefix, args.request_interval,
        args.request_timeout, headers, args.seed
    )
    environment = Environment(user_classes=[user_class])
    runner = environment.create_local_runner()
    results: list[StageResult] = []
    users = args.initial_users
    progress(f"starting {endpoint.label} for prefix {prefix}")
    try:
        while users <= args.max_users:
            progress(
                f"{endpoint.label}/{prefix}: ramping to {users} concurrent users"
            )
            runner.start(user_count=users, spawn_rate=args.spawn_rate)
            sleep_with_progress(
                args.settle_seconds,
                f"{endpoint.label}/{prefix}: waiting for load to settle",
            )
            environment.stats.reset_all()
            progress(
                f"{endpoint.label}/{prefix}: measuring {users} users for "
                f"{args.stage_seconds:.0f}s"
            )
            started = time.monotonic()
            sleep_with_progress(
                args.stage_seconds,
                f"{endpoint.label}/{prefix}: measurement in progress",
            )
            result = capture(
                environment, endpoint, prefix, users, time.monotonic() - started
            )
            reasons = saturation_reasons(
                result, results[-1] if results else None, args
            )
            result.saturated = bool(reasons)
            result.saturation_reasons = "; ".join(reasons)
            results.append(result)
            progress(
                f"{endpoint.label}/{prefix}: users={users}, "
                f"rps={result.rps:.2f}, p95={result.p95_ms:.1f}ms, "
                f"failures={result.failure_rate_pct:.2f}%"
            )
            if result.saturated:
                progress(
                    f"{endpoint.label}/{prefix}: saturation detected: "
                    f"{result.saturation_reasons}"
                )
                break
            next_users = max(users + 1, math.ceil(users * args.ramp_factor))
            if next_users > args.max_users:
                progress(
                    f"{endpoint.label}/{prefix}: safety ceiling of "
                    f"{args.max_users} users reached without saturation"
                )
                break
            users = next_users
    finally:
        runner.quit()
        progress(f"finished {endpoint.label} for prefix {prefix}")
    return results


def parse_headers(values: Sequence[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        if ":" not in value:
            raise ValueError(f"invalid header {value!r}; expected 'Name: value'")
        name, content = value.split(":", 1)
        result[name.strip()] = content.strip()
    return result


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--host", required=True)
    parser.add_argument(
        "--prefix", action="append", choices=all_prefixes(),
        help="Only test selected prefix(es); default is every ConceptPrefix"
    )
    parser.add_argument("--initial-users", type=int, default=5)
    parser.add_argument("--ramp-factor", type=float, default=2.0)
    parser.add_argument("--max-users", type=int, default=1280)
    parser.add_argument("--spawn-rate", type=float, default=50.0)
    parser.add_argument("--settle-seconds", type=float, default=10.0)
    parser.add_argument("--stage-seconds", type=float, default=60.0)
    parser.add_argument("--cooldown-seconds", type=float, default=15.0)
    parser.add_argument("--request-interval", type=float, default=0.0)
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument("--max-failure-rate", type=float, default=1.0)
    parser.add_argument("--max-p95-ms", type=float, default=2000.0)
    parser.add_argument("--latency-jump-ratio", type=float, default=1.75)
    parser.add_argument("--plateau-rps-gain", type=float, default=.10)
    parser.add_argument("--seed", type=int, default=20260730)
    parser.add_argument("--header", action="append", default=[])
    parser.add_argument("--output-dir", type=Path, default=Path("benchmark-results"))


def validate_args(args: argparse.Namespace) -> None:
    if args.initial_users < 1 or args.max_users < args.initial_users:
        raise ValueError("user limits must be positive and max >= initial")
    if args.ramp_factor <= 1:
        raise ValueError("--ramp-factor must be greater than 1")
    if args.stage_seconds <= 0 or args.settle_seconds < 0:
        raise ValueError("stage duration must be positive")


def write_raw_results(results: Sequence[StageResult], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = [asdict(result) for result in results]
    with (output_dir / "raw-statistics.csv").open(
        "w", newline="", encoding="utf-8"
    ) as handle:
        writer = csv.DictWriter(
            handle, fieldnames=[field.name for field in fields(StageResult)]
        )
        writer.writeheader()
        writer.writerows(rows)
    (output_dir / "raw-statistics.json").write_text(
        json.dumps(rows, indent=2), encoding="utf-8"
    )
    progress(f"wrote {len(rows)} raw statistic rows to {output_dir.resolve()}")


def run_suite(
    endpoints: Sequence[Endpoint],
    args: argparse.Namespace,
) -> list[StageResult]:
    validate_args(args)
    headers = parse_headers(args.header)
    prefixes = tuple(args.prefix) if args.prefix else all_prefixes()
    results: list[StageResult] = []
    combinations = [(endpoint, prefix) for endpoint in endpoints for prefix in prefixes]
    progress(
        f"running {len(combinations)} endpoint/prefix combinations sequentially"
    )
    for index, (endpoint, prefix) in enumerate(combinations):
        progress(
            f"combination {index + 1}/{len(combinations)}: "
            f"{endpoint.label}/{prefix}"
        )
        results.extend(run_one(endpoint, prefix, args, headers))
        if index + 1 < len(combinations):
            sleep_with_progress(
                args.cooldown_seconds,
                "cooling down before the next endpoint/prefix combination",
            )
    write_raw_results(results, args.output_dir)
    return results
