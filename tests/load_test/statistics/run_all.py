"""Run every non-data benchmark sequentially and build the aggregate report."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from load_test.statistics.benchmark import (
    StageResult,
    add_common_arguments,
    progress,
    run_suite,
    sleep_with_progress,
)
from load_test.statistics.endpoints import ALL_SUITES


def summaries(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    keys = ["purpose", "version", "endpoint", "prefix"]
    for key, group in frame.groupby(keys, sort=True):
        ordered = group.sort_values("users")
        saturated = ordered[ordered["saturated"]]
        if saturated.empty:
            sustainable = ordered.iloc[-1]
            first_saturated: int | None = None
            ceiling_reached = True
        else:
            first = saturated.iloc[0]
            before = ordered[ordered["users"] < first["users"]]
            sustainable = before.iloc[-1] if not before.empty else first
            first_saturated = int(first["users"])
            ceiling_reached = False
        sustainable_users = (
            0
            if not saturated.empty
            and ordered[ordered["users"] < saturated.iloc[0]["users"]].empty
            else int(sustainable["users"])
        )
        p95 = float(sustainable["p95_ms"])
        rps = float(sustainable["rps"])
        rows.append({
            **dict(zip(keys, key)),
            "maximum_sustainable_users": sustainable_users,
            "first_saturated_users": first_saturated,
            "capacity_not_found_before_safety_ceiling": ceiling_reached,
            "sustainable_rps": rps,
            "p95_at_sustainable_ms": p95,
            "p99_at_sustainable_ms": float(sustainable["p99_ms"]),
            "failure_rate_at_sustainable_pct": float(
                sustainable["failure_rate_pct"]
            ),
            "efficiency_rps_per_p95_second": rps * 1000 / p95 if p95 else 0,
        })
    summary = pd.DataFrame(rows)
    summary["relative_efficiency_score"] = 0.0
    for _, indexes in summary.groupby("purpose").groups.items():
        part = summary.loc[indexes]
        max_rps = max(float(part["sustainable_rps"].max()), 1e-9)
        positive = part.loc[part["p95_at_sustainable_ms"] > 0, "p95_at_sustainable_ms"]
        best_p95 = float(positive.min()) if not positive.empty else 1.0
        throughput = part["sustainable_rps"] / max_rps
        latency = best_p95 / part["p95_at_sustainable_ms"].replace(0, float("inf"))
        reliability = 1 - part["failure_rate_at_sustainable_pct"] / 100
        summary.loc[indexes, "relative_efficiency_score"] = (
            100 * (throughput * latency).clip(lower=0).pow(.5) * reliability
        )
    return summary


def plot_lines(
    frame: pd.DataFrame, metric: str, ylabel: str, output: Path
) -> None:
    figure, axis = plt.subplots(figsize=(14, 8))
    for (endpoint, prefix), data in frame.groupby(["endpoint", "prefix"]):
        ordered = data.sort_values("users")
        axis.plot(
            ordered["users"], ordered[metric], marker="o",
            label=f"{endpoint} / {prefix}", alpha=.8
        )
    axis.set_xscale("log", base=2)
    axis.set_xlabel("Concurrent users")
    axis.set_ylabel(ylabel)
    axis.set_title(f"{ylabel} by load, endpoint version, and prefix")
    axis.grid(alpha=.25)
    axis.legend(fontsize=7, ncol=3)
    figure.tight_layout()
    figure.savefig(output, dpi=160)
    plt.close(figure)


def plot_bar(
    frame: pd.DataFrame, index: str, columns: str, values: str,
    ylabel: str, title: str, output: Path
) -> None:
    pivot = frame.pivot_table(index=index, columns=columns, values=values, aggfunc="mean")
    axis = pivot.plot.bar(figsize=(14, 7))
    axis.set_ylabel(ylabel)
    axis.set_title(title)
    axis.grid(axis="y", alpha=.25)
    axis.figure.tight_layout()
    axis.figure.savefig(output, dpi=160)
    plt.close(axis.figure)


def markdown_table(frame: pd.DataFrame) -> str:
    values = frame.fillna("").astype(str)
    header = "| " + " | ".join(values.columns) + " |"
    separator = "| " + " | ".join("---" for _ in values.columns) + " |"
    rows = [
        "| " + " | ".join(row) + " |"
        for row in values.itertuples(index=False, name=None)
    ]
    return "\n".join([header, separator, *rows])


def write_report(summary: pd.DataFrame, output_dir: Path) -> None:
    version_comparison = (
        summary.groupby(["purpose", "version"], as_index=False)
        .agg(
            mean_efficiency_score=("relative_efficiency_score", "mean"),
            mean_sustainable_rps=("sustainable_rps", "mean"),
            mean_p95_ms=("p95_at_sustainable_ms", "mean"),
            mean_maximum_users=("maximum_sustainable_users", "mean"),
        )
        .sort_values(["purpose", "mean_efficiency_score"], ascending=[True, False])
    )
    prefix_comparison = (
        summary.groupby("prefix", as_index=False)
        .agg(
            mean_sustainable_rps=("sustainable_rps", "mean"),
            mean_p95_ms=("p95_at_sustainable_ms", "mean"),
            mean_maximum_users=("maximum_sustainable_users", "mean"),
        )
        .sort_values("mean_p95_ms")
    )
    version_comparison.to_csv(output_dir / "version-comparison.csv", index=False)
    prefix_comparison.to_csv(output_dir / "prefix-speed-comparison.csv", index=False)
    pd.DataFrame([{
        "maximum_observed_sustainable_users": int(
            summary["maximum_sustainable_users"].max()
        ),
        "endpoint_prefix_combinations_tested": len(summary),
        "combinations_saturated": int(
            (~summary["capacity_not_found_before_safety_ceiling"]).sum()
        ),
        "combinations_reaching_safety_ceiling": int(
            summary["capacity_not_found_before_safety_ceiling"].sum()
        ),
    }]).to_csv(output_dir / "overall-capacity.csv", index=False)

    best_versions = version_comparison.groupby("purpose", sort=True).first().reset_index()
    fastest_prefixes = prefix_comparison.head(5)
    lines = [
        "# Load-test comparison",
        "",
        "Maximum users means the last sustainable stage before the first saturation "
        "signal. A `true` safety-ceiling flag means saturation was not observed, so "
        "the reported capacity is a lower bound.",
        "",
        "## Most efficient version for each purpose",
        "",
        markdown_table(best_versions),
        "",
        "## Fastest prefixes (mean p95 across endpoints)",
        "",
        markdown_table(fastest_prefixes),
        "",
        "## Capacity by endpoint and prefix",
        "",
        markdown_table(
            summary[[
                "endpoint", "prefix", "maximum_sustainable_users",
                "first_saturated_users",
                "capacity_not_found_before_safety_ceiling",
                "sustainable_rps", "p95_at_sustainable_ms",
            ]].sort_values(["endpoint", "prefix"])
        ),
        "",
    ]
    (output_dir / "comparison-report.md").write_text(
        "\n".join(lines), encoding="utf-8"
    )


def generate_outputs(results: list[StageResult], output_dir: Path) -> None:
    progress(f"aggregating {len(results)} raw stage results")
    frame = pd.DataFrame(asdict(item) for item in results)
    summary = summaries(frame)
    frame.to_csv(output_dir / "all-raw-statistics.csv", index=False)
    summary.to_csv(output_dir / "endpoint-prefix-summary.csv", index=False)
    (output_dir / "endpoint-prefix-summary.json").write_text(
        json.dumps(json.loads(summary.to_json(orient="records")), indent=2),
        encoding="utf-8",
    )
    plot_lines(frame, "rps", "Requests per second", output_dir / "throughput-by-load.png")
    plot_lines(frame, "p95_ms", "p95 response time (ms)", output_dir / "p95-by-load.png")
    plot_bar(
        summary, "prefix", "endpoint", "maximum_sustainable_users",
        "Maximum sustainable concurrent users",
        "Maximum users for each endpoint and prefix",
        output_dir / "maximum-users-endpoint-prefix.png",
    )
    plot_bar(
        summary, "purpose", "version", "relative_efficiency_score",
        "Relative efficiency score",
        "Efficiency of endpoint versions for the same purpose",
        output_dir / "version-efficiency.png",
    )
    plot_bar(
        summary, "prefix", "endpoint", "p95_at_sustainable_ms",
        "p95 response time (ms)",
        "Query speed difference by prefix",
        output_dir / "prefix-query-speed.png",
    )
    write_report(summary, output_dir)
    progress(f"aggregate reports and diagrams written to {output_dir.resolve()}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_common_arguments(parser)
    args = parser.parse_args()
    root: Path = args.output_dir
    root.mkdir(parents=True, exist_ok=True)
    all_results: list[StageResult] = []
    suites = list(ALL_SUITES.items())
    for index, (name, endpoints) in enumerate(suites):
        progress(f"suite {index + 1}/{len(suites)}: starting {name}")
        args.output_dir = root / "raw" / name
        all_results.extend(run_suite(endpoints, args))
        progress(f"suite {index + 1}/{len(suites)}: completed {name}")
        if index + 1 < len(suites):
            sleep_with_progress(
                args.cooldown_seconds,
                "cooling down before the next endpoint suite",
            )
    progress("all endpoint suites completed; generating aggregate outputs")
    generate_outputs(all_results, root)


if __name__ == "__main__":
    main()
