# Adaptive load-test statistics

Run an individual endpoint family with `python -m`, for example:

```bash
PYTHONPATH=src:tests python -m load_test.statistics.auto_complete \
  --host http://localhost:8000 \
  --output-dir tests/load_test/statistics/results/auto-complete
```

Individual scripts test every `ConceptPrefix` by default, ramp each endpoint
version independently until a saturation signal is observed, and write only
`raw-statistics.csv` and `raw-statistics.json`. Use repeated `--prefix` options
for a smaller diagnostic run. Progress is printed to the terminal with flushed
output, including suite and prefix transitions, ramp levels, live measurement
countdowns, stage statistics, saturation reasons, cooldowns, and output paths.
Long stages emit a heartbeat approximately every 10 seconds so CI runners do
not appear frozen.

Run every load-testable endpoint except `/data` sequentially and create the
aggregate tables, report, and diagrams with:

```bash
PYTHONPATH=src:tests python -m load_test.statistics.run_all \
  --host http://localhost:8000 \
  --output-dir tests/load_test/statistics/results
```

The initial concurrency is 5 and doubles by default. Saturation is indicated by
the failure-rate or p95 limits, an RPS/latency plateau, or falling throughput.
`--max-users` is a safety ceiling; if it is reached first, the report marks the
capacity as a lower bound. Use `--help` to see timing, ramp, threshold, header,
and prefix controls.
