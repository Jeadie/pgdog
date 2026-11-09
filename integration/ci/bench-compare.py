#!/usr/bin/env python3
"""Compare base vs head benchmark runs produced by integration/ci/bench-gh.sh.

Usage: bench-compare.py [--all] LABEL=RESULTS_DIR [LABEL=RESULTS_DIR ...]

Each RESULTS_DIR holds <side>-<round>/<suite>.json files in Bencher Metric
Format ({"benchmark": {"measure": {"value": N, ...}}}), where side is "base"
or "head". Prints a Markdown report to stdout.

For every benchmark/measure the report lists each run, the head-vs-base
difference of the means, and the A/A spread: the largest max-min range among
the runs of the same build (base runs against each other, head runs against
each other). A measure is only flagged when the base and head runs do not
overlap and the gap between them (closest base run to closest head run) is
larger than that spread; otherwise it is reported as within noise. With two
runs per side this is a coarse filter, not a statistical test.

Microbenchmark measures are summarized (flagged ones only) unless --all is
given, which also lists every measure.
"""

import json
import os
import re
import sys
from collections import defaultdict

# Measures where a larger value is better. Everything else (cpu-per-txn,
# ctx-switches-per-txn, latency, allocations, instructions, ...) is
# lower-is-better.
HIGHER_IS_BETTER = {"throughput"}

# Gaps smaller than this fraction of the base mean are float/rounding noise.
MIN_RELATIVE_GAP = 1e-4

RUN_DIR = re.compile(r"^(base|head)-(\d+)$")


def load(results_dir):
    """Returns {(benchmark, measure): {"base": [..], "head": [..]}} and the suites that failed."""
    values = defaultdict(lambda: {"base": [], "head": []})
    missing = []
    runs = []
    for entry in sorted(os.listdir(results_dir)):
        match = RUN_DIR.match(entry)
        if match and os.path.isdir(os.path.join(results_dir, entry)):
            runs.append((int(match.group(2)), match.group(1), entry))
    runs.sort()

    for _, side, entry in runs:
        run_path = os.path.join(results_dir, entry)
        logs = sorted(f[: -len(".log")] for f in os.listdir(run_path) if f.endswith(".log"))
        for suite in logs:
            json_path = os.path.join(run_path, suite + ".json")
            try:
                with open(json_path) as f:
                    data = json.load(f)
            except (OSError, ValueError):
                missing.append(f"{suite} ({entry})")
                continue
            for benchmark, measures in data.items():
                for measure, result in measures.items():
                    values[(benchmark, measure)][side].append(float(result["value"]))
    return values, missing


def fmt(value):
    if value == 0:
        return "0"
    magnitude = abs(value)
    if magnitude >= 1000:
        return f"{value:,.0f}"
    if magnitude >= 1:
        return f"{value:.4g}"
    return f"{value:.3g}"


def pct(delta, reference):
    if reference == 0:
        return "n/a"
    return f"{delta / reference * 100:+.2f}%"


def mean(xs):
    return sum(xs) / len(xs)


def compare(base, head, measure):
    """Returns (delta cell, spread cell, verdict, flagged)."""
    if not base or not head:
        return "n/a", "n/a", "missing runs", False
    base_mean, head_mean = mean(base), mean(head)
    delta = head_mean - base_mean
    delta_cell = pct(delta, base_mean)
    if len(base) < 2 or len(head) < 2:
        return delta_cell, "n/a", "no A/A (need 2+ runs per side)", False

    spread = max(max(base) - min(base), max(head) - min(head))
    spread_cell = pct(spread, base_mean).lstrip("+")
    # Distance between the closest base and head runs; <= 0 when they overlap.
    gap = min(head) - max(base) if delta > 0 else min(base) - max(head)
    if gap <= spread or gap <= abs(base_mean) * MIN_RELATIVE_GAP:
        return delta_cell, spread_cell, "within noise", False

    better = delta > 0 if measure in HIGHER_IS_BETTER else delta < 0
    return delta_cell, spread_cell, "**better**" if better else "**worse**", True


def table(rows):
    lines = [
        "| Benchmark | Measure | Base runs | Head runs | Δ mean | A/A spread | Verdict |",
        "|---|---|---|---|---:|---:|---|",
    ]
    lines.extend(rows)
    return "\n".join(lines)


def report(label, results_dir, show_all):
    values, missing = load(results_dir)
    out = [f"### {label}", ""]
    if not values:
        out.append(f"No results found in `{results_dir}`.")
        return "\n".join(out), 0

    e2e, micro = [], []
    flagged = 0
    for (benchmark, measure), sides in sorted(values.items()):
        delta, spread, verdict, is_flagged = compare(sides["base"], sides["head"], measure)
        flagged += is_flagged
        row = "| {} | {} | {} | {} | {} | {} | {} |".format(
            benchmark,
            measure,
            ", ".join(fmt(v) for v in sides["base"]) or "-",
            ", ".join(fmt(v) for v in sides["head"]) or "-",
            delta,
            spread,
            verdict,
        )
        # End-to-end suites are named "<suite>-v3"; everything else is a divan microbench.
        (e2e if benchmark.endswith("-v3") else micro).append((is_flagged, row))

    if e2e:
        out += ["**End-to-end (pgbench through PgDog)**", "", table([r for _, r in e2e]), ""]
    if micro:
        micro_flagged = [r for f, r in micro if f]
        out += [f"**Microbenchmarks**: {len(micro_flagged)} of {len(micro)} measures outside the A/A spread", ""]
        if micro_flagged:
            out += [table(micro_flagged), ""]
        if show_all:
            out += [
                "<details><summary>All microbenchmark measures</summary>",
                "",
                table([r for _, r in micro]),
                "",
                "</details>",
                "",
            ]
    if missing:
        out += ["Failed or missing runs: " + ", ".join(missing), ""]
    return "\n".join(out), flagged


def main(argv):
    args = argv[1:]
    show_all = "--all" in args
    args = [a for a in args if a != "--all"]
    if not args:
        print(__doc__, file=sys.stderr)
        return 2
    sections = []
    total = 0
    for arg in args:
        label, _, results_dir = arg.partition("=")
        if not results_dir:
            print(f"expected LABEL=DIR, got {arg!r}", file=sys.stderr)
            return 2
        text, flagged = report(label, results_dir, show_all)
        sections.append(text)
        total += flagged
    print(f"{total} measure(s) differ by more than the A/A spread.\n")
    print("\n".join(sections))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
