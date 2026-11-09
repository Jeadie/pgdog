#!/bin/bash
# Runs benches/perf/bencher.sh for two PgDog builds (base and head) on the same
# machine, interleaved per suite (base, head, base, head, ...), so that the
# spread between repeated runs of the same build is visible next to the
# base-vs-head difference. Used by .github/workflows/bench-gh.yml on
# GitHub-hosted runners; must run as root (bencher.sh disables ASLR).
#
# Usage: bench-gh.sh <base-bin-dir> <head-bin-dir> <out-dir> <rounds> <suite>...
#
# Each bin dir holds `pgdog` and, for the microbench suite, `pgdog-microbench`.
# Results land in <out-dir>/<side>-<round>/<suite>.json (Bencher Metric Format),
# with the raw pgbench / divan output next to them.
set -euo pipefail

if [ "$#" -lt 5 ]; then
    echo "usage: $0 <base-bin-dir> <head-bin-dir> <out-dir> <rounds> <suite>..." >&2
    exit 2
fi

base_dir=$(realpath "$1")
head_dir=$(realpath "$2")
mkdir -p "$3"
out_dir=$(realpath "$3")
rounds=$4
shift 4

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "${SCRIPT_DIR}/../.."

export BENCH_GUEST=0
failed=0

for suite in "$@"; do
    for round in $(seq 1 "${rounds}"); do
        for side in base head; do
            bin_dir=${base_dir}
            if [ "${side}" = head ]; then
                bin_dir=${head_dir}
            fi
            run_dir="${out_dir}/${side}-${round}"
            mkdir -p "${run_dir}"
            echo "::group::${suite} ${side} round ${round}"
            start=$(date +%s)
            if PGDOG_BIN="${bin_dir}/pgdog" PGDOG_MICROBENCH_BIN="${bin_dir}/pgdog-microbench" \
                bash benches/perf/bencher.sh "${suite}" > "${run_dir}/${suite}.json" 2> "${run_dir}/${suite}.log"; then
                cat "${run_dir}/${suite}.json"
            else
                echo "::warning::${suite} ${side} round ${round} failed, see ${suite}.log in the results artifact"
                rm -f "${run_dir}/${suite}.json"
                failed=1
            fi
            tail -n 30 "${run_dir}/${suite}.log" || true
            echo "took $(($(date +%s) - start))s"
            # bencher.sh writes its raw output into the working directory.
            for f in "${suite}.warmup.txt" "${suite}.measured.txt" microbench_out.txt; do
                if [ -e "${f}" ]; then
                    mv -f "${f}" "${run_dir}/${f}"
                fi
            done
            echo "::endgroup::"
        done
    done
done

exit "${failed}"
