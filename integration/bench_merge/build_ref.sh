#!/bin/bash
#
# Build a release PgDog binary for a git ref, for A/B runs of run.sh.
#
#   bash integration/bench_merge/build_ref.sh <git-ref>
#
# Prints the binary path (target/bench_merge/pgdog-<sha>) on the last line.
# Builds share one target dir, so the second build reuses compiled dependencies.
#
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/../.." && pwd)
REF=${1:?usage: build_ref.sh <git-ref>}

SHA=$(git -C "${REPO_ROOT}" rev-parse --short=8 "${REF}^{commit}")
OUT_DIR="${REPO_ROOT}/target/bench_merge"
BIN="${OUT_DIR}/pgdog-${SHA}"
mkdir -p "${OUT_DIR}"

if [ ! -x "${BIN}" ]; then
    SRC="${OUT_DIR}/src-${SHA}"
    git -C "${REPO_ROOT}" worktree add --detach "${SRC}" "${SHA}" >&2
    trap 'git -C "${REPO_ROOT}" worktree remove --force "${SRC}"' EXIT
    (cd "${SRC}" && CARGO_TARGET_DIR="${OUT_DIR}/target" cargo build --release -p pgdog >&2)
    cp "${OUT_DIR}/target/release/pgdog" "${BIN}"
fi

echo "${BIN}"
