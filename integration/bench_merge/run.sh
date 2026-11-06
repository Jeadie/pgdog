#!/bin/bash
#
# Cross-shard ORDER BY benchmark.
#
# Starts one Postgres container hosting SHARDS databases (shard_0, shard_1, ...),
# loads synthetic rows into each, starts a release PgDog in front of them and
# runs each query in queries/*.sql through pgbench. Reports sample count and
# P50/P99/P99.9 latency per query and protocol, plus PgDog peak RSS.
#
# Usage:
#   bash integration/bench_merge/run.sh
#   PGDOG_BIN=/path/to/pgdog bash integration/bench_merge/run.sh   # A/B
#
# See README.md in this directory for every knob.
#
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/../.." && pwd)

SHARDS=${SHARDS:-2}
ROWS=${ROWS:-1000000}             # rows per shard
BUCKETS=${BUCKETS:-100}          # merge_bucket_score returns ROWS/BUCKETS rows per shard
TRANSACTIONS=${TRANSACTIONS:-10000} # samples per query per protocol
WARMUP=${WARMUP:-500}             # unmeasured transactions per query per protocol
CLIENTS=${CLIENTS:-4}
MODES=${MODES:-"simple extended"}
QUERIES=${QUERIES:-""}            # space-separated query names; empty = all
PG_IMAGE=${PG_IMAGE:-postgres:17}
PG_PORT=${PG_PORT:-15432}
PGDOG_PORT=${PGDOG_PORT:-16432}
PGDOG_WORKERS=${PGDOG_WORKERS:-4}
CONTAINER=${CONTAINER:-pgdog-bench-merge}
KEEP_DB=${KEEP_DB:-0}             # 1 = reuse/keep the container and its data
RUN_ID=${RUN_ID:-$(date +%Y%m%d-%H%M%S)}
RESULTS_DIR=${SCRIPT_DIR}/results # mounted into the container as /bench/results
OUT_DIR=${RESULTS_DIR}/${RUN_ID}

if [ $((TRANSACTIONS % CLIENTS)) -ne 0 ]; then
    echo "TRANSACTIONS (${TRANSACTIONS}) must be a multiple of CLIENTS (${CLIENTS})" >&2
    exit 1
fi

mkdir -p "${OUT_DIR}/logs"
PGDOG_PID=""
RSS_PID=""

cleanup() {
    [ -n "${RSS_PID}" ] && kill "${RSS_PID}" 2> /dev/null || true
    if [ -n "${PGDOG_PID}" ] && kill -0 "${PGDOG_PID}" 2> /dev/null; then
        kill -TERM "${PGDOG_PID}" 2> /dev/null || true
        wait "${PGDOG_PID}" 2> /dev/null || true
    fi
    if [ "${KEEP_DB}" != "1" ]; then
        docker rm -f "${CONTAINER}" > /dev/null 2>&1 || true
    fi
}
trap cleanup EXIT INT TERM

# psql/pgbench run inside the Postgres container; the host doesn't need them.
pg() { docker exec -i -e PGPASSWORD=pgdog "${CONTAINER}" "$@"; }

# ---------------------------------------------------------------------------
# 1. Postgres
# ---------------------------------------------------------------------------
if ! docker inspect "${CONTAINER}" > /dev/null 2>&1; then
    echo "Starting ${PG_IMAGE} as ${CONTAINER} on port ${PG_PORT}"
    docker run -d --name "${CONTAINER}" \
        --add-host host.docker.internal:host-gateway \
        -e POSTGRES_USER=pgdog -e POSTGRES_PASSWORD=pgdog -e POSTGRES_DB=pgdog \
        -p "127.0.0.1:${PG_PORT}:5432" \
        -v "${SCRIPT_DIR}/queries:/bench/queries:ro" \
        -v "${RESULTS_DIR}:/bench/results" \
        "${PG_IMAGE}" -c max_connections=200 -c shared_buffers=512MB > /dev/null
    LOAD_DATA=1
else
    echo "Reusing container ${CONTAINER} and its data"
    LOAD_DATA=0
fi

until pg pg_isready -U pgdog -h 127.0.0.1 > /dev/null 2>&1; do sleep 0.5; done
# The entrypoint restarts Postgres once after init; wait until it accepts queries.
until pg psql -U pgdog -h 127.0.0.1 -d pgdog -qAtc 'SELECT 1' > /dev/null 2>&1; do sleep 0.5; done

# ---------------------------------------------------------------------------
# 2. Synthetic data (before any timing)
# ---------------------------------------------------------------------------
if [ "${LOAD_DATA}" = "1" ]; then
    for ((s = 0; s < SHARDS; s++)); do
        echo "Loading ${ROWS} rows into shard_${s}"
        pg psql -U pgdog -h 127.0.0.1 -d pgdog -qc "CREATE DATABASE shard_${s}"
        pg psql -U pgdog -h 127.0.0.1 -d "shard_${s}" -v ON_ERROR_STOP=1 -q <<EOF
SELECT setseed(0.${s}1);
CREATE TABLE bench (
    id     BIGINT PRIMARY KEY,           -- globally unique: k * SHARDS + shard
    score  DOUBLE PRECISION NOT NULL,    -- uniform random in [0, 1)
    bucket INTEGER NOT NULL,             -- k % BUCKETS
    name   TEXT NOT NULL                 -- md5(id), for the text-key control
);
INSERT INTO bench
SELECT k * ${SHARDS} + ${s}, random(), k % ${BUCKETS}, md5((k * ${SHARDS} + ${s})::text)
FROM generate_series(0, ${ROWS} - 1) AS k;
CREATE INDEX ON bench (score);
CREATE INDEX ON bench (bucket);
CREATE INDEX ON bench (name);
VACUUM ANALYZE bench;
EOF
    done
fi

# ---------------------------------------------------------------------------
# 3. PgDog (release build)
# ---------------------------------------------------------------------------
if [ -z "${PGDOG_BIN:-}" ]; then
    echo "Building PgDog (release)"
    (cd "${REPO_ROOT}" && cargo build --release -p pgdog)
    PGDOG_BIN="${REPO_ROOT}/target/release/pgdog"
fi

{
    cat <<EOF
[general]
host = "127.0.0.1"
port = ${PGDOG_PORT}
workers = ${PGDOG_WORKERS}
default_pool_size = $((CLIENTS * 2))
EOF
    for ((s = 0; s < SHARDS; s++)); do
        cat <<EOF

[[databases]]
name = "bench"
host = "127.0.0.1"
port = ${PG_PORT}
database_name = "shard_${s}"
shard = ${s}
EOF
    done
    cat <<EOF

[[sharded_tables]]
database = "bench"
name = "bench"
column = "id"
data_type = "bigint"
EOF
} > "${OUT_DIR}/pgdog.toml"

cat > "${OUT_DIR}/users.toml" <<EOF
[[users]]
name = "pgdog"
database = "bench"
password = "pgdog"
EOF

echo "Starting ${PGDOG_BIN} on port ${PGDOG_PORT}"
RUST_LOG=${RUST_LOG:-warn} "${PGDOG_BIN}" \
    --config "${OUT_DIR}/pgdog.toml" \
    --users "${OUT_DIR}/users.toml" > "${OUT_DIR}/pgdog.log" 2>&1 &
PGDOG_PID=$!

# Clients connect from inside the container to PgDog on the host.
PGDOG_URL="postgres://pgdog:pgdog@host.docker.internal:${PGDOG_PORT}/bench"
until pg psql "${PGDOG_URL}" -qAtc 'SELECT 1' > /dev/null 2>&1; do
    kill -0 "${PGDOG_PID}" 2> /dev/null || { echo "PgDog exited:"; cat "${OUT_DIR}/pgdog.log"; exit 1; }
    sleep 0.2
done

# Sanity check: the query must see every shard.
total=$(pg psql "${PGDOG_URL}" -qAtc 'SELECT count(*) FROM bench')
if [ "${total}" != "$((ROWS * SHARDS))" ]; then
    echo "Expected $((ROWS * SHARDS)) rows through PgDog, got ${total}" >&2
    exit 1
fi

{
    echo "pgdog_bin:    ${PGDOG_BIN}"
    echo "pgdog_sha256: $(shasum -a 256 "${PGDOG_BIN}" | cut -d' ' -f1)"
    echo "shards=${SHARDS} rows_per_shard=${ROWS} buckets=${BUCKETS} clients=${CLIENTS}"
    echo "transactions=${TRANSACTIONS} warmup=${WARMUP} image=${PG_IMAGE}"
} | tee "${OUT_DIR}/summary.txt"

# ---------------------------------------------------------------------------
# 4. Benchmark
# ---------------------------------------------------------------------------

# Track PgDog's peak resident set size (KiB) while a query runs.
start_rss() {
    rm -f "${OUT_DIR}/rss"
    (
        peak=0
        while true; do
            rss=$(ps -o rss= -p "${PGDOG_PID}" | tr -d ' ')
            [ -n "${rss}" ] && [ "${rss}" -gt "${peak}" ] && peak=${rss} && echo "${peak}" > "${OUT_DIR}/rss"
            sleep 0.05
        done
    ) &
    RSS_PID=$!
}
stop_rss() {
    kill "${RSS_PID}" 2> /dev/null || true
    wait "${RSS_PID}" 2> /dev/null || true
    RSS_PID=""
}

# Nearest-rank percentiles over the pgbench per-transaction log (column 3, µs).
percentiles() {
    sort -n | awk '
        { v[NR] = $1 }
        function p(q,   i) { i = int(q * NR); if (i < q * NR) i++; if (i < 1) i = 1; return v[i] / 1000 }
        END { printf "%8d %9.3f %9.3f %9.3f %9.3f", NR, p(0.50), p(0.99), p(0.999), v[NR] / 1000 }'
}

if [ -z "${QUERIES}" ]; then
    QUERIES=$(cd "${SCRIPT_DIR}/queries" && ls *.sql | sed 's/\.sql$//')
fi

printf '\n%-26s %-9s %8s %9s %9s %9s %9s %10s\n' \
    query mode samples p50_ms p99_ms p99.9_ms max_ms peak_rss_mb | tee -a "${OUT_DIR}/summary.txt"

for query in ${QUERIES}; do
    for mode in ${MODES}; do
        common=(pgbench -n -M "${mode}" -c "${CLIENTS}" -j "${CLIENTS}" -D "buckets=${BUCKETS}"
            -f "/bench/queries/${query}.sql")
        pg "${common[@]}" -t $((WARMUP / CLIENTS + 1)) "${PGDOG_URL}" > /dev/null

        rm -f "${OUT_DIR}/logs/${query}_${mode}".*
        start_rss
        pg "${common[@]}" -t $((TRANSACTIONS / CLIENTS)) \
            -l --log-prefix="/bench/results/${RUN_ID}/logs/${query}_${mode}" "${PGDOG_URL}" \
            > "${OUT_DIR}/${query}_${mode}.pgbench.txt"
        stop_rss

        stats=$(cat "${OUT_DIR}/logs/${query}_${mode}".* | awk '{ print $3 }' | percentiles)
        rss_mb=$(awk '{ printf "%.1f", $1 / 1024 }' "${OUT_DIR}/rss" 2> /dev/null || echo "?")
        printf '%-26s %-9s %s %10s\n' "${query}" "${mode}" "${stats}" "${rss_mb}" | tee -a "${OUT_DIR}/summary.txt"
    done
done

echo
echo "Results in ${OUT_DIR}"
