# Cross-shard ORDER BY benchmark

Measures latency of cross-shard `SELECT ... ORDER BY` through PgDog. Queries
with numeric keys take the streaming k-way merge path; queries ordered by a
text column fall back to full buffering and serve as a control.

`run.sh`:

1. Starts one Postgres container (`postgres:17`) hosting `SHARDS` databases
   (`shard_0`, `shard_1`, ...). Only Docker is needed on the host; `psql` and
   `pgbench` run inside the container.
2. Loads `ROWS` synthetic rows per shard (`generate_series`, fixed seed) into
   `bench(id bigint, score float8, bucket int, name text)` and indexes
   `score`, `bucket` and `name`. Loading happens before any timing.
3. Builds PgDog with `cargo build --release` (unless `PGDOG_BIN` is set) and
   starts it with a generated sharded config in `results/<run>/`.
4. For each query in `queries/` and each protocol in `MODES`, runs `WARMUP`
   unmeasured transactions, then `TRANSACTIONS` measured ones with
   `pgbench --log`.
5. Prints sample count, P50, P99, P99.9 and max latency, plus PgDog's peak
   RSS while the query ran. Percentiles are nearest-rank over every logged
   transaction.
6. Stops PgDog and removes the container on exit.

| Query | ORDER BY | Path |
|---|---|---|
| `merge_limit_id` | `id LIMIT 10` | streaming merge |
| `merge_limit_offset_score` | `score DESC LIMIT 100 OFFSET 50` | streaming merge |
| `merge_bucket_score` | `score`, no limit, `ROWS/BUCKETS` rows per shard | streaming merge |
| `control_limit_name` | `name LIMIT 10` (text) | full buffer |
| `control_bucket_name` | `name`, no limit, same rows as `merge_bucket_score` | full buffer |

pgbench measures total query latency. The streaming merge mainly improves
time to first row and memory, so read `peak_rss_mb` alongside latency.

## Running

```sh
bash integration/bench_merge/run.sh                       # defaults
SHARDS=2 ROWS=10000 TRANSACTIONS=1000 WARMUP=100 \
  bash integration/bench_merge/run.sh                     # quick smoke run
```

| Variable | Default | Meaning |
|---|---|---|
| `SHARDS` | 2 | shard databases |
| `ROWS` | 1000000 | rows per shard |
| `BUCKETS` | 100 | `bucket` cardinality; unlimited queries return `ROWS/BUCKETS` rows per shard |
| `TRANSACTIONS` | 10000 | measured samples per query and protocol (10,000 leaves 10 samples above P99.9) |
| `WARMUP` | 500 | unmeasured transactions per query and protocol |
| `CLIENTS` | 4 | pgbench clients (and threads) |
| `MODES` | `simple extended` | pgbench `-M` protocols |
| `QUERIES` | all | space-separated names from `queries/` |
| `PGDOG_BIN` | build | PgDog binary to test |
| `KEEP_DB` | 0 | 1 keeps the container and data for the next run, and reuses it if present |
| `PG_IMAGE`, `PG_PORT`, `PGDOG_PORT`, `PGDOG_WORKERS` | `postgres:17`, 15432, 16432, 4 | |

Output (config, PgDog log, raw pgbench logs, `summary.txt`) goes to
`integration/bench_merge/results/<run>/`.

## A/B

Build both binaries the same way, then run them against the same container and
data. Run each binary twice: a difference smaller than the gap between two runs
of the same binary is noise.

```sh
BASE=$(bash integration/bench_merge/build_ref.sh <base-ref> | tail -1)
HEAD=$(bash integration/bench_merge/build_ref.sh <head-ref> | tail -1)

for bin in "$BASE" "$HEAD" "$BASE" "$HEAD"; do
  KEEP_DB=1 PGDOG_BIN="$bin" bash integration/bench_merge/run.sh
done
docker rm -f pgdog-bench-merge
```

The first run loads data; the rest reuse it.
