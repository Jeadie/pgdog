# LOG.md
Tinkering with pgdog.

## Broadcast Tables
Inspired by some [experimentation](https://github.com/spiceai/spiceai/issues/10081) I did for optimising [distributed accelerations](https://docs.spice.ai/docs/enterprise/features/distributed-accelerations) for Spice.ai's multi-node cluster product, support sharded pgdog writing

## Vector Clustering
Pgdog can shard by vector centroids.
```toml
[[databases]]
name = "vectors"
host = "127.0.0.1"
port = 5432
database_name = "shard_0"
shard = 0

[[databases]]
name = "vectors"
host = "localhost"
port = 5432
database_name = "shard_1"
shard = 1

[[databases]]
name = "vectors"
host = "localhost"
port = 5432
database_name = "shard_2"
shard = 2

[[sharded_tables]]
database = "vectors"
name = "embeddings"
column = "embedding"
data_type = "vector"
centroids_path = "examples/pgvector/centroids.json"
centroid_probes = 3
```
A vector search like SQL query can be sharded
```sql
SELECT id, embedding
FROM embeddings
WHERE category = 'books' AND active = true
ORDER BY embedding <-> $1  -- pgvector notation
LIMIT 10;
```
The shards holding the `centroid_probes` closest centroid are queried. Pgdog buffers the `centroid_shards x 10` results, sorts and returns the first 10.

There is a tradeoff in `centroid_probes` and recall. More centroids will increase the number of shards queried, and more work within pgdog. Less centroids risks more similar results being excluded. The latter is more significant when filter predicates are introduced too.

It would be ideal to k-way merge from these shards and return early after the first N results (accounting for `OFFSET M`). This would reduce the cost on pgdog of increasing `centroid_probes`, and if the shard query gets cancelled (or possibly a cursor), the cost on the postgres shards.

### Implementation
1. Update, or make new similar to `Buffer` that can k-way merge. Will require shard id being sent through (or similar; unique id for k-way merge).
2. Use it in `MultiShard::handle_command_complete` & `MultiShard::handle_data_row`. Need to handle early exit. Might need to update how we handle basic for-loop of sharding servers (```rust for server in &mut self.servers```).
