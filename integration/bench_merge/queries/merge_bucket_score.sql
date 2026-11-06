-- Streaming merge: float key, no LIMIT. Returns every row of one bucket
-- from every shard (ROWS / BUCKETS rows per shard).
\set b random(0, :buckets - 1)
SELECT id, score FROM bench WHERE bucket = :b ORDER BY score;
