-- Control: text key falls back to full buffering + sort. No LIMIT, same
-- row count as merge_bucket_score.
\set b random(0, :buckets - 1)
SELECT id, name FROM bench WHERE bucket = :b ORDER BY name;
