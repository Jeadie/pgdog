-- Streaming merge: float key, DESC, LIMIT + OFFSET.
SELECT id, score FROM bench ORDER BY score DESC LIMIT 100 OFFSET 50;
