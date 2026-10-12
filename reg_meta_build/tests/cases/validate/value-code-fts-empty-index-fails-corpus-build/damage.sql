-- The value-code search index stays but holds no rows, which a corpus build must
-- reject. The index is contentless, which refuses a plain DELETE; FTS5's
-- 'delete-all' command empties it.
INSERT INTO value_code_fts(value_code_fts) VALUES ('delete-all');
