-- The FTS table stays but its index is cleared (the _docsize shadow count drops
-- to 0), which a corpus build must reject.
INSERT INTO value_code_fts(value_code_fts) VALUES ('delete-all');
