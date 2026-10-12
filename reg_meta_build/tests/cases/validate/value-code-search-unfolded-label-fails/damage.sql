-- An owned value code indexed under its own code_id but with its raw label: the
-- contentless index reads back no text and its rowids match the source's, but the
-- reader matches folded queries, so 'Övrigt' (indexed as övrigt) is unreachable by
-- 'ovrigt'. Fails if the value_code_fts check stops at rowid parity, or its probe
-- skips non-ASCII labels.
INSERT INTO value_code (code_id, code, label, mapping_count) VALUES (10, '9', 'Övrigt', 1);
INSERT INTO value_code_fts(rowid, label) VALUES (10, 'Övrigt');
