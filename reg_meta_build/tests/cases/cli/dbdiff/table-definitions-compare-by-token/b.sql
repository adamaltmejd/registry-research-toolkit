-- t_unique's constraint moved to b, t_spacing reindented, doc_fts's
-- tokenizer changed.
CREATE TABLE t_unique (a INTEGER, b INTEGER, UNIQUE(b));
CREATE TABLE t_spacing   (a   INTEGER,
      b    INTEGER);
CREATE VIRTUAL TABLE doc_fts USING fts5(body, tokenize='porter');
