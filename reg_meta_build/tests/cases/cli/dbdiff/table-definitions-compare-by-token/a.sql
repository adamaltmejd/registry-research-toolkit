CREATE TABLE t_unique (a INTEGER, b INTEGER, UNIQUE(a));
CREATE TABLE t_spacing (a INTEGER, b INTEGER);
CREATE VIRTUAL TABLE doc_fts USING fts5(body, tokenize='unicode61');
