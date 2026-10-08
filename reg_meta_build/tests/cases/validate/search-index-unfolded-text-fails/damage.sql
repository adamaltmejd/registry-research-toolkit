-- A classification row indexed under its id but with its raw text: the reader
-- matches folded queries, so unfolded text is unreachable by search.
INSERT INTO classification (id, short_name, name, slug)
VALUES (1, 'ALPHA', 'Straße nomenclature', 'alpha');
INSERT INTO classification_fts(rowid, short_name, name, name_en, description)
VALUES (1, 'ALPHA', 'Straße nomenclature', NULL, NULL);
