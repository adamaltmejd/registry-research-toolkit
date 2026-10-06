-- One classification row, indexed under an id that names no classification: the
-- index and table counts agree, but the reader's join finds nothing by name.
INSERT INTO classification (id, short_name, name, slug)
VALUES (1, 'ALPHA', 'Alpha nomenclature', 'alpha');
INSERT INTO classification_fts(rowid, short_name, name, name_en, description)
VALUES (99, 'ALPHA', 'Alpha nomenclature', NULL, NULL);
