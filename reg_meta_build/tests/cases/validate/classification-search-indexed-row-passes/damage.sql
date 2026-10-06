-- The same classification row, indexed the way the writer indexes it.
INSERT INTO classification (short_name, name, slug)
VALUES ('ALPHA', 'Alpha nomenclature', 'alpha');
INSERT INTO classification_fts(classification_fts) VALUES ('rebuild');
