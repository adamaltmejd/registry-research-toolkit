-- The same classification row, indexed the way derive indexes it: fold_search
-- text (case fold, NFKD, marks dropped; ß folds to ss) under the row's id.
INSERT INTO classification (short_name, name, slug)
VALUES ('ALPHA', 'Straße Ålder nomenclature', 'alpha');
INSERT INTO classification_fts(rowid, short_name, name, name_en, description)
SELECT id, 'alpha', 'strasse alder nomenclature', NULL, NULL
FROM classification WHERE short_name = 'ALPHA';
-- A classification without succession is its own one-edition chain (derive).
INSERT INTO classification_chain VALUES ('alpha', 0, 'alpha', NULL, 1);
