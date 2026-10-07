-- A derived_from edge whose source slug names no classification.
INSERT INTO classification (short_name, name, slug) VALUES ('KS87-P', 'KS87-P', 'ks87-p');
INSERT INTO classification_derived_from (derived_slug, source_slug, note)
VALUES ('ks87-p', 'no-such-slug-9999', 'variant');
