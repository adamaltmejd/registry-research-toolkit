-- A live predecessor whose successor slug names no classification.
INSERT INTO classification (short_name, name, slug) VALUES ('SSYK1996', 'SSYK1996', 'ssyk1996');
INSERT INTO classification_replaced_by (predecessor_slug, successor_slug, effective_year, note)
VALUES ('ssyk1996', 'no-such-slug-9999', 2020, 'derived:vintage_chain');
