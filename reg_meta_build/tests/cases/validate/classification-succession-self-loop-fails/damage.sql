-- A succession edge from a live classification to itself.
INSERT INTO classification (short_name, name, slug) VALUES ('SSYK2012', 'SSYK2012', 'ssyk2012');
INSERT INTO classification_replaced_by (predecessor_slug, successor_slug, effective_year, note)
VALUES ('ssyk2012', 'ssyk2012', 2020, 'derived:vintage_chain');
