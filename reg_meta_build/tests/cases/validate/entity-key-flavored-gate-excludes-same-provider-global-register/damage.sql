-- A sos (provider 2) register 500 whose variant 5000 keys its panel on `sosvar`
-- (source_id 500.LOPNR).
INSERT INTO register (register_id, provider_id, slug, name) VALUES (500, 2, 'dors', 'Dodsorsaker');
INSERT INTO variable (register_id, provider_key, name, slug)
VALUES (500, CAST('LOPNR' AS TEXT), 'Lopnummer', 'sosvar');
INSERT INTO register_variant (register_variant_id, register_id, slug, name, panel_entity_key)
VALUES (5000, 500, 'grund', 'G', 'sosvar');
INSERT INTO variable_state (variable_id, register_variant_id, valid_from, valid_to, data_type, delivery_column_name)
SELECT variable_id, 5000, '0001-01-01', '9999-12-31', 'int', 'Lopnr' FROM variable WHERE slug = 'sosvar';

-- A second sos register 501 (a global-base register sharing the steward's provider)
-- whose variant keys on `globalk` (source_id 501.GLOBALK).
INSERT INTO register (register_id, provider_id, slug, name) VALUES (501, 2, 'global-base', 'GlobalBase');
INSERT INTO variable (register_id, provider_key, name, slug)
VALUES (501, CAST('GLOBALK' AS TEXT), 'GlobalKey', 'globalk');
INSERT INTO register_variant (register_variant_id, register_id, slug, name, panel_entity_key)
VALUES (5010, 501, 'base', 'B', 'globalk');
INSERT INTO variable_state (variable_id, register_variant_id, valid_from, valid_to, data_type, delivery_column_name)
SELECT variable_id, 5010, '0001-01-01', '9999-12-31', 'int', 'Globalk' FROM variable WHERE slug = 'globalk';
