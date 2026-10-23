-- A sos (provider 2) steward register 500 whose variant 5000 keys its panel on the
-- pinned `sosvar` and times its rows by `sosdatum`, which carries no pin: a reslug
-- would dangle the time key as it would an entity key.
INSERT INTO register (register_id, provider_id, slug, name) VALUES (500, 2, 'dors', 'Dodsorsaker');
INSERT INTO variable (register_id, provider_key, name, source_name, slug)
VALUES (500, CAST('LOPNR' AS TEXT), 'Lopnummer', 'Lopnummer', 'sosvar');
INSERT INTO variable (register_id, provider_key, name, source_name, slug)
VALUES (500, CAST('DODSDAT' AS TEXT), 'Dodsdatum', 'Dodsdatum', 'sosdatum');
INSERT INTO register_variant (register_variant_id, register_id, slug, name, panel_entity_key, panel_time_key, panel_time_grain)
VALUES (5000, 500, 'grund', 'G', 'sosvar', 'sosdatum', 'row');
INSERT INTO variable_state (state_id, variable_id, register_variant_id, valid_from, valid_to, data_type, delivery_column_name)
SELECT (SELECT COALESCE(MAX(state_id), 0) + 1 FROM variable_state), variable_id, 5000, '0001-01-01', '9999-12-31', 'int', 'Lopnr' FROM variable WHERE slug = 'sosvar';
INSERT INTO variable_state (state_id, variable_id, register_variant_id, valid_from, valid_to, data_type, delivery_column_name)
SELECT (SELECT COALESCE(MAX(state_id), 0) + 1 FROM variable_state), variable_id, 5000, '0001-01-01', '9999-12-31', 'int', 'Dodsdat' FROM variable WHERE slug = 'sosdatum';
