-- A minted sos (provider 2) register whose only variable carries one state
-- (ids in the minted band, 2^62 + n).
INSERT INTO register (register_id, provider_id, slug, name) VALUES (4611686018427387905, 2, 'thr', 'Tandhälsoregistret');
INSERT INTO register_variant (register_variant_id, register_id, slug, name) VALUES (4611686018427387906, 4611686018427387905, '_default', '_default');
INSERT INTO variable (variable_id, register_id, provider_key, name, slug, is_sensitive, is_identifier)
VALUES (4611686018427387907, 4611686018427387905, 'X', 'X', 'x', 0, 0);
INSERT INTO variable_state (state_id, variable_id, register_variant_id, valid_from, valid_to, data_type, delivery_column_name, value_set_version_label)
VALUES (4611686018427387908, 4611686018427387907, 4611686018427387906, '2000-01-01', '2010-12-31', 'int', 'X', '');
INSERT INTO variable_alias (variable_id, register_variant_id, delivery_column_name)
VALUES (4611686018427387907, 4611686018427387906, 'X');
