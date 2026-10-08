-- A minted sos (provider 2) register with one variant and one variable that has
-- no variable_state row at all (ids in the minted band, 2^62 + n).
INSERT INTO register (register_id, provider_id, slug, name) VALUES (4611686018427387905, 2, 'lova', 'LOVA');
INSERT INTO register_variant (register_variant_id, register_id, slug, name) VALUES (4611686018427387906, 4611686018427387905, '_default', '_default');
INSERT INTO variable (variable_id, register_id, provider_key, name, slug, is_sensitive, is_identifier)
VALUES (4611686018427387907, 4611686018427387905, 'X', 'X', 'x', 0, 0);
-- The search rows derive would write for them.
INSERT INTO register_fts (rowid, register_id, name, purpose)
VALUES (4611686018427387905, 4611686018427387905, 'lova', NULL);
INSERT INTO variable_fts (rowid, register_id, provider_key, name, delivery_column_names)
VALUES (4611686018427387907, 4611686018427387905, 'x', 'x', '[]');
