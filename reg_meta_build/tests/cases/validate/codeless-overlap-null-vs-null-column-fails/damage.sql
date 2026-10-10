-- A code-bearing and a code-less state, both with a NULL column, on one window:
-- the same ambiguity without a header, so the guard fires.
INSERT INTO variable_state (state_id, variable_id, register_variant_id, valid_from, valid_to,
    delivery_column_name, value_set_id, value_set_version_label)
VALUES ((SELECT COALESCE(MAX(state_id), 0) + 1 FROM variable_state), 1, 10, '2020-01-01', '2021-12-31', NULL, 1, 'null-bound-set'),
       ((SELECT COALESCE(MAX(state_id), 0) + 2 FROM variable_state), 1, 10, '2020-01-01', '2021-12-31', NULL, NULL, 'null-bound-less');
