-- A code-less twin of state 1 spelled `kON`: the consumer folds column casing
-- (py_lower), so the guard must too.
INSERT INTO variable_state (variable_id, register_variant_id, valid_from, valid_to,
    delivery_column_name, value_set_id, value_set_version_label)
VALUES (1, 10, '2020-01-01', '2021-12-31', 'kON', NULL, 'codeless-fold-inject');
