-- A code-less twin of state 1 (Kon, 2020-2021, value set 1) on the same window:
-- the global guard fails it, a flavored overlay skips the guard.
INSERT INTO variable_state (variable_id, register_variant_id, valid_from, valid_to,
    delivery_column_name, value_set_id, value_set_version_label)
VALUES (1, 10, '2020-01-01', '2021-12-31', 'Kon', NULL, 'codeless-inject');
