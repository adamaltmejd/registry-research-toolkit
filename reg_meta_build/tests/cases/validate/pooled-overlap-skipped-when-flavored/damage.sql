-- A pooled twin of state 1 (same window, column and value set): the global guard
-- fails it, a flavored overlay skips the guard.
INSERT INTO variable_state (state_id, variable_id, register_variant_id, valid_from, valid_to,
    delivery_column_name, data_type, data_length, value_set_id, value_set_version_label, pooled)
VALUES ((SELECT COALESCE(MAX(state_id), 0) + 1 FROM variable_state), 1, 10, '2020-01-01', '2021-12-31', 'Kon', 'int', '1', 1, 'pooled-inject', 1);
