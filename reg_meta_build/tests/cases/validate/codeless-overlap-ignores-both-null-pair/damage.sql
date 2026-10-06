-- Two code-less states overlap on one synthetic column: symmetric, not flagged.
INSERT INTO variable_state (variable_id, register_variant_id, valid_from, valid_to,
    delivery_column_name, value_set_id, value_set_version_label)
VALUES (1, 10, '2020-01-01', '2021-12-31', 'SYNTH_NULL_COL', NULL, 'null-inject-0'),
       (1, 10, '2020-01-01', '2021-12-31', 'SYNTH_NULL_COL', NULL, 'null-inject-1');
