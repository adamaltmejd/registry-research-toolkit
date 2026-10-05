-- Two distinct value sets, both with a NULL column, on one window: they conflict.
INSERT INTO variable_state (variable_id, register_variant_id, valid_from, valid_to,
    delivery_column_name, value_set_id, value_set_version_label)
VALUES (1, 10, '2020-01-01', '2021-12-31', NULL, 1, 'null-bound-0'),
       (1, 10, '2020-01-01', '2021-12-31', NULL, 2, 'null-bound-1');
