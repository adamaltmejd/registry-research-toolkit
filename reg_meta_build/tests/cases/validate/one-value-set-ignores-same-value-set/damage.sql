-- Value set 1 again on kON in the same window: one coding, no conflict.
-- The first side is fixture state 1 (Kon, variant 10, 2020-2021, value set 1).
INSERT INTO variable_state (variable_id, register_variant_id, valid_from, valid_to,
    delivery_column_name, value_set_id, value_set_version_label)
VALUES (1, 10, '2020-01-01', '2021-12-31', 'kON', 1, 'bound-inject');
