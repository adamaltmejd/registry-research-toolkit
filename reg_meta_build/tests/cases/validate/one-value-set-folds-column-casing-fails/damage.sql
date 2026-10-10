-- Value set 2 on kON overlaps value set 1 on Kon: one physical column, two codings.
-- The first side is fixture state 1 (Kon, variant 10, 2020-2021, value set 1).
INSERT INTO variable_state (state_id, variable_id, register_variant_id, valid_from, valid_to,
    delivery_column_name, value_set_id, value_set_version_label)
VALUES ((SELECT COALESCE(MAX(state_id), 0) + 1 FROM variable_state), 1, 10, '2020-01-01', '2021-12-31', 'kON', 2, 'bound-inject');
