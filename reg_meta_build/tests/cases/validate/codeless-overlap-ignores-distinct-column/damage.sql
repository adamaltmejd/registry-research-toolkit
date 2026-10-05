-- A code-less state on another column never meets the code-bearing Kon window.
-- The code-bearing side is fixture state 1 (Kon, variant 10, 2020-2021, value set 1).
INSERT INTO variable_state (variable_id, register_variant_id, valid_from, valid_to,
    delivery_column_name, value_set_id, value_set_version_label)
VALUES (1, 10, '2020-01-01', '2021-12-31', 'AnnanKolumn', NULL, 'bound-inject');
