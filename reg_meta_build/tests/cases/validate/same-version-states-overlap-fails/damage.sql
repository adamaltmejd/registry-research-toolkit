-- A later writer gives kon a second 'Kön' state on the last day of state 1's
-- window: a one-day overlap, so the closed-interval bound is pinned. Its distinct
-- valid_from passes idx_variable_state_unique, and it shares state 1's column and
-- value set, so no per-column check sees it.
-- Two allowed overlaps must not add a pair, so the count stays 1: 'Kön 2021'
-- overlaps both under another code-list version label (fails if the label leaves
-- the key), and a 'Kön' state in sibling variant 999 overlaps state 1 (fails if
-- the variant leaves the key).
INSERT INTO register_variant (register_variant_id, register_id, slug, name)
VALUES (999, 1, 'sibling', 'Sibling');
INSERT INTO variable_alias (variable_id, register_variant_id, delivery_column_name)
VALUES (1, 999, 'Kon');
INSERT INTO variable_state (state_id, variable_id, register_variant_id, period_scope, valid_from, valid_to,
    delivery_column_name, data_type, data_length, value_set_id, value_set_version_label)
VALUES ((SELECT COALESCE(MAX(state_id), 0) + 1 FROM variable_state), 1, 10, 'intervals', '2021-12-31', '2021-12-31', 'Kon', 'int', '1', 1, 'Kön'),
       ((SELECT COALESCE(MAX(state_id), 0) + 2 FROM variable_state), 1, 10, 'intervals', '2021-01-01', '2021-12-31', 'Kon', 'int', '1', 1, 'Kön 2021'),
       ((SELECT COALESCE(MAX(state_id), 0) + 3 FROM variable_state), 1, 999, 'intervals', '2020-01-01', '2021-12-31', 'Kon', 'int', '1', 1, 'Kön');
