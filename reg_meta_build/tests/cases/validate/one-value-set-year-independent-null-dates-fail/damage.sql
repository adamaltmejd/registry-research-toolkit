-- Two year-independent states (NULL dates) hold distinct value sets on case-variant
-- spellings of one column: NULL dates do not evade the per-period guard.
INSERT INTO variable_state (variable_id, register_variant_id, period_scope, valid_from, valid_to,
    delivery_column_name, value_set_id, value_set_version_label)
VALUES (2, 10, 'year_independent', NULL, NULL, 'Group', 1, 'independent-0'),
       (2, 10, 'year_independent', NULL, NULL, 'group', 2, 'independent-1');
