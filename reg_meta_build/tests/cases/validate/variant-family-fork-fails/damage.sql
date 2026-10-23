-- Two TESTREG variants both succeeded by the fixture's `individer` (variant 10),
-- with disjoint days: a merge, not a chain.
INSERT INTO register_variant (register_variant_id, register_id, name, description, slug)
VALUES (11, 1, 'Individer äldre', 'Äldre individer', 'individer-aldre'),
    (12, 1, 'Individer urval', 'Ett urval', 'individer-urval');
INSERT INTO variable_state (state_id, variable_id, register_variant_id, valid_from, valid_to,
    data_type, data_length, delivery_column_name, value_set_id, value_set_version_label)
VALUES ((SELECT MAX(state_id) + 1 FROM variable_state), 1, 11, '2015-01-01', '2016-12-31',
    'int', '1', 'Kon', 1, 'Kön'),
    ((SELECT MAX(state_id) + 2 FROM variable_state), 1, 12, '2017-01-01', '2019-12-31',
    'int', '1', 'Kon', 1, 'Kön');
INSERT INTO variant_replaced_by VALUES
    ('scb', 'testreg', 'individer-aldre', 'scb', 'testreg', 'individer'),
    ('scb', 'testreg', 'individer-urval', 'scb', 'testreg', 'individer');
INSERT INTO variable_alias (variable_id, register_variant_id, delivery_column_name)
VALUES (1, 11, 'Kon'), (1, 12, 'Kon');
