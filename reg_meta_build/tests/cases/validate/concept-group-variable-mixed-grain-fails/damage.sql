-- A coherent edge concept group in register 1: variables 1 and 2 as
-- whole-variable members, no declared axes and no member facets.
INSERT INTO concept_group (group_id, kind, register_id, group_key, label, source)
VALUES (10, 'variable', 1, 'kon', 'Kön', 'edge');
INSERT INTO concept_group_variable (member_id, group_id, variable_id, delivery_column_name)
VALUES
    (1, 10, 1, NULL),
    (2, 10, 2, NULL);

-- Damage: variable 1 is already a whole-variable member of group 10; add a
-- representation member (column 'Kon') for it in the same group.
INSERT INTO concept_group_variable (member_id, group_id, variable_id, delivery_column_name)
VALUES (3, 10, 1, 'Kon');
