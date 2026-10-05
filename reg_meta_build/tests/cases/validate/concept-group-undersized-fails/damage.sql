-- A coherent edge concept group in register 1: variables 1 and 2 as
-- whole-variable members, no declared axes and no member facets.
INSERT INTO concept_group (group_id, kind, register_id, group_key, label, source)
VALUES (10, 'variable', 1, 'kon', 'Kön', 'edge');
INSERT INTO concept_group_variable (member_id, group_id, variable_id, delivery_column_name)
VALUES
    (1, 10, 1, NULL),
    (2, 10, 2, NULL);

-- Damage: drop member 2, leaving a one-member group.
DELETE FROM concept_group_variable WHERE member_id = 2;
