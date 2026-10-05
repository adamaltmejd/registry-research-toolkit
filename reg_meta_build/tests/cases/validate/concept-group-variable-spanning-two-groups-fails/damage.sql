-- A coherent edge concept group in register 1: variables 1 and 2 as
-- whole-variable members, no declared axes and no member facets.
INSERT INTO concept_group (group_id, kind, register_id, group_key, label, source)
VALUES (10, 'variable', 1, 'kon', 'Kön', 'edge');
INSERT INTO concept_group_variable (member_id, group_id, variable_id, delivery_column_name)
VALUES
    (1, 10, 1, NULL),
    (2, 10, 2, NULL);

-- Damage: a second (curated) group also claims variable 1.
INSERT INTO concept_group (group_id, kind, register_id, group_key, label, source)
VALUES (11, 'variable', 1, 'other', 'Annan', 'curated');
INSERT INTO concept_group_variable (member_id, group_id, variable_id, delivery_column_name)
VALUES (3, 11, 1, NULL);
