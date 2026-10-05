-- A coherent edge concept group in register 1: variables 1 and 2 as
-- whole-variable members, no declared axes and no member facets.
INSERT INTO concept_group (group_id, kind, register_id, group_key, label, source)
VALUES (10, 'variable', 1, 'kon', 'Kön', 'edge');
INSERT INTO concept_group_variable (member_id, group_id, variable_id, delivery_column_name)
VALUES
    (1, 10, 1, NULL),
    (2, 10, 2, NULL);

-- A two-member classification umbrella (source 'curated') over two vintages.
INSERT INTO classification (id, short_name, name, slug)
VALUES
    (1, 'TESTKLASS2000', 'Testklassning 2000', 'testklass2000'),
    (2, 'TESTKLASS2020', 'Testklassning 2020', 'testklass2020');
INSERT INTO concept_group (group_id, kind, register_id, group_key, label, source)
VALUES (20, 'classification', NULL, 'testklass', 'Testklassning', 'curated');
INSERT INTO concept_group_classification (classification_id, group_id, facet_value, facet_label)
VALUES
    (1, 20, '2000', 'Testklassning 2000'),
    (2, 20, '2020', 'Testklassning 2020');

-- Damage: the classification umbrella declares two axes.
INSERT INTO concept_group_axis (group_id, axis, ordinal, label)
VALUES
    (20, 'vintage', 0, 'Vintage'),
    (20, 'region', 1, 'Region');
