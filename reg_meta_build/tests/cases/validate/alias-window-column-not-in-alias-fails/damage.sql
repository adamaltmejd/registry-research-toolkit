-- A coherent shared window on kon's delivery column, inside state 1.
INSERT INTO variable_alias_window (variable_id, register_variant_id, delivery_column_name, valid_from, valid_to)
VALUES (1, 10, 'Kon', '2020-01-01', '2020-01-31');

-- Damage: rename kon's alias so the window column has no alias row.
UPDATE variable_alias SET delivery_column_name = 'OtherCol' WHERE variable_id = 1;
