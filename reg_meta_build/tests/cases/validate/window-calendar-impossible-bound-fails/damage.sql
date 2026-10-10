-- A shared window on kon's delivery column ending on a calendar-impossible day.
-- `2020-02-30` (a day <= 31 the month lacks) is the value older SQLite builds
-- (3.40.1) leave unchanged under `date()`, so a `date(x) IS NOT x` check passes
-- it there. Fails if validate_built_db stops refusing a calendar-impossible
-- stored bound or stops naming the column.
INSERT INTO variable_alias_window (variable_id, register_variant_id, delivery_column_name, valid_from, valid_to)
VALUES (1, 10, 'Kon', '2020-01-01', '2020-02-30');
