-- Tab-pad kon's delivery column on its states (1, 2) and its alias in lockstep,
-- so only the hygiene check fires. A tab, not a space: the check must match
-- str.strip(), not SQLite TRIM().
UPDATE variable_state SET delivery_column_name = 'Kon' || char(9) WHERE variable_id = 1;
UPDATE variable_alias SET delivery_column_name = 'Kon' || char(9) WHERE variable_id = 1;
