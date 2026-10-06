-- A --skip-slugs bootstrap build leaves every slug NULL by design.
UPDATE register SET slug = NULL WHERE register_id = 1;
UPDATE variable SET slug = NULL WHERE variable_id = 2;
