-- Build-only staging tables that must be dropped before ship.
CREATE TABLE variable_instance (variable_id INTEGER);
CREATE TABLE variable_alias_build (variable_id INTEGER);
