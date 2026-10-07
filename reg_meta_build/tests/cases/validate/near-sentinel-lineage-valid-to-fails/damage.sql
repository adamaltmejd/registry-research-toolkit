-- A lineage edge ending '9999-00-00': malformed, sorts below '9999-01-01', so
-- only a prefix match (not a >= '9999-01-01' range) catches it.
INSERT INTO variable_state_lineage (consumer_state_id, source_state_id, valid_from, valid_to)
VALUES (1, 1, '2000-01-01', '9999-00-00');
