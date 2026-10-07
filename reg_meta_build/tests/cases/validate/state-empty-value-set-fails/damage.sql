-- An empty value set 99 (no members); fixture state 1 (kon, Kon) points at it,
-- so its year projection yields zero codes.
INSERT INTO value_set (value_set_id, member_hash)
VALUES (99, X'EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE');
UPDATE variable_state SET value_set_id = 99 WHERE state_id = 1;
