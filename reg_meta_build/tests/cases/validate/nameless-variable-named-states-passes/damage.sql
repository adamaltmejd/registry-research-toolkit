-- Kon (testreg) has no common name, but both of its states carry a positive
-- delivery name: the permitted shape.
UPDATE variable SET name = NULL WHERE variable_id = 1;
UPDATE variable_state SET name = 'Kön' WHERE state_id = 1;
UPDATE variable_state SET name = 'Kön (ny)' WHERE state_id = 2;
