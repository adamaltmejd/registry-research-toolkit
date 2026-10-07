-- Fixture state 4 (aaocol, AaoCol, 2022, its variable's only state) ends on a 9999
-- date that is not the exact open-ended sentinel '9999-12-31'.
UPDATE variable_state SET valid_to = '9999-06-30' WHERE state_id = 4;
