-- A state bound that is not a full ISO date, and one whose window is inverted.
UPDATE variable_state SET valid_from = '2020' WHERE state_id = 3;
UPDATE variable_state SET valid_from = '2022-12-31', valid_to = '2022-01-01' WHERE state_id = 4;
