-- A composite time key whose elements (`kon`, `testcol`) both live in register 1.
UPDATE register_variant SET panel_time_key = '["kon", "testcol"]'
WHERE register_variant_id = 10;
