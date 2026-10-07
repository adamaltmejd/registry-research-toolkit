-- Variant 10's time key names a slug no variable in register 1 carries.
UPDATE register_variant SET panel_time_key = 'nonexistent_slug' WHERE register_variant_id = 10;
