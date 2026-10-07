-- A composite time key resolves element-wise: `kon` resolves, `ghost` does not.
UPDATE register_variant SET panel_time_key = '["kon", "ghost"]'
WHERE register_variant_id = 10;
