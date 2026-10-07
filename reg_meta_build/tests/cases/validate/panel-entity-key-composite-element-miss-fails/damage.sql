-- A composite entity key resolves element-wise: `kon` resolves, `ghost` does not.
UPDATE register_variant SET panel_entity_key = '["kon", "ghost"]'
WHERE register_variant_id = 10;
