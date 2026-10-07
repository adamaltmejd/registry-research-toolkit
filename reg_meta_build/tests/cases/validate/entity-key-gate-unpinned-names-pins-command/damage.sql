-- Variant 10 keys its panel on `kon` (scb register 1, source_id 1.44), which
-- the empty slug directory does not pin: the failure names the fix-it command.
UPDATE register_variant SET panel_entity_key = 'kon' WHERE register_variant_id = 10;
