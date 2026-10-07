-- Variant 10 (register 1) keys its panel on `kon`, which has states in variant 10;
-- the literal `period` time key is exempt from resolution.
UPDATE register_variant SET panel_entity_key = 'kon', panel_time_key = 'period'
WHERE register_variant_id = 10;
