-- Variant 10 keys its panel on `kon` (scb register 1), but the slug directory
-- has no register entry for scb/testreg, so the variable has no pin key: the
-- gate refuses the register instead of keying pins on the catalog's own id.
UPDATE register_variant SET panel_entity_key = 'kon' WHERE register_variant_id = 10;
