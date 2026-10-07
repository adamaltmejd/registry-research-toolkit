-- `parencol` lives in register 2 only; variant 10 is in register 1, and slugs
-- are register-scoped, so the ref dangles.
UPDATE register_variant SET panel_entity_key = 'parencol' WHERE register_variant_id = 10;
