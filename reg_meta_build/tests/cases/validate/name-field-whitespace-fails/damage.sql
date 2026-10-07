-- Tab-pad one name in each checked table (str.strip() semantics, not TRIM()).
UPDATE variable SET name = name || char(9) WHERE variable_id = 1;
UPDATE register SET name = name || char(9) WHERE register_id = 1;
UPDATE register_variant SET name = name || char(9) WHERE register_variant_id = 10;
