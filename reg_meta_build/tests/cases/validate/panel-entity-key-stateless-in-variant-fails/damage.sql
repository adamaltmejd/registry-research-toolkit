-- Sibling variant 999 in register 1 keys on `kon`, whose states live in variant 10
-- only: the slug resolves, but has no state in the keyed variant.
INSERT INTO register_variant (register_variant_id, register_id, slug, name,
    panel_entity_key, panel_time_key, panel_time_grain)
VALUES (999, 1, 'sibling', 'Sibling', 'kon', 'period', 'delivery');
