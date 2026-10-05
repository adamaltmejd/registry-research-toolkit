-- A derived vintage-lift edge joins variables from two slug stems (individ-* vs
-- foretag-*) across one classification succession step.
INSERT INTO classification (short_name, name, slug) VALUES ('SNI2002', 'SNI 2002', 'sni2002');
INSERT INTO classification (short_name, name, slug) VALUES ('SNI2007', 'SNI 2007', 'sni2007');
INSERT INTO classification_replaced_by (predecessor_slug, successor_slug, effective_year, note)
VALUES ('sni2002', 'sni2007', 2007, 'derived:vintage_chain');
INSERT INTO variable (register_id, provider_key, slug, name)
VALUES (1, 'stream-pred', 'individ-sni-2002', 'Näringsgren'),
       (1, 'stream-succ', 'foretag-sni-2007', 'Näringsgren');
INSERT INTO variable_state (variable_id, register_variant_id, valid_from, valid_to, value_set_version_label)
SELECT variable_id, 10, '2002-01-01', '2006-12-31', '' FROM variable WHERE slug = 'individ-sni-2002';
INSERT INTO variable_state (variable_id, register_variant_id, valid_from, valid_to, value_set_version_label)
SELECT variable_id, 10, '2007-01-01', '9999-12-31', '' FROM variable WHERE slug = 'foretag-sni-2007';
INSERT INTO state_classification
SELECT s.state_id, c.id, NULL
FROM variable_state s JOIN variable v USING (variable_id), classification c
WHERE (v.slug = 'individ-sni-2002' AND c.slug = 'sni2002')
   OR (v.slug = 'foretag-sni-2007' AND c.slug = 'sni2007');
INSERT INTO variable_replaced_by (
    predecessor_provider, predecessor_register, predecessor_variable,
    successor_provider, successor_register, successor_variable, effective_year, note
) VALUES ('scb', 'testreg', 'individ-sni-2002', 'scb', 'testreg', 'foretag-sni-2007',
          2007, 'derived:classification_vintage_lift');
