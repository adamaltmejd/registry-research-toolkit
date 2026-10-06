-- Variable 2's fixture windows (TestCol, TestKolumn; exact over state 3, which
-- is unclassified and carries no value set) get a per-column coding override:
-- value set 4 = '1' 'Ja' and '9' 'Okänt'. The TestCol window declares book
-- `testklass` (canonical code '1'); '9' is a scoped sentinel for TestCol 2020.
INSERT INTO value_code (code_id, code, label, mapping_count) VALUES
    (4, '1', 'Ja', 0),
    (5, '9', 'Okänt', 0),
    (10, '1', 'Kanonisk', 0);
INSERT INTO value_set (value_set_id, member_hash)
VALUES (4, X'6161616161616161616161616161616161616161616161616161616161616161');
INSERT INTO value_set_member (value_set_id, code_id) VALUES (4, 4), (4, 5);
UPDATE variable_alias_window
SET coding_metadata = 'per_column', value_set_id = 4, value_set_version_label = 'Egen'
WHERE variable_id = 2;
INSERT INTO classification (id, short_name, name, slug, code_count, valid_code_count)
VALUES (1, 'TESTKLASS', 'Testklassning', 'testklass', 1, 1);
INSERT INTO classification_code (classification_id, code_id, level, is_valid) VALUES (1, 10, 1, 1);
INSERT INTO alias_window_classification (
    variable_id, register_variant_id, delivery_column_name, valid_from, classification_id, provenance, conformance
) VALUES (2, 10, 'TestCol', '2020-01-01', 1, NULL, '{"declared_classification": "testklass", "status": "extended", "checked_codes": ["1", "9"], "nonconforming_members": [], "sentinel_members": [["9", "Okänt"]], "scoped_sentinels": [{"valid_from": "2020-01-01", "valid_to": "2020-12-31", "delivery_column_name": "TestCol", "classification_sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "source_fingerprints": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"], "members": [["9", "Okänt"]], "provenance": "checked-source-sentinel: synthetic case"}]}');

-- Damage: the sentinel member's stored label differs from the recorded decision.
UPDATE value_code SET label = 'Ändrad' WHERE code_id = 5;
