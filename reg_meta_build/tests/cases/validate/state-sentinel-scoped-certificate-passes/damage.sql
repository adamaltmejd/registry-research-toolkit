-- State 6 (UniqCol, 2021, value set 2 = code '2' 'Övriga civilstånd') declares
-- book `testklass`, whose canonical codes are only '1'. Its one non-canonical
-- member is kept as a sentinel by a scoped certificate for UniqCol over 2021.
INSERT INTO value_code (code_id, code, label, mapping_count) VALUES (10, '1', 'Kanonisk', 0);
INSERT INTO classification (id, short_name, name, slug, code_count, valid_code_count)
VALUES (1, 'TESTKLASS', 'Testklassning', 'testklass', 1, 1);
INSERT INTO classification_code (classification_id, code_id, level, is_valid) VALUES (1, 10, 1, 1);
INSERT INTO state_classification (state_id, classification_id, provenance) VALUES (6, 1, NULL);
INSERT INTO classification_conformance (
    state_id, declared_classification_id, status, checked_code_count,
    matched_code_count, nonconforming_code_count, overlap
) VALUES (6, 1, 'extended', 1, 0, 1, 0.0);

-- The certificate covers exactly the state's column and window.
INSERT INTO classification_conformance_code (
    state_id, declared_classification_id, code_id, member_kind, sentinel_meaning, scoped_sentinels
) VALUES (6, 1, 3, 'sentinel', NULL, '[{"valid_from": "2021-01-01", "valid_to": "2021-12-31", "delivery_column_name": "UniqCol", "classification_sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "source_fingerprints": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"], "members": [["2", "Övriga civilstånd"]], "provenance": "checked-source-sentinel: synthetic case"}]');
-- The writer indexes every classification row for name search.
INSERT INTO classification_fts(classification_fts) VALUES ('rebuild');
