-- A coherent register-wide user-facing warning: its id is the SHA-256 of the
-- warning with code unassigned_original_columns.
INSERT INTO data_warning_text (text_id, text) VALUES
    (1, 'Fixture detail.'),
    (2, 'Some original columns are unassigned');
INSERT INTO data_warning VALUES (
    X'5d27d80383b0b344d291faf7317f7af35fb22a346ecb53f074b7c1dbf9f50937',
    1, NULL, NULL, NULL, NULL, NULL,
    'unassigned_original_columns', 'warning', 2, 1,
    '{"acknowledged_by":null,"case_id":null,"diagnostic_detail_sha256":"fa0d70e9ad0892677a8b4da3e4c4578aa3f393493ada4b19f71342a90ca3db60","fields":[],"refs":[],"withheld_output":[]}'
);

-- Damage: the column now holds a build-only code, and the evidence carries the
-- original code, which would restore the hashed content if it could override
-- the column.
UPDATE data_warning SET
    code = 'source_identity_assumption',
    evidence_json = json_set(evidence_json, '$.code', 'unassigned_original_columns');
