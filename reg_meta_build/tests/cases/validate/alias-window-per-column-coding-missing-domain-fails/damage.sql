-- Variable 2's fixture windows (TestCol, TestKolumn; exact over state 3, which
-- is unclassified and carries no value set) get a per-column coding override
-- with their own one-code value set 4.
INSERT INTO value_code (code_id, code, label, mapping_count) VALUES (4, '1', 'Ja', 0);
INSERT INTO value_set (value_set_id, member_hash)
VALUES (4, X'6161616161616161616161616161616161616161616161616161616161616161');
INSERT INTO value_set_member (value_set_id, code_id) VALUES (4, 4);
UPDATE variable_alias_window
SET coding_metadata = 'per_column', value_set_id = 4, value_set_version_label = 'Egen'
WHERE variable_id = 2;

-- Damage: drop the override's value set (bypassing the DDL CHECK).
UPDATE variable_alias_window SET value_set_id = NULL WHERE variable_id = 2;
