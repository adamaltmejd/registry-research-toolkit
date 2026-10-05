-- One curated tag with a variable-grain member (variable 2) and a
-- register-grain member (register 1).
INSERT INTO tag (tag_id, slug, label, description)
VALUES (1, 'testtag', 'Testtagg', NULL);
INSERT INTO tag_member (tag_id, register_id, variable_id, rank, starred, note)
VALUES
    (1, NULL, 2, 0, 1, 'primary'),
    (1, 1, NULL, 1, 0, NULL);

-- Damage: the register-grain member points at a register that does not exist.
UPDATE tag_member SET register_id = 99 WHERE register_id = 1;
