-- Repoint fixture variable 1 (testreg/kon) to the anchor's (register 34,
-- provider_key 24193) and give both its states (1, 2) a 2010 window on a fresh
-- value set 99 carrying codes 01, 02, 03.
INSERT INTO register (register_id, provider_id, name, slug)
VALUES (34, 1, 'Anchor register', 'anchor-reg');
UPDATE variable SET provider_key = '24193', register_id = 34 WHERE variable_id = 1;
INSERT INTO value_code (code_id, code, label) VALUES
    (100, '01', '01'),
    (101, '02', '02'),
    (102, '03', '03');
INSERT INTO value_set (value_set_id, member_hash)
VALUES (99, X'ABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABAB');
INSERT INTO value_set_member (value_set_id, code_id) VALUES
    (99, 100),
    (99, 101),
    (99, 102);
UPDATE variable_state SET valid_from = '2010-01-01', valid_to = '2010-12-31',
    value_set_id = 99 WHERE variable_id = 1;
