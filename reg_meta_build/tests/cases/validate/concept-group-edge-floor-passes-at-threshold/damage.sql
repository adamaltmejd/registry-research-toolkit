-- Exactly 1,800 edge groups in register 1 (the corpus floor). They carry no
-- members: the floor counts groups by source, and this case asserts only the
-- floor line (the undersized-group failure they also raise is expected).
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 1800)
INSERT INTO concept_group (kind, register_id, group_key, label, source)
SELECT 'variable', 1, printf('edge%04d', i), printf('Edge %d', i), 'edge' FROM n;
