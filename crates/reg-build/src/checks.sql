-- Post-write checks of the catalog file (schema v4): each returns no rows.
-- PRAGMA integrity_check runs separately (it returns 'ok').
PRAGMA foreign_key_check;

-- (1) namespace consistency
SELECT pid FROM register EXCEPT SELECT pid FROM ident WHERE kind='register';
-- The implicit table takes its register's id, held in ident as the register's.
SELECT pid FROM tbl WHERE implicit = 0 EXCEPT SELECT pid FROM ident WHERE kind='table';
SELECT pid FROM variable EXCEPT SELECT pid FROM ident WHERE kind='variable';
SELECT pid FROM var_set WHERE kind='set' EXCEPT SELECT pid FROM ident WHERE kind='set';
SELECT pid FROM ident INTERSECT SELECT pid FROM ident_retired;
SELECT pid FROM var_set INTERSECT SELECT pid FROM ident_retired;
-- Picking units contain at least two distinct final variables, all in their register.
SELECT s.pid FROM var_set s LEFT JOIN var_set_member m ON m.set_id = s.id
WHERE s.kind = 'set' GROUP BY s.id HAVING COUNT(DISTINCT m.variable_id) < 2;
SELECT s.pid FROM var_set s JOIN var_set_member m ON m.set_id = s.id
JOIN variable v ON v.id = m.variable_id
WHERE s.kind = 'set' AND s.register_id <> v.register_id;
-- Presentation axes belong to picking units and retain a dense authored order.
SELECT a.set_id FROM var_set_axis a JOIN var_set s ON s.id = a.set_id WHERE s.kind <> 'set';
SELECT set_id FROM var_set_axis GROUP BY set_id HAVING MIN(ord) <> 0 OR MAX(ord) + 1 <> COUNT(*);
-- A register's bare-name key is its public name (the pid's last segment), not its handle.
-- Public names are lowercase slugs, so SQL lower() agrees with fold_identity.
SELECT pid FROM ident WHERE kind = 'register'
  AND name_fold <> lower(substr(pid, length(rtrim(pid, replace(pid, '/', ''))) + 1));

-- (4) no overlap within (variable, table): one folded spelling at a time
SELECT id FROM (
  SELECT id, start, LAG(end) OVER (
    PARTITION BY variable_id, table_id ORDER BY start) AS prev_end
  FROM segment) WHERE start < prev_end;

-- (5) ambiguity is marked and symmetric
SELECT a.id FROM segment a JOIN segment b
  ON b.table_id = a.table_id AND b.fold_id = a.fold_id AND b.variable_id <> a.variable_id
 AND a.start < b.end AND b.start < a.end
WHERE a.state <> 'ambiguous';

-- (6) segment inside table coverage
SELECT s.id FROM segment s WHERE NOT EXISTS (
  SELECT 1 FROM tbl_span t WHERE t.table_id = s.table_id AND t.start <= s.start AND s.end <= t.end);

-- (7) used occurrences sit inside their segment
SELECT o.id FROM occurrence o
  JOIN segment s ON s.id = o.segment_id
  JOIN spelling sp ON sp.id = o.spelling_id
WHERE o.variable_id <> s.variable_id OR o.table_id <> s.table_id OR sp.fold_id <> s.fold_id
   OR o.start < s.start OR o.end > s.end
   OR (s.state <> 'ambiguous' AND (o.valueset_id IS NOT s.valueset_id
       OR o.class_version_id IS NOT s.class_version_id));
-- Type agreement uses reg-frame's family map in verify.rs.

-- (8) variables have segments; hull matches
SELECT v.id FROM variable v LEFT JOIN (
  SELECT variable_id, MIN(start) s, MAX(end) e FROM segment GROUP BY variable_id) g
  ON g.variable_id = v.id
WHERE g.variable_id IS NULL OR g.s <> v.start OR g.e <> v.end;

-- (9) one primary per parsed (table, period, qualifier)
SELECT table_id, start, end, qualifier FROM release WHERE parse_state <> 'unparsed'
GROUP BY table_id, start, end, qualifier HAVING SUM(is_primary) <> 1;

-- (10) value-set structure (the hash is re-verified by `catalog verify`, not SQL)
SELECT valueset_id FROM vs_item GROUP BY valueset_id HAVING MAX(ord) + 1 <> COUNT(*);
SELECT v.id FROM valueset v JOIN (SELECT valueset_id, COUNT(*) n FROM vs_item GROUP BY valueset_id) c
  ON c.valueset_id = v.id WHERE c.n <> v.n_items;
SELECT i.valueset_id, i.ord FROM vs_item i WHERE i.parent_ord IS NOT NULL AND NOT EXISTS (
  SELECT 1 FROM vs_item p WHERE p.valueset_id = i.valueset_id AND p.ord = i.parent_ord);
-- a value set's classification link: equal is the version's content; subset has no
-- code outside the version but its sentinels. A set anti-join: vs_item has no index on
-- code, and SQLite indexes the NOT IN subquery itself (0.23 s at 0.80.0 scale; a
-- correlated NOT EXISTS scans the version's items per code). Both columns are NOT NULL.
SELECT v.id FROM valueset v JOIN class_version c ON c.id = v.class_version_id
WHERE v.class_match = 'equal' AND c.valueset_id <> v.id;
SELECT DISTINCT v.id FROM valueset v
  JOIN class_version c ON c.id = v.class_version_id
  JOIN vs_item i ON i.valueset_id = v.id
WHERE v.class_match = 'subset' AND i.sentinel = 0
  AND (c.valueset_id, i.code_text) NOT IN (
    SELECT valueset_id, code_text FROM vs_item
    WHERE valueset_id IN (SELECT valueset_id FROM class_version));

-- A classification states a part on every version or on none: its page groups the
-- versions by part, and a version without one would belong to no line.
SELECT f.pid FROM class_family f JOIN class_version c ON c.family_id = f.id
GROUP BY f.id HAVING COUNT(c.part_text) NOT IN (0, COUNT(*));

-- Class-version periods describe book existence. Same-family overlaps are allowed;
-- applicability belongs to exact-version occurrence bindings.

-- (12) continues is acyclic (variables and tables)
WITH RECURSIVE walk(k, origin, node, depth) AS (
  SELECT subject_kind, subject_id, object_id, 1 FROM relation WHERE kind = 'continues'
  UNION ALL
  SELECT w.k, w.origin, r.object_id, w.depth + 1 FROM walk w JOIN relation r
    ON r.kind = 'continues' AND r.subject_kind = w.k AND r.subject_id = w.node
  WHERE w.depth < 64)
SELECT k, origin FROM walk WHERE origin = node;

-- (13) successors of one table start on distinct days
SELECT object_id, at_start FROM relation
WHERE kind = 'continues' AND subject_kind = 'table'
GROUP BY object_id, at_start HAVING COUNT(*) > 1;

-- unplaced occurrences belong to unparsed releases
SELECT o.id FROM occurrence o LEFT JOIN release r ON r.id = o.release_id
WHERE o.status = 'unplaced' AND (r.id IS NULL OR r.parse_state <> 'unparsed');

-- search rows and FTS rows agree (contentless index: compare counts)
SELECT 1 WHERE (SELECT COUNT(*) FROM search_item) <> (SELECT COUNT(*) FROM search_docsize);

-- supplementary FTS rows use the primary search rowid of a variable
SELECT d.id FROM search_description_docsize d LEFT JOIN search_item s ON s.id = d.id
WHERE s.kind IS NOT 'variable';

-- every code row has a value set, and its value sets are exactly the segment-referenced
-- sets that hold its (code, label) pair
SELECT id FROM search_item s WHERE kind = 'code' AND NOT EXISTS (
  SELECT 1 FROM search_code_valueset c WHERE c.item_id = s.id);
SELECT 1 WHERE EXISTS (
  SELECT s.id, v.valueset_id FROM vs_item v
    JOIN search_item s ON s.kind = 'code' AND s.code_text = v.code_text
      AND s.label_text IS v.label_text
  WHERE EXISTS (SELECT 1 FROM segment g WHERE g.valueset_id = v.valueset_id)
  EXCEPT SELECT item_id, valueset_id FROM search_code_valueset
) OR EXISTS (
  SELECT item_id, valueset_id FROM search_code_valueset
  EXCEPT SELECT s.id, v.valueset_id FROM vs_item v
    JOIN search_item s ON s.kind = 'code' AND s.code_text = v.code_text
      AND s.label_text IS v.label_text
  WHERE EXISTS (SELECT 1 FROM segment g WHERE g.valueset_id = v.valueset_id)
);

-- meta has its required keys
SELECT column1 FROM (VALUES ('schema_version'),('generation_id'),('catalog_version'),
  ('builder_version'),('sqlite_version'),('open_start'),('open_end'),
  ('curation_digest'),('period_grammar'),('delivery_name_grammar'))
EXCEPT SELECT key FROM meta;

-- a stored pdf is the size its row states (verify.rs re-hashes it)
SELECT id FROM doc WHERE kind = 'pdf' AND bytes <> length(content);

-- every document has its search row
SELECT d.id FROM doc d WHERE NOT EXISTS (
  SELECT 1 FROM search_item s WHERE s.kind = 'document' AND s.entity_id = d.id);

-- a series break's subject exists, and a release subject is placed (its scope is its period)
SELECT b.id FROM series_break b WHERE NOT CASE b.subject_kind
  WHEN 'register' THEN EXISTS (SELECT 1 FROM register r WHERE r.id = b.subject_id)
  WHEN 'table' THEN EXISTS (SELECT 1 FROM tbl t WHERE t.id = b.subject_id)
  WHEN 'release' THEN EXISTS (SELECT 1 FROM release x WHERE x.id = b.subject_id AND x.start IS NOT NULL)
  WHEN 'variable' THEN EXISTS (SELECT 1 FROM variable v WHERE v.id = b.subject_id)
END;
