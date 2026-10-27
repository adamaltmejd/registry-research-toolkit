-- Post-write checks of the catalog file (schema v4): each returns no rows.
-- PRAGMA integrity_check runs separately (it returns 'ok').
PRAGMA foreign_key_check;

-- (1) namespace consistency
SELECT pid FROM register EXCEPT SELECT pid FROM ident WHERE kind='register';
-- The implicit table takes its register's id, held in ident as the register's.
SELECT pid FROM tbl WHERE implicit = 0 EXCEPT SELECT pid FROM ident WHERE kind='table';
SELECT pid FROM variable EXCEPT SELECT pid FROM ident WHERE kind='variable';
SELECT pid FROM ident INTERSECT SELECT pid FROM ident_retired;

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
   OR (s.state <> 'ambiguous' AND (o.dtype IS NOT s.dtype OR o.valueset_id IS NOT s.valueset_id));

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

-- (11) class versions do not overlap within a family
SELECT id FROM (SELECT id, start, LAG(end) OVER (PARTITION BY family_id ORDER BY start) pe
                FROM class_version) WHERE start < pe;

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

-- every code row's (code, label) pair occurs in vs_item (via vs_item_code)
SELECT id FROM search_item s WHERE kind = 'code' AND NOT EXISTS (
  SELECT 1 FROM vs_item v WHERE v.code_text = s.code_text AND v.label_text IS s.label_text);

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
