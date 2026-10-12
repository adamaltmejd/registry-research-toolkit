-- Book `cykel`: 'A' names parent 'B' and 'B' names parent 'A', both in the same
-- book and without levels, so only the cycle invariant can refuse them; 'C' hangs
-- under 'A' and reaches the cycle without being on it.
INSERT INTO value_code (code_id, code, label, mapping_count) VALUES
    (9001, 'A', 'Första', 0), (9002, 'B', 'Andra', 0), (9003, 'C', 'Tredje', 0);
INSERT INTO classification (id, short_name, name, slug, code_count, valid_code_count)
VALUES (9001, 'CYKEL', 'Cykel', 'cykel', 3, 3);
INSERT INTO classification_code (classification_id, code_id, level, is_valid, parent_code_id)
VALUES (9001, 9001, NULL, 1, 9002), (9001, 9002, NULL, 1, 9001), (9001, 9003, NULL, 1, 9001);
