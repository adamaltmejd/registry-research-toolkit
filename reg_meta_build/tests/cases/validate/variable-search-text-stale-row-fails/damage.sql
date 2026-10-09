-- variable_search_text must equal its source, the variable index's source and the
-- display text of variable hits. One row's definition drifts while its key stays.
-- Fails if the check compares keys or row counts instead of whole rows.
UPDATE variable_search_text SET definition = 'stale'
WHERE variable_id = (SELECT MIN(variable_id) FROM variable_search_text);
