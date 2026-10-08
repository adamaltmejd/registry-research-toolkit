-- coded_variable_stats must equal get_coded_variables per scope. One name's distinct
-- code count drifts by one while its key and other counts stay. Fails if the check
-- compares names or row counts instead of whole rows.
UPDATE coded_variable_stats SET n_distinct_codes = n_distinct_codes + 1
WHERE (scope, variable_name) = (
    SELECT scope, variable_name FROM coded_variable_stats ORDER BY 1, 2 LIMIT 1
);
