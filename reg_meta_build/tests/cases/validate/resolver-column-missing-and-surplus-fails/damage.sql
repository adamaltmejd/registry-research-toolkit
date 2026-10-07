-- resolver_column must equal a recomputation of the resolver's emitted columns. One
-- authorized column goes missing and one unemitted column is authorized at a real
-- (variable, variant). Fails if the check compares row counts instead of rows, or
-- stops recomputing the universe.
DELETE FROM resolver_column
WHERE (variable_id, register_variant_id, delivery_column_lower) = (
    SELECT variable_id, register_variant_id, delivery_column_lower
    FROM resolver_column ORDER BY 1, 2, 3 LIMIT 1
);
INSERT INTO resolver_column (variable_id, register_variant_id, delivery_column_lower, delivery_column_name)
SELECT variable_id, register_variant_id, 'zz_unemitted', 'ZZ_UNEMITTED'
FROM resolver_column ORDER BY 1, 2 LIMIT 1;
