-- State 3's replaceable base (kind base_fallback, outside the resolver_column
-- projection) is respelled TESTCOL while its source window keeps TestCol, so one
-- column fold of (variable 2, variant 10) has two spellings. Fails if the spelling
-- check stops grouping by the py_lower fold.
UPDATE expanded_state SET canonical_column = 'TESTCOL'
WHERE state_id = 3 AND kind = 'base_fallback';
