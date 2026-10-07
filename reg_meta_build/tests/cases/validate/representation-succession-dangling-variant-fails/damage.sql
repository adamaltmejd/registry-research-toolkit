-- A resolvable TestCol -> TestKolumn rename scoped to a variant testreg lacks
-- (its live variant is `individer`).
INSERT INTO representation_replaced_by (predecessor_provider, predecessor_register,
    predecessor_variable, predecessor_column, successor_provider, successor_register,
    successor_variable, successor_column, variant, effective_year, note)
VALUES ('scb', 'testreg', 'testcol', 'TestCol', 'scb', 'testreg', 'testcol',
        'TestKolumn', 'no-such-variant', 2010, 'curated:slug_toml');
