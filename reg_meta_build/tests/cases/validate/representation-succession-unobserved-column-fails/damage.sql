-- scb/testreg/testcol observes `TestCol` and `TestKolumn`, not `NoSuchColumn`.
INSERT INTO representation_replaced_by (predecessor_provider, predecessor_register,
    predecessor_variable, predecessor_column, successor_provider, successor_register,
    successor_variable, successor_column, effective_year, note)
VALUES ('scb', 'testreg', 'testcol', 'NoSuchColumn', 'scb', 'testreg', 'testcol',
        'TestKolumn', 2010, 'curated:slug_toml');
