-- scb/testreg/kon observes column `Kon`; the same endpoint on both sides.
INSERT INTO representation_replaced_by (predecessor_provider, predecessor_register,
    predecessor_variable, predecessor_column, successor_provider, successor_register,
    successor_variable, successor_column, effective_year, note)
VALUES ('scb', 'testreg', 'kon', 'Kon', 'scb', 'testreg', 'kon', 'Kon',
        2010, 'curated:slug_toml');
