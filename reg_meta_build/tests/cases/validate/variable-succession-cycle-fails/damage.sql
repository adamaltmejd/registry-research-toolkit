-- Two live variables succeed each other: kon -> testcol -> kon.
-- Fails if validate_built_db stops refusing succession cycles or stops naming a
-- node on the cycle.
INSERT INTO variable_replaced_by (predecessor_provider, predecessor_register, predecessor_variable, successor_provider, successor_register, successor_variable)
VALUES ('scb', 'testreg', 'kon', 'scb', 'testreg', 'testcol'),
       ('scb', 'testreg', 'testcol', 'scb', 'testreg', 'kon');
