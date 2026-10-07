-- A derived vintage-lift edge from live scb/testreg/kon to itself.
INSERT INTO variable_replaced_by (predecessor_provider, predecessor_register,
    predecessor_variable, successor_provider, successor_register, successor_variable,
    effective_year, note)
VALUES ('scb', 'testreg', 'kon', 'scb', 'testreg', 'kon', 2012,
        'derived:classification_vintage_lift');
