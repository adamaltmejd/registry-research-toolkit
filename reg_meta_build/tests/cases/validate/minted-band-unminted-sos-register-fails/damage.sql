-- A sos (provider 2) register whose id sits in the SCB low band, as an adapter
-- that forgot to mint() would write it.
INSERT INTO register (register_id, provider_id, slug, name) VALUES (500, 2, 'dors', 'Dodsorsaker');
