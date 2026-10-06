-- A thin-provider-only build: no scb (1) or sos (2) register survives, so the
-- curated concept-group floor (which only scb/sos families feed) reports a skip.
DELETE FROM register WHERE provider_id IN (1, 2);
