-- A non-SCB `--providers` build: no SCB (provider 1) register survives, so the four
-- SCB-sourced corpus volume floors report a skip instead of failing.
DELETE FROM register WHERE provider_id = 1;
