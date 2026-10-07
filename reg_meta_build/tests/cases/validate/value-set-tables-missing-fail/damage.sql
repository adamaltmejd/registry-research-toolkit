-- The shipped value-set tables are gone; the dependent projection checks
-- must report, not crash.
DROP TABLE value_set_member;
DROP TABLE value_set;
