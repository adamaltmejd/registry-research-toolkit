-- REAL -inf and inf. 1e999 overflows a double, so SQLite stores it as REAL inf.
-- A NaN cannot be stored: SQLite reads it as NULL.
CREATE TABLE t (v);
INSERT INTO t VALUES (-1e999), (1e999);
