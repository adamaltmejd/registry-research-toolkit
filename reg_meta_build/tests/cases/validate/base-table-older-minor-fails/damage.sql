-- A 12.0 base that derive lifted to 12.1.0: holding_table without the 12.1.0
-- period columns. The synthetic catalog holds no holdings, so the table is empty.
DROP TABLE holding_table;
CREATE TABLE holding_table (
    table_id INTEGER PRIMARY KEY,
    physical_id TEXT NOT NULL UNIQUE,
    scope TEXT NOT NULL,
    edition_json TEXT,
    partition TEXT,
    projection_of TEXT,
    projection TEXT,
    retain_unknown_reason TEXT,
    source_ref TEXT NOT NULL
);
