-- Physical holdings are paired with, never appended to, the reference catalog.
CREATE TABLE holding_table (
 id INTEGER PRIMARY KEY,
 physical_name TEXT NOT NULL UNIQUE,
 key_requirements TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(key_requirements)),
 schema_name TEXT,
 table_type TEXT,
 file_name TEXT NOT NULL UNIQUE,
 disposition TEXT NOT NULL CHECK(disposition IN ('dated','timeless','unknown','lookup','excluded')),
 delivery TEXT,
 partition TEXT,
 projection_of INTEGER REFERENCES holding_table(id),
 projection_token TEXT,
 period_column_id INTEGER REFERENCES holding_column(id),
 period_encoding TEXT CHECK(period_encoding IN ('date','year','yyyymm','yyyymmdd')),
 reason TEXT,
 locator TEXT NOT NULL,
 CHECK((disposition = 'dated') = (delivery IS NOT NULL)),
 CHECK(partition IS NULL OR projection_of IS NULL),
 CHECK((projection_of IS NULL) = (projection_token IS NULL)),
 CHECK((period_column_id IS NULL) = (period_encoding IS NULL))
) STRICT;
CREATE TABLE holding_route (
 holding_id INTEGER NOT NULL REFERENCES holding_table(id),
 table_pid TEXT NOT NULL,
 PRIMARY KEY(holding_id,table_pid)
) STRICT, WITHOUT ROWID;
CREATE TABLE holding_period (
 holding_id INTEGER NOT NULL REFERENCES holding_table(id),
 start INTEGER NOT NULL,
 end INTEGER NOT NULL,
 PRIMARY KEY(holding_id,start),
 CHECK(start < end)
) STRICT, WITHOUT ROWID;
CREATE TABLE holding_column (
 id INTEGER PRIMARY KEY,
 holding_id INTEGER NOT NULL REFERENCES holding_table(id),
 name TEXT NOT NULL,
 ordinal INTEGER NOT NULL CHECK(ordinal > 0),
 storage_type TEXT,
 length INTEGER,
 precision INTEGER,
 scale INTEGER,
 slot_kind TEXT CHECK(slot_kind IN ('position','month','year')),
 slot_index TEXT,
 disposition TEXT NOT NULL CHECK(disposition IN ('candidate','hold','unmap')),
 reason TEXT,
 follow_up TEXT,
 locator TEXT NOT NULL,
 UNIQUE(holding_id,name),
 UNIQUE(holding_id,ordinal),
 CHECK((slot_kind IS NULL) = (slot_index IS NULL)),
 CHECK(disposition != 'hold' OR (reason IS NOT NULL AND follow_up IS NOT NULL)),
 CHECK(disposition != 'unmap' OR reason IS NOT NULL)
) STRICT;
CREATE TABLE holding_mapping (
 id INTEGER PRIMARY KEY,
 column_id INTEGER NOT NULL REFERENCES holding_column(id),
 table_pid TEXT NOT NULL,
 variable_pid TEXT NOT NULL,
 start INTEGER NOT NULL,
 end INTEGER NOT NULL,
 role TEXT NOT NULL CHECK(role IN ('row_key','time_start','time_stop','time_point','measure','code','label')),
 carried INTEGER NOT NULL CHECK(carried IN (0,1)),
 caveat TEXT,
 window TEXT,
 locator TEXT NOT NULL,
 CHECK(start < end),
 UNIQUE(column_id,table_pid,variable_pid,start,end)
) STRICT;
CREATE INDEX holding_mapping_logical ON holding_mapping(table_pid,variable_pid,start,end);
CREATE INDEX holding_column_name ON holding_column(holding_id,name);
CREATE TABLE holding_notice (
 id INTEGER PRIMARY KEY,
 holding_id INTEGER NOT NULL REFERENCES holding_table(id),
 column_id INTEGER REFERENCES holding_column(id),
 code TEXT NOT NULL,
 start INTEGER,
 end INTEGER,
 requested TEXT,
 documented TEXT,
 admitted TEXT,
 message TEXT NOT NULL,
 locator TEXT NOT NULL,
 CHECK((start IS NULL) = (end IS NULL)),
 CHECK(start IS NULL OR start < end)
) STRICT;
CREATE INDEX holding_notice_column ON holding_notice(column_id,code);
