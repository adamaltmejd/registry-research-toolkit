-- An ordinary curated derivation whose object is in the paired reference catalog.
-- Only public ids cross the file boundary; statement ids belong to this file.
CREATE TABLE IF NOT EXISTS holding_relation (
 id INTEGER PRIMARY KEY,
 kind TEXT NOT NULL CHECK(kind='derived_from'),
 subject_kind TEXT NOT NULL CHECK(subject_kind IN ('register','table','variable')),
 subject_pid TEXT NOT NULL,
 object_kind TEXT NOT NULL CHECK(object_kind IN ('register','variable')),
 object_pid TEXT NOT NULL,
 at_start INTEGER,
 at_end INTEGER,
 statement_id INTEGER NOT NULL REFERENCES statement(id),
 caveat TEXT,
 CHECK((at_start IS NULL)=(at_end IS NULL)),
 CHECK(at_start IS NULL OR at_start<at_end),
 UNIQUE(subject_kind,subject_pid,object_kind,object_pid,at_start,at_end)
) STRICT;
CREATE INDEX IF NOT EXISTS holding_relation_subject ON holding_relation(subject_pid,kind);
CREATE INDEX IF NOT EXISTS holding_relation_object ON holding_relation(object_pid,kind);
