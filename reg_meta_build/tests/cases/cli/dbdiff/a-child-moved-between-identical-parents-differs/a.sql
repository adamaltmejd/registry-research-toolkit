-- `provider` is keyed by TEXT: a natural key, never a surrogate id.
CREATE TABLE provider (slug TEXT PRIMARY KEY, name TEXT NOT NULL);
INSERT INTO provider VALUES ('scb', 'SCB');
CREATE TABLE register (
  register_id INTEGER PRIMARY KEY,
  provider TEXT NOT NULL REFERENCES provider(slug),
  slug TEXT NOT NULL,
  UNIQUE (provider, slug)
);
INSERT INTO register VALUES (1, 'scb', 'lisa');
-- Two versions equal but for their id: no key tells them apart.
CREATE TABLE version (
  version_id INTEGER PRIMARY KEY,
  register_id INTEGER NOT NULL REFERENCES register(register_id),
  name TEXT
);
INSERT INTO version VALUES (1, 1, '2020'), (2, 1, '2020');
CREATE TABLE population (
  version_id INTEGER NOT NULL REFERENCES version(version_id),
  name TEXT NOT NULL
);
INSERT INTO population VALUES (1, 'persons');
-- A child whose primary key is its reference: compared by what it references.
CREATE TABLE version_note (
  version_id INTEGER PRIMARY KEY REFERENCES version(version_id),
  note TEXT NOT NULL
);
INSERT INTO version_note VALUES (1, 'x');
