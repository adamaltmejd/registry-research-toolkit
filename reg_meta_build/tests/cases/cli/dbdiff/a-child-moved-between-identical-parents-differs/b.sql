-- a.sql with the register's id shifted (1 -> 7), and the population and the
-- version note moved from version 1 to version 2.
CREATE TABLE provider (slug TEXT PRIMARY KEY, name TEXT NOT NULL);
INSERT INTO provider VALUES ('scb', 'SCB');
CREATE TABLE register (
  register_id INTEGER PRIMARY KEY,
  provider TEXT NOT NULL REFERENCES provider(slug),
  slug TEXT NOT NULL,
  UNIQUE (provider, slug)
);
INSERT INTO register VALUES (7, 'scb', 'lisa');
CREATE TABLE version (
  version_id INTEGER PRIMARY KEY,
  register_id INTEGER NOT NULL REFERENCES register(register_id),
  name TEXT
);
INSERT INTO version VALUES (1, 7, '2020'), (2, 7, '2020');
CREATE TABLE population (
  version_id INTEGER NOT NULL REFERENCES version(version_id),
  name TEXT NOT NULL
);
INSERT INTO population VALUES (2, 'persons');
CREATE TABLE version_note (
  version_id INTEGER PRIMARY KEY REFERENCES version(version_id),
  note TEXT NOT NULL
);
INSERT INTO version_note VALUES (2, 'x');
