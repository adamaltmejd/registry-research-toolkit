-- One table per pair of storage classes, A's first. A column with no declared
-- type keeps each value's storage class.
CREATE TABLE int_text (v);
INSERT INTO int_text VALUES ('1');
CREATE TABLE int_blob (v);
INSERT INTO int_blob VALUES (X'31');
CREATE TABLE int_null (v);
INSERT INTO int_null VALUES (NULL);
CREATE TABLE text_blob (v);
INSERT INTO text_blob VALUES (X'31');
CREATE TABLE text_null (v);
INSERT INTO text_null VALUES (NULL);
CREATE TABLE blob_null (v);
INSERT INTO blob_null VALUES (NULL);
CREATE TABLE int_real (v);
INSERT INTO int_real VALUES (1.0);
