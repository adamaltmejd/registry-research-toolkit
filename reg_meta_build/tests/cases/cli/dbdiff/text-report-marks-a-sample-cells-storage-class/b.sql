-- A column with no declared type keeps each value's storage class: text '1' here.
CREATE TABLE int_text (v);
INSERT INTO int_text VALUES ('1');
