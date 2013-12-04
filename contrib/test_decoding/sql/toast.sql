CREATE EXTENSION test_decoding;
-- predictability
SET synchronous_commit = on;

DROP TABLE IF EXISTS xpto;

SELECT 'init' FROM create_decoding_replication_slot('regression_slot', 'test_decoding');

SELECT setseed(0);
CREATE TABLE xpto (
    id serial primary key,
    toasted_col1 text,
    rand1 float8 DEFAULT random(),
    toasted_col2 text,
    rand2 float8 DEFAULT random()
);

-- uncompressed external toast data
INSERT INTO xpto (toasted_col1, toasted_col2) SELECT string_agg(g.i::text, ''), string_agg((g.i*2)::text, '') FROM generate_series(1, 2000) g(i);

-- compressed external toast data
INSERT INTO xpto (toasted_col2) SELECT repeat(string_agg(to_char(g.i, 'FM0000'), ''), 50) FROM generate_series(1, 500) g(i);

-- update of existing column
UPDATE xpto SET toasted_col1 = (SELECT string_agg(g.i::text, '') FROM generate_series(1, 2000) g(i)) WHERE id = 1;

UPDATE xpto SET rand1 = 123.456 WHERE id = 1;

DELETE FROM xpto WHERE id = 1;

DROP TABLE IF EXISTS toasted_key;
CREATE TABLE toasted_key (
    id serial,
    toasted_key text PRIMARY KEY,
    toasted_col1 text,
    toasted_col2 text
);

ALTER TABLE toasted_key ALTER COLUMN toasted_key SET STORAGE EXTERNAL;
ALTER TABLE toasted_key ALTER COLUMN toasted_col1 SET STORAGE EXTERNAL;

INSERT INTO toasted_key(toasted_key, toasted_col1) VALUES(repeat('1234567890', 200), repeat('9876543210', 200));

-- test update of a toasted key without changing it
UPDATE toasted_key SET toasted_col2 = toasted_col1;
-- test update of a toasted key, changing it
UPDATE toasted_key SET toasted_key = toasted_key || '1';

DELETE FROM toasted_key;

SELECT substr(data, 1, 200) FROM decoding_slot_get_changes('regression_slot', 'now', 'include-xids', '0');
SELECT drop_replication_slot('regression_slot');

DROP EXTENSION test_decoding;
