SET synchronous_commit = on;

SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding');

CREATE SEQUENCE test_seq;

SELECT sum(nextval('test_seq')) FROM generate_series(1, 100);

ALTER SEQUENCE test_seq RESTART WITH 2;

SELECT sum(nextval('test_seq')) FROM generate_series(1, 100);

ALTER SEQUENCE test_seq INCREMENT BY 5;

SELECT sum(nextval('test_seq')) FROM generate_series(1, 100);

ALTER SEQUENCE test_seq CACHE 10000;

SELECT sum(nextval('test_seq')) FROM generate_series(1, 15000);

DROP SEQUENCE test_seq;

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1', 'include-sequences', '1');


CREATE SEQUENCE test2_seq MINVALUE 100 MAXVALUE 200 START 200 INCREMENT BY -1 CYCLE;

SELECT sum(nextval('test2_seq')) FROM generate_series(1, 300);

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1', 'include-sequences', '1');

DROP SEQUENCE test2_seq;


SELECT pg_drop_replication_slot('regression_slot');
