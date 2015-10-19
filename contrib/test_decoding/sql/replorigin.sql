-- predictability
SET synchronous_commit = on;

CREATE TABLE origin_tbl(id serial primary key, data text);
CREATE TABLE target_tbl(id serial primary key, data text);

SELECT pg_replication_origin_create('test_decoding: regression_slot');
-- ensure duplicate creations fail
SELECT pg_replication_origin_create('test_decoding: regression_slot');

-- Create a second origin too
SELECT pg_replication_origin_create('test_decoding: regression_slot 2');

--ensure deletions work (once)
SELECT pg_replication_origin_create('test_decoding: temp');
SELECT pg_replication_origin_drop('test_decoding: temp');
SELECT pg_replication_origin_drop('test_decoding: temp');

SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding');

-- origin tx
INSERT INTO origin_tbl(data) VALUES ('will be replicated and decoded and decoded again');
INSERT INTO target_tbl(data)
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');

-- as is normal, the insert into target_tbl shows up
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');

INSERT INTO origin_tbl(data) VALUES ('will be replicated, but not decoded again');

-- mark session as replaying
SELECT pg_replication_origin_session_setup('test_decoding: regression_slot');

-- ensure we prevent duplicate setup
SELECT pg_replication_origin_session_setup('test_decoding: regression_slot');

BEGIN;
-- setup transaction origin
SELECT pg_replication_origin_xact_setup('0/aabbccdd', '2013-01-01 00:00');
INSERT INTO target_tbl(data)
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1', 'only-local', '1');
COMMIT;

-- check replication progress for the session is correct
SELECT pg_replication_origin_session_progress(false);
SELECT pg_replication_origin_session_progress(true);

SELECT pg_replication_origin_session_reset();

SELECT local_id, external_id, remote_lsn, local_lsn <> '0/0' FROM pg_replication_origin_status;

-- check replication progress identified by name is correct
SELECT pg_replication_origin_progress('test_decoding: regression_slot', false);
SELECT pg_replication_origin_progress('test_decoding: regression_slot', true);

-- ensure reset requires previously setup state
SELECT pg_replication_origin_session_reset();

-- and magically the replayed xact will be filtered!
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1', 'only-local', '1');

--but new original changes still show up
INSERT INTO origin_tbl(data) VALUES ('will be replicated');
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1',  'only-local', '1');

-- This time we'll prepare a series of transactions to decode in one go
-- rather than decoding one by one.
--
-- First, to make sure remote-originated tx's are not filtered out when only-local is unset,
-- we need another tx with an origin. This time we'll set a different origin for each
-- change.
SELECT pg_replication_origin_session_setup('test_decoding: regression_slot');
BEGIN;
SELECT pg_replication_origin_xact_setup('0/aabbccff', '2013-01-01 00:10');
INSERT INTO origin_tbl(data) VALUES ('will be replicated even though remotely originated');
SELECT pg_replication_origin_xact_setup('0/aabbcd00', '2013-01-01 00:11');
INSERT INTO origin_tbl(data) VALUES ('will also be replicated even though remotely originated');
-- Change the origin replication identifier mid-transaction
SELECT pg_replication_origin_session_reset();
SELECT pg_replication_origin_session_setup('test_decoding: regression_slot 2');
SELECT pg_replication_origin_xact_setup('0/aabbcd01', '2013-01-01 00:13');
INSERT INTO origin_tbl(data) VALUES ('from second origin');
COMMIT;

-- then run an empty tx, since this test will be setting skip-empty-xacts=0
-- Note that we need to do something, just something that won't get decoded,
-- to force a commit to be recorded.
BEGIN;
SELECT pg_replication_origin_xact_setup('1/aabbccff', '2013-01-01 00:20');
CREATE TEMPORARY TABLE test_empty_tx(blah integer);
COMMIT;

-- Decode with options not otherwise tested - skip empty xacts on, only-local off. Can't include xids since they'll change each run.
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '0',  'only-local', '0', 'include-origins', '1');

SELECT pg_replication_origin_session_reset();

SELECT pg_drop_replication_slot('regression_slot');
SELECT pg_replication_origin_drop('test_decoding: regression_slot');
