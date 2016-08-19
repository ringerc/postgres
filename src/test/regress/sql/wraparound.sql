-- We need to be able to force vacuuming of template0
UPDATE pg_database
SET datallowconn = true
WHERE datname = 'template0';

-- For debugging these tests you'll find something like this useful:
-- SELECT txid_current() \gset
-- SELECT BIGINT :'txid_current' AS txid,
--       BIGINT :'txid_current' >> 32 AS epoch,
--       BIGINT :'txid_current' & 4294967295 AS xid32;

SELECT txid_current() AS start_epoch0 \gset


SET client_min_messages = 'error';
SELECT txid_incinerate('stop', 1000);

-- Should be near UINT32_MAX/2 now
SELECT txid_current() \gset
SELECT BIGINT :'txid_current' > (BIGINT '1' << 31) - (BIGINT '1' << 30);
SELECT BIGINT :'txid_current' < (BIGINT '1' << 31) + (BIGINT '1' << 30);
SELECT BIGINT :'txid_current' >> 32 AS txid_current_epoch;

\c template0
SET client_min_messages = 'error';
VACUUM FREEZE;

\c template1
SET client_min_messages = 'error';
VACUUM FREEZE;

\c postgres
SET client_min_messages = 'error';
VACUUM FREEZE;

\c regression
SET client_min_messages = 'error';
VACUUM FREEZE;

CHECKPOINT;

select min(datfrozenxid::text::bigint) AS min_datfrozenxid from pg_database \gset
SELECT BIGINT :'min_datfrozenxid' > (BIGINT '1' << 31) - (BIGINT '1' << 30);
SELECT BIGINT :'min_datfrozenxid' < (BIGINT '1' << 31) + (BIGINT '1' << 30);






-- That got us to nearly UINT32_MAX/2, another run will get us to near UINT32_MAX
SELECT txid_current() AS mid_epoch0 \gset
SET client_min_messages = 'error';
SELECT txid_incinerate('stop', 1000);
SELECT txid_current() \gset
SELECT BIGINT :'txid_current' > (BIGINT '1' << 31) + (BIGINT '1' << 30);
SELECT BIGINT :'txid_current' < (BIGINT '1' << 32);
SELECT BIGINT :'txid_current' >> 32 AS txid_current_epoch;

\c template0
SET client_min_messages = 'error';
VACUUM FREEZE;

\c template1
SET client_min_messages = 'error';
VACUUM FREEZE;

\c postgres
SET client_min_messages = 'error';
VACUUM FREEZE;

\c regression
SET client_min_messages = 'error';
VACUUM FREEZE;

CHECKPOINT;

select min(datfrozenxid::text::bigint) AS min_datfrozenxid from pg_database \gset
SELECT BIGINT :'min_datfrozenxid' > (BIGINT '1' << 31) + (BIGINT '1' << 30);
SELECT BIGINT :'min_datfrozenxid' < (BIGINT '1' << 32);




-- We should be near UINT32_MAX now, so the next run will
-- bring us across the epoch boundary.
SELECT txid_current() AS end_epoch0 \gset
SET client_min_messages = 'error';
SELECT txid_incinerate('stop', 1000);
SELECT txid_current() \gset
SELECT BIGINT :'txid_current' > (BIGINT '1' << 32);
SELECT BIGINT :'txid_current' >> 32 AS txid_current_epoch;

\c template0
SET client_min_messages = 'error';
VACUUM FREEZE;

\c template1
SET client_min_messages = 'error';
VACUUM FREEZE;

\c postgres
SET client_min_messages = 'error';
VACUUM FREEZE;

CHECKPOINT;

\c regression
SET client_min_messages = 'error';
VACUUM FREEZE;


UPDATE pg_database
SET datallowconn = false
WHERE datname = 'template0';


SELECT txid_current() AS start_epoch1 \gset

-- Make sure our txid functions handle the epoch wrap
SELECT txid_convert_if_recent(BIGINT :'start_epoch0');
SELECT txid_convert_if_recent(BIGINT :'mid_epoch0');
SELECT txid_convert_if_recent(BIGINT :'end_epoch0');
SELECT txid_convert_if_recent(BIGINT :'start_epoch1');

SELECT txid_status(BIGINT :'start_epoch0');
SELECT txid_status(BIGINT :'mid_epoch0');
SELECT txid_status(BIGINT :'end_epoch0');
SELECT txid_status(BIGINT :'start_epoch1');

-- Hunt for the clog truncation boundary, which will be somewhere well
-- short of wraparound.
SET log_statement = 'all';
SET log_min_messages = 'debug1';
SELECT txid_status(BIGINT :'start_epoch1' - 1000);
SELECT txid_status(BIGINT :'start_epoch1' - 10000);
SELECT txid_status(BIGINT :'start_epoch1' - 100000);
SELECT txid_status(BIGINT :'start_epoch1' - 1000000);
SELECT txid_status(BIGINT :'start_epoch1' - 10000000);
SELECT txid_status(BIGINT :'start_epoch1' - 100000000);
