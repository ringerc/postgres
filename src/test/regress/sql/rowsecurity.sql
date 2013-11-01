--
-- Test of Row-level security feature
--

-- Clean up in case a prior regression run failed

-- Suppress NOTICE messages when users/groups don't exist
SET client_min_messages TO 'warning';

DROP USER IF EXISTS rls_regress_user0;
DROP USER IF EXISTS rls_regress_user1;
DROP USER IF EXISTS rls_regress_user2;

DROP SCHEMA IF EXISTS rls_regress_schema CASCADE;

RESET client_min_messages;

-- initial setup
CREATE USER rls_regress_user0;
CREATE USER rls_regress_user1;
CREATE USER rls_regress_user2;

CREATE SCHEMA rls_regress_schema;
GRANT ALL ON SCHEMA rls_regress_schema TO public;
SET search_path = rls_regress_schema;

-- setup of malicious function
CREATE OR REPLACE FUNCTION f_leak(text) RETURNS bool
    COST 0.0000001 LANGUAGE plpgsql
    AS 'BEGIN RAISE NOTICE ''f_leak => %'', $1; RETURN true; END';
GRANT EXECUTE ON FUNCTION f_leak(text) TO public;

-- BASIC Row-Level Security Scenario

SET SESSION AUTHORIZATION rls_regress_user0;
CREATE TABLE uaccount (
    pguser      name primary key,
    seclv       int
);
INSERT INTO uaccount VALUES
    ('rls_regress_user0', 99),
    ('rls_regress_user1',  1),
    ('rls_regress_user2',  2),
    ('rls_regress_user3',  3);
GRANT SELECT ON uaccount TO public;

CREATE TABLE category (
    cid         int primary key,
    cname       text
);
GRANT ALL ON category TO public;
INSERT INTO category VALUES
    (11, 'novel'),
    (22, 'science fiction'),
    (33, 'technology'),
    (44, 'manga');

CREATE TABLE document (
    did         int primary key,
    cid         int references category(cid),
    dlevel      int not null,
    dauthor     name,
    dtitle      text
);
GRANT ALL ON document TO public;
INSERT INTO document VALUES
    ( 1, 11, 1, 'rls_regress_user1', 'my first novel'),
    ( 2, 11, 2, 'rls_regress_user1', 'my second novel'),
    ( 3, 22, 2, 'rls_regress_user1', 'my science fiction'),
    ( 4, 44, 1, 'rls_regress_user1', 'my first manga'),
    ( 5, 44, 2, 'rls_regress_user1', 'my second manga'),
    ( 6, 22, 1, 'rls_regress_user2', 'great science fiction'),
    ( 7, 33, 2, 'rls_regress_user2', 'great technology book'),
    ( 8, 44, 1, 'rls_regress_user2', 'great manga');

-- user's security level must higher than or equal to document's one
ALTER TABLE document SET ROW SECURITY FOR ALL
    TO (dlevel <= (SELECT seclv FROM uaccount WHERE pguser = current_user));

-- viewpoint from rls_regress_user1
SET SESSION AUTHORIZATION rls_regress_user1;
SELECT * FROM document WHERE f_leak(dtitle);
SELECT * FROM document NATURAL JOIN category WHERE f_leak(dtitle);

-- viewpoint from rls_regress_user2
SET SESSION AUTHORIZATION rls_regress_user2;
SELECT * FROM document WHERE f_leak(dtitle);
SELECT * FROM document NATURAL JOIN category WHERE f_leak(dtitle);

EXPLAIN (costs off) SELECT * FROM document WHERE f_leak(dtitle);
EXPLAIN (costs off) SELECT * FROM document NATURAL JOIN category WHERE f_leak(dtitle);

-- only owner can change row-level security
ALTER TABLE document SET ROW SECURITY FOR ALL TO (true);     -- fail
ALTER TABLE document RESET ROW SECURITY FOR ALL;             -- fail

SET SESSION AUTHORIZATION rls_regress_user0;
ALTER TABLE document SET ROW SECURITY FOR ALL TO (dauthor = current_user);

-- viewpoint from rls_regress_user1 again
SET SESSION AUTHORIZATION rls_regress_user1;
SELECT * FROM document WHERE f_leak(dtitle);
SELECT * FROM document NATURAL JOIN category WHERE f_leak(dtitle);

-- viewpoint from rls_regress_user2 again
SET SESSION AUTHORIZATION rls_regress_user2;
SELECT * FROM document WHERE f_leak(dtitle);
SELECT * FROM document NATURAL JOIN category WHERE f_leak(dtitle);

EXPLAIN (costs off) SELECT * FROM document WHERE f_leak(dtitle);
EXPLAIN (costs off) SELECT * FROM document NATURAL JOIN category WHERE f_leak(dtitle);

-- interaction of FK/PK constraints
SET SESSION AUTHORIZATION rls_regress_user0;
ALTER TABLE category SET ROW SECURITY FOR ALL
    TO (CASE WHEN current_user = 'rls_regress_user1' THEN cid IN (11, 33)
        WHEN current_user = 'rls_regress_user2' THEN cid IN (22, 44)
        ELSE false END);

-- cannot delete PK referenced by invisible FK
SET SESSION AUTHORIZATION rls_regress_user1;
SELECT * FROM document d full outer join category c on d.cid = c.cid;
DELETE FROM category WHERE cid = 33;    -- fails with FK violation

-- cannot insert FK referencing invisible PK
SET SESSION AUTHORIZATION rls_regress_user2;
SELECT * FROM document d full outer join category c on d.cid = c.cid;
INSERT INTO document VALUES (10, 33, 1, current_user, 'hoge'); -- fail with FK violation

-- UNIQUE or PRIMARY KEY constraint violation DOES reveal presence of row
SET SESSION AUTHORIZATION rls_regress_user1;
INSERT INTO document VALUES ( 8, 44, 1, 'rls_regress_user_1', 'my third manga' ); -- Must fail with unique violation, revealing presence of did we can't see
SELECT * FROM document WHERE did = 8; -- and confirm we can't see it

-- UNIQUE or PRIMARY KEY constraint violation DOES reveal presence of row
SET SESSION AUTHORIZATION rls_regress_user1;
INSERT INTO document VALUES ( 8, 44, 1, 'rls_regress_user_1', 'my third manga' ); -- Must fail with unique violation, revealing presence of did we can't see
SELECT * FROM document WHERE did = 8; -- and confirm we can't see it

-- database superuser can bypass RLS policy
RESET SESSION AUTHORIZATION;
SELECT * FROM document;
SELECT * FROM category;

--
-- Table inheritance and RLS policy
--
SET SESSION AUTHORIZATION rls_regress_user0;

CREATE TABLE t1 (a int, junk1 text, b text) WITH OIDS;
ALTER TABLE t1 DROP COLUMN junk1;    -- just a disturbing factor
GRANT ALL ON t1 TO public;

COPY t1 FROM stdin WITH (oids);
101	1	aaa
102	2	bbb
103	3	ccc
104	4	ddd
\.

CREATE TABLE t2 (c float) INHERITS (t1);
COPY t2 FROM stdin WITH (oids);
201	1	abc	1.1
202	2	bcd	2.2
203	3	cde	3.3
204	4	def	4.4
\.

CREATE TABLE t3 (c text, b text, a int) WITH OIDS;
ALTER TABLE t3 INHERIT t1;
COPY t3(a,b,c) FROM stdin WITH (oids);
301	1	xxx	X
302	2	yyy	Y
303	3	zzz	Z
\.

ALTER TABLE t1 SET ROW SECURITY FOR ALL TO (a % 2 = 0); -- be even number
ALTER TABLE t2 SET ROW SECURITY FOR ALL TO (a % 2 = 1); -- be odd number

SELECT * FROM t1;
EXPLAIN (costs off) SELECT * FROM t1;

SELECT * FROM t1 WHERE f_leak(b);
EXPLAIN (costs off) SELECT * FROM t1 WHERE f_leak(b);

-- reference to system column
SELECT oid, * FROM t1;
EXPLAIN (costs off) SELECT * FROM t1;

-- reference to whole-row reference
SELECT *,t1 FROM t1;
EXPLAIN (costs off) SELECT *,t1 FROM t1;

-- for share/update lock
SELECT * FROM t1 FOR SHARE;
EXPLAIN (costs off) SELECT * FROM t1 FOR SHARE;

SELECT * FROM t1 WHERE f_leak(b) FOR SHARE;
EXPLAIN (costs off) SELECT * FROM t1 WHERE f_leak(b) FOR SHARE;

--
-- COPY TO statement 
--
COPY t1 TO stdout;
COPY t1 TO stdout WITH OIDS;
COPY t2(c,b) TO stdout WITH OIDS;
COPY (SELECT * FROM t1) TO stdout;
COPY document TO stdout WITH OIDS;	-- failed (no oid column)

--
-- recursive RLS and VIEWs in policy
--
CREATE TABLE s1 (a int, b text);
INSERT INTO s1 (SELECT x, md5(x::text) FROM generate_series(-10,10) x);

CREATE TABLE s2 (x int, y text);
INSERT INTO s2 (SELECT x, md5(x::text) FROM generate_series(-6,6) x);
CREATE VIEW v2 AS SELECT * FROM s2 WHERE y like '%af%';

ALTER TABLE s1 SET ROW SECURITY FOR ALL
   TO (a in (select x from s2 where y like '%2f%'));

ALTER TABLE s2 SET ROW SECURITY FOR ALL
   TO (x in (select a from s1 where b like '%22%'));

SELECT * FROM s1 WHERE f_leak(b);	-- fail (infinite recursion)

ALTER TABLE s2 SET ROW SECURITY FOR ALL TO (x % 2 = 0);

SELECT * FROM s1 WHERE f_leak(b);	-- OK
EXPLAIN SELECT * FROM only s1 WHERE f_leak(b);

ALTER TABLE s1 SET ROW SECURITY FOR ALL
   TO (a in (select x from v2));		-- using VIEW in RLS policy
SELECT * FROM s1 WHERE f_leak(b);	-- OK
EXPLAIN (COSTS OFF) SELECT * FROM s1 WHERE f_leak(b);

SELECT (SELECT x FROM s1 LIMIT 1) xx, * FROM s2 WHERE y like '%28%';
EXPLAIN (COSTS OFF) SELECT (SELECT x FROM s1 LIMIT 1) xx, * FROM s2 WHERE y like '%28%';

ALTER TABLE s2 SET ROW SECURITY FOR ALL
   TO (x in (select a from s1 where b like '%d2%'));
SELECT * FROM s1 WHERE f_leak(b);	-- fail (infinite recursion via view)

-- prepared statement with rls_regress_user0 privilege
PREPARE p1(int) AS SELECT * FROM t1 WHERE a <= $1;
EXECUTE p1(2);
EXPLAIN (costs off) EXECUTE p1(2);

-- superuser is allowed to bypass RLS checks
RESET SESSION AUTHORIZATION;
SELECT * FROM t1 WHERE f_leak(b);
EXPLAIN (costs off) SELECT * FROM t1 WHERE f_leak(b);

-- plan cache should be invalidated
EXECUTE p1(2);
EXPLAIN (costs off) EXECUTE p1(2);

PREPARE p2(int) AS SELECT * FROM t1 WHERE a = $1;
EXECUTE p2(2);
EXPLAIN (costs off) EXECUTE p2(2);

-- also, case when privilege switch from superuser
SET SESSION AUTHORIZATION rls_regress_user0;
EXECUTE p2(2);
EXPLAIN (costs off) EXECUTE p2(2);

--
-- UPDATE / DELETE and Row-level security
--
SET SESSION AUTHORIZATION rls_regress_user0;
EXPLAIN (costs off) UPDATE t1 SET b = b || b WHERE f_leak(b);
UPDATE t1 SET b = b || b WHERE f_leak(b);

EXPLAIN (costs off) UPDATE only t1 SET b = b || '_updt' WHERE f_leak(b);
UPDATE only t1 SET b = b || '_updt' WHERE f_leak(b);

-- returning clause with system column
UPDATE only t1 SET b = b WHERE f_leak(b) RETURNING oid, *, t1;
UPDATE t1 SET b = b WHERE f_leak(b) RETURNING *;
UPDATE t1 SET b = b WHERE f_leak(b) RETURNING oid, *, t1;

RESET SESSION AUTHORIZATION;
SELECT * FROM t1;

SET SESSION AUTHORIZATION rls_regress_user0;
EXPLAIN (costs off) DELETE FROM only t1 WHERE f_leak(b);
EXPLAIN (costs off) DELETE FROM t1 WHERE f_leak(b);

DELETE FROM only t1 WHERE f_leak(b) RETURNING oid, *, t1;
DELETE FROM t1 WHERE f_leak(b) RETURNING oid, *, t1;


----------------------------------------------------------------------
-- Check refcursors returned from PL/PgSQL SECURITY DEFINER functions

RESET SESSION AUTHORIZATION;

CREATE OR REPLACE FUNCTION return_refcursor_assuper() RETURNS refcursor AS $$
DECLARE
  curs1 refcursor;
BEGIN
  curs1 = 'super_cursor';
  OPEN curs1 FOR SELECT * FROM document;
  RETURN curs1;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;

-- Run the function entirely as rls_regress_user1
SET SESSION AUTHORIZATION rls_regress_user1;
BEGIN;
SELECT return_refcursor_assuper();
-- This fetch should return the full results, even though we are now
-- running as a user with much lower access according to the current
-- RLS policy.
FETCH ALL FROM "super_cursor";
-- But this should still return the usual result set
SELECT * FROM document;
ROLLBACK;

-- Do the same check where we return a refcursor from one RLS-affected
-- user to another RLS-affected user.

SET SESSION AUTHORIZATION rls_regress_user2;

CREATE OR REPLACE FUNCTION return_refcursor_asuser2() RETURNS refcursor AS $$
DECLARE
  curs1 refcursor;
BEGIN
  curs1 = 'user2_cursor';
  OPEN curs1 FOR SELECT * FROM document;
  RETURN curs1;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;

BEGIN;
SET SESSION AUTHORIZATION rls_regress_user1;
SELECT return_refcursor_asuser2();
-- Even though we're user1, we should see only user2's results from this.
-- This FAILS, returning user1's results.
FETCH ALL FROM "user2_cursor";
-- but user1's results for this
SELECT * FROM document;
ROLLBACK;

-- Now as the superuser, see if the SECURITY DEFINER on an RLS-affected
-- user filters the rows the superuser sees. It should, for consistency.

BEGIN;
RESET SESSION AUTHORIZATION;
SELECT return_refcursor_asuser2();
-- Should see user2's results, but FAILS, instead returning an empty result set (!)
FETCH ALL FROM "user2_cursor";
-- Should see superuser's results
SELECT * FROM document;
ROLLBACK;

--------------------------------------------------------------------
-- Tests of DECLARE and FETCH cursors during privilege
-- transitions.
-- 

-- Declare as user1, switch to user2, fetch all. Returns results as user2;
-- should be user1. FIXME.
BEGIN;
SET SESSION AUTHORIZATION rls_regress_user1;
DECLARE curs CURSOR FOR SELECT * FROM rls_regress_schema.document;
SET SESSION AUTHORIZATION rls_regress_user2;
FETCH ALL FROM curs;
ROLLBACK;

-- If we add an ORDER BY clause on a non-indexed column to force a sort,
-- still returns rows for user2 because execution didn't start. FIXME.
BEGIN;
SET SESSION AUTHORIZATION rls_regress_user1;
DECLARE curs CURSOR FOR SELECT * FROM rls_regress_schema.document ORDER BY cid;
SET SESSION AUTHORIZATION rls_regress_user2;
FETCH ALL FROM curs;
ROLLBACK;

-- If we add a single row FETCH before switching to force materialization
-- though, suddenly we see rows for user1. This is correct, just inconsistent.
BEGIN;
SET SESSION AUTHORIZATION rls_regress_user1;
DECLARE curs CURSOR FOR SELECT * FROM rls_regress_schema.document ORDER BY cid;
FETCH 1 FROM curs;
SET SESSION AUTHORIZATION rls_regress_user2;
FETCH ALL FROM curs;
ROLLBACK;

-- Remove the ORDER BY, and we get rows for user2 again, because the result set
-- isn't materialized anymore. FIXME.
BEGIN;
SET SESSION AUTHORIZATION rls_regress_user1;
DECLARE curs CURSOR FOR SELECT * FROM rls_regress_schema.document;
FETCH 1 FROM curs;
SET SESSION AUTHORIZATION rls_regress_user2;
FETCH ALL FROM curs;
ROLLBACK;


-- Perform similar tests with superuser.
BEGIN;
SET SESSION AUTHORIZATION rls_regress_user1;
DECLARE curs CURSOR FOR SELECT * FROM rls_regress_schema.document;
RESET SESSION AUTHORIZATION;
-- Should return user1 rows, returns none instead (FIXME)
FETCH ALL FROM curs;
ROLLBACK;

BEGIN;
SET SESSION AUTHORIZATION rls_regress_user2;
DECLARE curs CURSOR FOR SELECT * FROM rls_regress_schema.document;
FETCH 2 FROM curs;
RESET SESSION AUTHORIZATION;
-- Should see user2's rows; instead sees none due to username check change (FIXME)
FETCH ALL FROM curs;
ROLLBACK;

BEGIN;
RESET SESSION AUTHORIZATION;
DECLARE curs CURSOR FOR SELECT * FROM rls_regress_schema.document;
SET SESSION AUTHORIZATION rls_regress_user1;
-- Should return all rows and does so because it's planned as superuser before switch so rls qual not added
FETCH ALL FROM curs;
ROLLBACK;

BEGIN;
RESET SESSION AUTHORIZATION;
DECLARE curs CURSOR FOR SELECT * FROM rls_regress_schema.document;
FETCH 3 FROM curs;
SET SESSION AUTHORIZATION rls_regress_user2;
-- Still returns all rows because it's planned as superuser before switch, so rls qual not added
FETCH ALL FROM curs;
ROLLBACK;

----------------------------------------------------------------------
-- Test psql \dt+ command
--
ALTER TABLE category RESET ROW SECURITY FOR ALL;  -- too long qual
\dt+

----------------------------------------------------------------------
-- Clean up objects
--
RESET SESSION AUTHORIZATION;

DROP SCHEMA rls_regress_schema CASCADE;

DROP USER rls_regress_user0;
DROP USER rls_regress_user1;
DROP USER rls_regress_user2;
