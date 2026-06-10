--
-- role_escapes_grammar
--
-- Exercises the SQL grammar forms added in Phase 3 alongside the
-- function-style API tested by role_escapes_irrevocable.sql and
-- role_escapes_cookie.sql.  Same security invariants apply; this
-- file only verifies the grammar is wired correctly to the
-- AuthLock dispatch in ExecSetVariableStmt.
--
-- Grammar additions covered:
--   SET ROLE x IRREVOCABLE
--   SET ROLE x WITH COOKIE
--   SET SESSION AUTHORIZATION x IRREVOCABLE
--   SET SESSION AUTHORIZATION x WITH COOKIE
--   RESET ROLE WITH COOKIE 'literal'
--   RESET SESSION AUTHORIZATION WITH COOKIE 'literal'
--
-- The cookie returned by the SET ... WITH COOKIE grammar form is
-- emitted as a NOTICE (function form is preferred for programmatic
-- capture by drivers).  Test captures it indirectly by using the
-- function form, then exercises the RESET grammar to clear.

\set VERBOSITY terse

\c -
CREATE ROLE regress_gram_high SUPERUSER;
CREATE ROLE regress_gram_low NOSUPERUSER NOINHERIT;
CREATE ROLE regress_gram_other NOSUPERUSER NOINHERIT;
GRANT regress_gram_low TO regress_gram_high;
GRANT regress_gram_other TO regress_gram_high;


-- ============================================================
-- G1.  SET ROLE x IRREVOCABLE installs the lock; downstream
--      escape attempts are refused.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_gram_high;
SET ROLE regress_gram_low IRREVOCABLE;
SELECT current_user AS g1_after_lock;
SELECT scope, lock_kind, ceiling_role_name FROM pg_auth_lock_status()
    ORDER BY scope;
-- Escape attempts must be rejected by the lock.
SET ROLE regress_gram_high;
RESET ROLE;


-- ============================================================
-- G2.  SET SESSION AUTHORIZATION x IRREVOCABLE.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_gram_high;
SET SESSION AUTHORIZATION regress_gram_low IRREVOCABLE;
SELECT current_user AS g2_after_lock;
SELECT lock_kind FROM pg_auth_lock_status() ORDER BY scope;
SET SESSION AUTHORIZATION regress_gram_high;


-- ============================================================
-- G3.  SET ROLE x WITH COOKIE emits the cookie via NOTICE and
--      installs the lock.  Cookie format is bytea hex.
--
--      We suppress the NOTICE during the test because the cookie
--      value is non-deterministic; we only verify the lock got
--      installed.  See G4 for the function-captured-then-grammar-
--      reset round-trip that exercises a deterministic cookie.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_gram_high;
SET client_min_messages = warning;
SET ROLE regress_gram_low WITH COOKIE;
RESET client_min_messages;
SELECT lock_kind, cookie_outstanding FROM pg_auth_lock_status() ORDER BY scope;


-- ============================================================
-- G4.  Round-trip: function-form set → grammar-form reset.
--      Verifies the grammar RESET ROLE WITH COOKIE 'literal'
--      accepts the same cookie format the function-form set
--      produces (bytea ::text format = "\x0123...").
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_gram_high;
SELECT pg_set_role_with_cookie('regress_gram_low')::text AS c \gset
SELECT current_user AS g4_locked;
-- Lock is in effect; SET ROLE high errors.
SET ROLE regress_gram_high;
-- Reset with the captured cookie via grammar form.
RESET ROLE WITH COOKIE :'c';
SELECT lock_kind FROM pg_auth_lock_status() ORDER BY scope;
-- Lock cleared.  Role manipulation works again.
SET ROLE regress_gram_high;
SELECT current_user AS g4_unlocked;


-- ============================================================
-- G5.  Round-trip for SAS via grammar form.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_gram_high;
SELECT pg_set_session_authorization_with_cookie('regress_gram_low')::text AS c \gset
SELECT current_user AS g5_locked;
RESET SESSION AUTHORIZATION WITH COOKIE :'c';
SELECT lock_kind FROM pg_auth_lock_status() ORDER BY scope;


-- ============================================================
-- G6.  Grammar error cases.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_gram_high;
-- SET LOCAL ... IRREVOCABLE is rejected (would have weird
-- lifetime semantics).
SET LOCAL ROLE regress_gram_low IRREVOCABLE;
-- Bad cookie format in grammar reset.
SET SESSION AUTHORIZATION regress_gram_high;
SELECT pg_set_role_with_cookie('regress_gram_low')::text AS dummy \gset
RESET ROLE WITH COOKIE 'not_hex_format';
-- Wrong cookie value (well-formed hex but doesn't match).
RESET ROLE WITH COOKIE '\x0000000000000000000000000000000000000000000000000000000000000000';


-- ============================================================
-- G7.  pg_stat_statements-style query jumbling: the cookie
--      literal must NOT be part of the query identity.
--      Verified structurally — two RESET ROLE WITH COOKIE
--      statements with different cookies normalize to the same
--      jumbled identity because args are not jumbled when
--      jumble_args=false (the default).
--
--      We don't load pg_stat_statements here (it's not in core).
--      The jumble check is structural only: the auth_lock_kind
--      field IS jumbled so "SET ROLE x" and "SET ROLE x
--      IRREVOCABLE" are distinct queries.  See
--      queryjumblefuncs.c _jumbleVariableSetStmt.
-- ============================================================


-- ============================================================
-- Cleanup
-- ============================================================
\c -
DROP ROLE regress_gram_low;
DROP ROLE regress_gram_other;
DROP ROLE regress_gram_high;
