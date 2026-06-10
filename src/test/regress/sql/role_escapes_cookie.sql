--
-- role_escapes_cookie
--
-- Cookie-protected variant of the authorization lock.  Covers:
--   - cookie set returns a 32-byte bytea and installs lock on both
--     scopes with kind=cookie
--   - locked-state behaviour is identical to IRREVOCABLE: every
--     escape path errors or stays at ceiling (proven via goal_post)
--   - wrong cookie / bad length / NULL cookie are all refused
--     with a uniform "invalid auth_lock cookie" error
--   - correct cookie clears the lock and restores normal SET ROLE
--     behaviour, including the ability to write to goal_post once
--     escalation works again
--
-- Evidence model identical to role_escapes_irrevocable.sql: a
-- goal_post table owned by regress_cookie_high, with INSERT
-- granted ONLY to regress_cookie_high.  Locked-state escape
-- attempts record nothing (INSERT errors); successful unlock
-- leads to a recorded row, proving the unlock genuinely restored
-- escalation capability.
--
-- The cookie value is non-deterministic (random per call), so the
-- test uses \gset to capture and replay it; assertions are
-- structural rather than value-based.
--
-- Structured like role_escapes_irrevocable: one connection per
-- scenario via \c.

\set VERBOSITY terse

\c -
CREATE ROLE regress_cookie_high SUPERUSER;
CREATE ROLE regress_cookie_low NOSUPERUSER NOINHERIT;
GRANT regress_cookie_low TO regress_cookie_high;

CREATE SCHEMA regress_cookie_high_schema AUTHORIZATION regress_cookie_high;
GRANT USAGE ON SCHEMA regress_cookie_high_schema TO PUBLIC;

CREATE TABLE regress_cookie_high_schema.goal_post (
    achieved_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    achieved_by name NOT NULL,
    vector text NOT NULL,
    note text
);
REVOKE ALL ON regress_cookie_high_schema.goal_post FROM PUBLIC;
GRANT INSERT ON regress_cookie_high_schema.goal_post TO regress_cookie_high;
-- regress_cookie_low has no grant.  Locked-state INSERTs error.


-- ============================================================
-- C1.  Cookie set returns 32 bytes; both scopes show cookie lock.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_cookie_high;
SELECT octet_length(pg_set_role_with_cookie('regress_cookie_low')) AS c1_cookie_len;
SELECT scope, lock_kind, ceiling_role_name, cookie_outstanding
    FROM pg_auth_lock_status() ORDER BY scope;


-- ============================================================
-- C2.  Lock blocks the same escape paths as IRREVOCABLE.  Each
--      sub-case attempts goal_post INSERT after the escape try;
--      INSERT must error with permission denied.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_cookie_high;
SELECT octet_length(pg_set_role_with_cookie('regress_cookie_low')) AS c2a_cookie_len;
RESET ROLE;
INSERT INTO regress_cookie_high_schema.goal_post(achieved_by, vector, note)
    VALUES (current_user, 'C2a', 'RESET ROLE locked');

\c -
SET SESSION AUTHORIZATION regress_cookie_high;
SELECT octet_length(pg_set_role_with_cookie('regress_cookie_low')) AS c2b_cookie_len;
DISCARD ALL;
INSERT INTO regress_cookie_high_schema.goal_post(achieved_by, vector, note)
    VALUES (current_user, 'C2b', 'DISCARD ALL locked');

\c -
SET SESSION AUTHORIZATION regress_cookie_high;
SELECT octet_length(pg_set_role_with_cookie('regress_cookie_low')) AS c2c_cookie_len;
SET ROLE regress_cookie_high;
INSERT INTO regress_cookie_high_schema.goal_post(achieved_by, vector, note)
    VALUES (current_user, 'C2c', 'SET ROLE high locked');


-- ============================================================
-- C3.  Wrong cookie -> error, lock preserved.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_cookie_high;
SELECT octet_length(pg_set_role_with_cookie('regress_cookie_low')) AS c3_cookie_len;
SELECT pg_reset_role_with_cookie(
    '\x00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff'::bytea);
SELECT lock_kind FROM pg_auth_lock_status() WHERE scope='role';
-- Lock should still be COOKIE; further escape attempts still blocked.
INSERT INTO regress_cookie_high_schema.goal_post(achieved_by, vector, note)
    VALUES (current_user, 'C3', 'after bad cookie attempt');


-- ============================================================
-- C4.  Cookie of wrong length -> error.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_cookie_high;
SELECT octet_length(pg_set_role_with_cookie('regress_cookie_low')) AS c4_cookie_len;
SELECT pg_reset_role_with_cookie('\xdeadbeef'::bytea);
SELECT pg_reset_role_with_cookie(''::bytea);


-- ============================================================
-- C5.  NULL cookie -> error.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_cookie_high;
SELECT octet_length(pg_set_role_with_cookie('regress_cookie_low')) AS c5_cookie_len;
SELECT pg_reset_role_with_cookie(NULL);


-- ============================================================
-- C6.  Correct cookie clears the lock; subsequent SET ROLE works
--      and the now-escalated role can write to goal_post,
--      proving the unlock genuinely restored escalation capability.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_cookie_high;
SELECT pg_set_role_with_cookie('regress_cookie_low') AS c6_cookie \gset
-- pre-unlock: INSERT fails because session is locked at low.
INSERT INTO regress_cookie_high_schema.goal_post(achieved_by, vector, note)
    VALUES (current_user, 'C6.pre', 'pre-unlock attempt');
SELECT pg_reset_role_with_cookie(:'c6_cookie'::bytea);
SELECT lock_kind FROM pg_auth_lock_status() ORDER BY scope;
-- post-unlock: role manipulation works; INSERT as escalated role
-- succeeds.  Recorded in goal_post.
RESET ROLE;
INSERT INTO regress_cookie_high_schema.goal_post(achieved_by, vector, note)
    VALUES (current_user, 'C6.post', 'after cookie-clear unlock');


-- ============================================================
-- C7.  Cookie is one-shot.  Re-presenting after a successful
--      reset must fail (no COOKIE-kind lock remains on either
--      scope).
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_cookie_high;
SELECT pg_set_role_with_cookie('regress_cookie_low') AS c7_cookie \gset
SELECT pg_reset_role_with_cookie(:'c7_cookie'::bytea);
-- First reset succeeded.  Second presentation must fail.
SELECT pg_reset_role_with_cookie(:'c7_cookie'::bytea);


-- ============================================================
-- C8.  Cannot install a second lock on top of an existing one.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_cookie_high;
SELECT octet_length(pg_set_role_with_cookie('regress_cookie_low')) AS c8_first_len;
-- Second attempt errors (lock already in effect on role scope).
SELECT octet_length(pg_set_role_with_cookie('regress_cookie_low')) AS c8_second_len;
-- Same applies to IRREVOCABLE alongside an existing cookie lock.
SELECT pg_set_role_irrevocable('regress_cookie_low');


-- ============================================================
-- Score board (fresh connection, privileged).
-- ============================================================
-- Expected rows: exactly one ('C6.post'), proving the cookie
-- unlock genuinely restored escalation capability.  Any OTHER
-- vector appearing here is a regression: a locked session should
-- not have been able to write to goal_post.
\c -
SELECT vector, achieved_by, note FROM regress_cookie_high_schema.goal_post
    ORDER BY vector;


-- ============================================================
-- Cleanup
-- ============================================================
\c -
DROP SCHEMA regress_cookie_high_schema CASCADE;
DROP ROLE regress_cookie_low;
DROP ROLE regress_cookie_high;
