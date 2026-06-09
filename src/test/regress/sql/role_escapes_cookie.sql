--
-- role_escapes_cookie
--
-- Cookie-protected variant of the authorization lock.  Covers:
--   - cookie set returns a 32-byte bytea and installs lock on both
--     scopes with kind=cookie
--   - locked-state behaviour is identical to IRREVOCABLE: every
--     escape path errors or stays at ceiling
--   - wrong cookie / bad length / NULL cookie are all refused
--     with a uniform "invalid auth_lock cookie" error
--   - correct cookie clears the lock and restores normal SET ROLE
--     behaviour
--
-- Note: the cookie value is non-deterministic (random per call),
-- so the test must use \gset to capture and replay it; the test
-- assertions are length-only / structural rather than value-based.
--
-- Structured like role_escapes_irrevocable: one connection per
-- scenario via \c, so future hardening that further constrains
-- in-session behaviour won't break test isolation.

\set VERBOSITY terse

\c -
CREATE ROLE regress_cookie_high SUPERUSER;
CREATE ROLE regress_cookie_low NOSUPERUSER NOINHERIT;
GRANT regress_cookie_low TO regress_cookie_high;


-- ============================================================
-- C1.  Cookie set returns 32 bytes; both scopes show cookie lock.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_cookie_high;
SELECT octet_length(pg_set_role_with_cookie('regress_cookie_low')) AS c1_cookie_len;
SELECT scope, lock_kind, ceiling_role_name, cookie_outstanding
    FROM pg_auth_lock_status() ORDER BY scope;
SELECT current_user AS c1_current_user;


-- ============================================================
-- C2.  Lock blocks the same escape paths as IRREVOCABLE.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_cookie_high;
SELECT octet_length(pg_set_role_with_cookie('regress_cookie_low')) AS c2_cookie_len;
RESET ROLE;
SELECT current_user AS c2_after_reset_role;

\c -
SET SESSION AUTHORIZATION regress_cookie_high;
SELECT octet_length(pg_set_role_with_cookie('regress_cookie_low')) AS c2b_cookie_len;
DISCARD ALL;
SELECT current_user AS c2b_after_discard_all;

\c -
SET SESSION AUTHORIZATION regress_cookie_high;
SELECT octet_length(pg_set_role_with_cookie('regress_cookie_low')) AS c2c_cookie_len;
SET ROLE regress_cookie_high;
SELECT current_user AS c2c_after_set_high;


-- ============================================================
-- C3.  Wrong cookie -> error, lock preserved.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_cookie_high;
SELECT octet_length(pg_set_role_with_cookie('regress_cookie_low')) AS c3_cookie_len;
SELECT pg_reset_role_with_cookie(
    '\x00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff'::bytea);
SELECT current_user AS c3_after_bad_cookie;
SELECT lock_kind FROM pg_auth_lock_status() WHERE scope='role';


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
-- C6.  Correct cookie clears the lock; subsequent SET ROLE works.
--      \gset captures the cookie literal for replay in the same
--      connection.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_cookie_high;
SELECT pg_set_role_with_cookie('regress_cookie_low') AS c6_cookie \gset
SELECT current_user AS c6_locked;
SELECT pg_reset_role_with_cookie(:'c6_cookie'::bytea);
SELECT lock_kind FROM pg_auth_lock_status() ORDER BY scope;
-- After unlock, normal role manipulation works again.
RESET ROLE;
SELECT current_user AS c6_after_reset;
SET ROLE regress_cookie_low;
SELECT current_user AS c6_after_set_low;


-- ============================================================
-- C7.  Cookie is one-shot: re-presenting after a successful
--      reset fails (because the lock is no longer COOKIE-kind).
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
-- Cleanup
-- ============================================================
\c -
DROP ROLE regress_cookie_low;
DROP ROLE regress_cookie_high;
