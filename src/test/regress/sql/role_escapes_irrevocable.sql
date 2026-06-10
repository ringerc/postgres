--
-- role_escapes_irrevocable
--
-- Verification of pg_set_role_irrevocable: re-runs class-A
-- (direct restoration) and class-B (GUC rollback) escapes from
-- role_escapes.sql against a session locked by the IRREVOCABLE
-- variant.  Each escape that previously succeeded must now either
-- error or be silently clipped, with current_user remaining at the
-- locked role.
--
-- ----------------------------------------------------------------
-- Structural rule for this file: ONE TEST CASE PER CONNECTION.
--
-- Each test reconnects via \c so it sets up its locked state from a
-- clean session, runs ONE escape attempt, observes the result, and
-- ends.  This is important because the irrevocable lock is by
-- design single-use within a connection — RESET ROLE / DISCARD ALL
-- error or clip silently depending on which Phase 1 hook fired.
-- Test cases must not depend on being able to recover the session
-- state between scenarios.
--
-- ----------------------------------------------------------------
-- Evidence model: goal_post table
--
-- A shared goal_post table is created at top.  It is owned by
-- regress_irr_high, and INSERT is granted ONLY to regress_irr_high.
-- regress_irr_low and regress_irr_other have no grant.
--
-- After each escape attempt, the test issues an INSERT into
-- goal_post.  The INSERT runs under whatever current_user the
-- session ended up at:
--   * If the lock held (escape blocked or clipped), current_user is
--     regress_irr_low → INSERT fails with permission denied → no
--     row is recorded.
--   * If the lock had failed (escape succeeded), current_user would
--     be a privileged role with INSERT → row would be recorded.
--
-- The final score board at the end of the file lists any escape
-- vectors that succeeded.  A passing run records ZERO rows; any
-- row in the score board is a security-critical regression in the
-- lock's enforcement.
--
-- Classes C–G of role_escapes.sql are deliberately out of scope
-- here — they require attacker-controlled code to run as the
-- caller, which the IRREVOCABLE lock does not address (those
-- classes need orthogonal hardening — see role-isolation-
-- hardening-followups.md).
-- ----------------------------------------------------------------

\set VERBOSITY terse


-- ============================================================
-- Shared setup (one connection).  Roles + goal_post persist across
-- the \c reconnects below.
-- ============================================================
\c -
CREATE ROLE regress_irr_high SUPERUSER;
CREATE ROLE regress_irr_low NOSUPERUSER NOINHERIT;
CREATE ROLE regress_irr_other NOSUPERUSER NOINHERIT;
GRANT regress_irr_low   TO regress_irr_high;
GRANT regress_irr_other TO regress_irr_high;

CREATE SCHEMA regress_irr_high_schema AUTHORIZATION regress_irr_high;
GRANT USAGE ON SCHEMA regress_irr_high_schema TO PUBLIC;

CREATE TABLE regress_irr_high_schema.goal_post (
    achieved_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    achieved_by name NOT NULL,
    vector text NOT NULL,
    note text
);
REVOKE ALL ON regress_irr_high_schema.goal_post FROM PUBLIC;
GRANT INSERT ON regress_irr_high_schema.goal_post TO regress_irr_high;
-- regress_irr_low and regress_irr_other have NO grant.  If the lock
-- holds, any goal_post INSERT issued by the locked session errors
-- with "permission denied for table goal_post" — proof the lock
-- prevented the role from escaping to regress_irr_high.


-- ============================================================
-- A1.  RESET ROLE from a locked session.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_irr_high;
SELECT pg_set_role_irrevocable('regress_irr_low');
RESET ROLE;
INSERT INTO regress_irr_high_schema.goal_post(achieved_by, vector, note)
    VALUES (current_user, 'A1', 'RESET ROLE from locked session');


-- ============================================================
-- A2.  SET ROLE NONE from a locked session.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_irr_high;
SELECT pg_set_role_irrevocable('regress_irr_low');
SET ROLE NONE;
INSERT INTO regress_irr_high_schema.goal_post(achieved_by, vector, note)
    VALUES (current_user, 'A2', 'SET ROLE NONE from locked session');


-- ============================================================
-- A3.  SET ROLE to a sibling reachable via session_user but not
--      from the ceiling.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_irr_high;
SELECT pg_set_role_irrevocable('regress_irr_low');
SET ROLE regress_irr_other;
INSERT INTO regress_irr_high_schema.goal_post(achieved_by, vector, note)
    VALUES (current_user, 'A3', 'SET ROLE sibling from locked session');


-- ============================================================
-- A4.  SET ROLE to a strictly higher role.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_irr_high;
SELECT pg_set_role_irrevocable('regress_irr_low');
SET ROLE regress_irr_high;
INSERT INTO regress_irr_high_schema.goal_post(achieved_by, vector, note)
    VALUES (current_user, 'A4', 'SET ROLE high from locked session');


-- ============================================================
-- A5.  SET SESSION AUTHORIZATION (the other scope is also locked).
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_irr_high;
SELECT pg_set_role_irrevocable('regress_irr_low');
SET SESSION AUTHORIZATION regress_irr_high;
INSERT INTO regress_irr_high_schema.goal_post(achieved_by, vector, note)
    VALUES (current_user, 'A5', 'SAS from locked session');


-- ============================================================
-- A6.  RESET ROLE invoked from inside a function — the canonical
--      class-A attack pattern from role_escapes.sql.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_irr_high;
SELECT pg_set_role_irrevocable('regress_irr_low');
DO $$
BEGIN
    RESET ROLE;
    INSERT INTO regress_irr_high_schema.goal_post(achieved_by, vector, note)
        VALUES (current_user, 'A6', 'RESET ROLE inside DO block');
END;
$$;


-- ============================================================
-- B1.  Transaction abort rewinding SET LOCAL ROLE.
--      The chokepoint clip in SetCurrentRoleId silently corrects
--      the unwind target to the ceiling.  current_user stays at
--      regress_irr_low; INSERT fails.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_irr_high;
SELECT pg_set_role_irrevocable('regress_irr_low');
BEGIN;
SET LOCAL ROLE regress_irr_low;
DO $$ BEGIN RAISE EXCEPTION 'rollback triggers unwind'; END $$;
ROLLBACK;
INSERT INTO regress_irr_high_schema.goal_post(achieved_by, vector, note)
    VALUES (current_user, 'B1', 'transaction abort unwind from locked session');


-- ============================================================
-- B2.  set_config('role', ...) — Layer-2 set_config_option hook
--      errors before the role change reaches the chokepoint.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_irr_high;
SELECT pg_set_role_irrevocable('regress_irr_low');
SELECT set_config('role', 'regress_irr_high', false);
INSERT INTO regress_irr_high_schema.goal_post(achieved_by, vector, note)
    VALUES (current_user, 'B2', 'set_config(role) from locked session');


-- ============================================================
-- F1.  DISCARD ALL from a locked session.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_irr_high;
SELECT pg_set_role_irrevocable('regress_irr_low');
DISCARD ALL;
INSERT INTO regress_irr_high_schema.goal_post(achieved_by, vector, note)
    VALUES (current_user, 'F1', 'DISCARD ALL from locked session');


-- ============================================================
-- F2.  RESET role (lowercase, just the GUC).
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_irr_high;
SELECT pg_set_role_irrevocable('regress_irr_low');
RESET role;
INSERT INTO regress_irr_high_schema.goal_post(achieved_by, vector, note)
    VALUES (current_user, 'F2', 'RESET role lc from locked session');


-- ============================================================
-- F3.  set_config('role', NULL, false) — the deepest non-statement
--      RESET surface, covered by the set_config_option Layer-2
--      hook.
-- ============================================================
\c -
SET SESSION AUTHORIZATION regress_irr_high;
SELECT pg_set_role_irrevocable('regress_irr_low');
SELECT set_config('role', NULL, false);
INSERT INTO regress_irr_high_schema.goal_post(achieved_by, vector, note)
    VALUES (current_user, 'F3', 'set_config(role,NULL) from locked session');


-- ============================================================
-- L3.  Layer-3 observability check under abort-driven GUC unwind.
--      The previous test pattern relied on SELECT current_user /
--      current_setting('role') to detect drift between the GUC
--      string and the effective identity.  Under the goal_post
--      model, the equivalent check is: after the abort, INSERT
--      runs under the *effective* identity (clipped to ceiling);
--      it must error.  Drift detectable via current_setting
--      separately.
-- ============================================================
\c -
BEGIN;
SET LOCAL ROLE regress_irr_high;
SELECT pg_set_role_irrevocable('regress_irr_low');
DO $$ BEGIN RAISE EXCEPTION 'force abort'; END $$;
ROLLBACK;
-- current_setting reports truth (Layer 3); INSERT runs under the
-- effective identity (clipped, Layer 1).
SELECT current_setting('role') = current_user::text AS string_agrees_with_oid;
INSERT INTO regress_irr_high_schema.goal_post(achieved_by, vector, note)
    VALUES (current_user, 'L3', 'abort-driven unwind from locked session');


-- ============================================================
-- Score board (fresh connection, privileged).
-- ============================================================
-- Expected: zero rows.  Any row here is a regression in the
-- lock's enforcement — that test case's escape attempt SUCCEEDED
-- in writing to a privileged-owned table, meaning the lock failed
-- to constrain the session's effective privileges.
\c -
SELECT count(*) AS escapes_recorded FROM regress_irr_high_schema.goal_post;
SELECT vector, achieved_by, note FROM regress_irr_high_schema.goal_post
    ORDER BY vector;


-- ============================================================
-- Cleanup (fresh connection, superuser).
-- ============================================================
\c -
DROP SCHEMA regress_irr_high_schema CASCADE;
DROP ROLE regress_irr_low;
DROP ROLE regress_irr_other;
DROP ROLE regress_irr_high;
