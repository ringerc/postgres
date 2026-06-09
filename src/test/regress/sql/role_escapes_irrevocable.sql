--
-- role_escapes_irrevocable
--
-- Phase 0 verification of pg_set_role_irrevocable: re-runs the
-- direct-restoration (class A) and GUC-rollback (class B) escapes from
-- role_escapes.sql with the irrevocable lock installed.  Each escape
-- that previously succeeded must now either error or be silently
-- clipped, with current_user remaining at the locked role.
--
-- Classes C–G are deliberately out of scope here: they require the
-- caller to be high-privilege at the moment of attack, which the
-- IRREVOCABLE lock has by construction prevented.  Those classes need
-- orthogonal hardening and are covered by other tests / documentation.
--

\set VERBOSITY terse

CREATE ROLE regress_irr_high SUPERUSER;
CREATE ROLE regress_irr_low NOSUPERUSER NOINHERIT;
CREATE ROLE regress_irr_other NOSUPERUSER NOINHERIT;
GRANT regress_irr_low   TO regress_irr_high;
GRANT regress_irr_other TO regress_irr_high;

SET SESSION AUTHORIZATION regress_irr_high;
SELECT pg_set_role_irrevocable('regress_irr_low');
SELECT current_user AS locked;


-- A1.  RESET ROLE: silently clipped (assign_role) — GUC string may
--      revert to "none" but the effective role stays locked.
RESET ROLE;
SELECT current_user AS after_reset_role;             -- regress_irr_low

-- A2.  SET ROLE NONE: errored (check_role lock check fires).
SET ROLE NONE;                                       -- ERROR
SELECT current_user AS after_set_role_none;          -- regress_irr_low

-- A3.  SET ROLE to a sibling reachable via session_user but NOT via
--      the lock's ceiling.
SET ROLE regress_irr_other;                          -- ERROR
SELECT current_user AS after_set_role_sibling;       -- regress_irr_low

-- A4.  SET ROLE to a strictly higher role.
SET ROLE regress_irr_high;                           -- ERROR
SELECT current_user AS after_set_role_high;          -- regress_irr_low

-- A5.  SET SESSION AUTHORIZATION (the other scope is also locked).
SET SESSION AUTHORIZATION regress_irr_high;          -- ERROR
SELECT current_user AS after_set_sess_auth;          -- regress_irr_low


-- B1.  Transaction abort attempting to rewind a SET LOCAL to a higher role.
--      Since SET LOCAL above ceiling would already fail at check time,
--      this exercise verifies the assign_role clip path on the unwind
--      side via a SET LOCAL to the same role (no-op above ceiling but
--      a value gets stacked).
BEGIN;
SET LOCAL ROLE regress_irr_low;                      -- ok (at ceiling)
DO $$ BEGIN RAISE EXCEPTION 'rollback triggers unwind'; END $$;
ROLLBACK;
SELECT current_user AS after_rollback;               -- regress_irr_low

-- B2.  set_config() function variant.
SELECT set_config('role', 'regress_irr_high', false);   -- ERROR
SELECT current_user AS after_set_config;             -- regress_irr_low


-- F1.  DISCARD ALL: silently clipped (does not error, but role stays).
DISCARD ALL;
SELECT current_user AS after_discard_all;            -- regress_irr_low

-- F2.  RESET role (lowercase): also clipped.
RESET role;
SELECT current_user AS after_reset_role_guc;         -- regress_irr_low


-- A1 via inline DO block (the original A1 from role_escapes.sql).
-- The RESET ROLE inside the body is silently clipped on assign, so
-- current_user inside the block stays at the locked role.
DO $$
BEGIN
    RESET ROLE;
    RAISE NOTICE 'inside DO block, current_user = %', current_user;
END;
$$;
SELECT current_user AS after_do_block;               -- regress_irr_low


-- Cleanup.  The current session is locked, so we cannot RESET back
-- to a superuser identity in-band.  Reconnect to drop the locked
-- session, then clean up roles as the superuser.
\c -
DROP ROLE regress_irr_low;
DROP ROLE regress_irr_other;
DROP ROLE regress_irr_high;
