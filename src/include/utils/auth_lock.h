/*-------------------------------------------------------------------------
 *
 * auth_lock.h
 *	  Irrevocable / cookie-protected privilege-drop state for a backend.
 *
 *	  See doc commentary in src/backend/utils/init/auth_lock.c and the
 *	  design note "irrevocable-privilege-drop-design.md" in the project
 *	  workspace.  Phase 0 implements the IRREVOCABLE variant only.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/auth_lock.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AUTH_LOCK_H
#define AUTH_LOCK_H

#include "fmgr.h"

typedef enum AuthLockScope
{
	AUTH_LOCK_SCOPE_ROLE = 0,
	AUTH_LOCK_SCOPE_SESSION_AUTH = 1,

	AUTH_LOCK_NSCOPES				/* must be last */
} AuthLockScope;

typedef enum AuthLockKind
{
	AUTH_LOCK_NONE = 0,
	AUTH_LOCK_IRREVOCABLE = 1,
	AUTH_LOCK_COOKIE = 2
} AuthLockKind;

#define AUTH_LOCK_COOKIE_HASH_LEN	32	/* SHA-256 output size */
#define AUTH_LOCK_COOKIE_RAW_LEN	32	/* random bytes returned to caller */

/*
 * Install an IRREVOCABLE lock on the given scope.  This is one-way for
 * the lifetime of the backend.
 *
 * Caller is responsible for having validated that the session user is
 * allowed to become ceiling_role; this routine does no membership check
 * of its own.
 */
extern void AuthLockSetIrrevocable(AuthLockScope scope,
								   Oid ceiling_role,
								   bool ceiling_is_superuser);

/*
 * Install a cookie-protected lock on the given scope.  The caller has
 * already generated the cookie via pg_strong_random and hashed it via
 * SHA-256; only the hash is stored.  The raw cookie should be returned
 * to the legitimate holder exactly once and then scrubbed from
 * server-side memory.
 *
 * The same caller restriction applies as for AuthLockSetIrrevocable:
 * membership of ceiling_role must already have been verified.
 */
extern void AuthLockSetCookie(AuthLockScope scope,
							  Oid ceiling_role,
							  bool ceiling_is_superuser,
							  const uint8 hash[AUTH_LOCK_COOKIE_HASH_LEN]);

/*
 * Attempt to clear a cookie-protected lock by presenting the cookie's
 * hash.  Comparison is constant-time.  Returns true iff the lock was
 * actively COOKIE and the hash matched (in which case the scope's lock
 * is now NONE).  Returns false if the scope had no cookie lock OR the
 * hash did not match (in which case state is unchanged).
 *
 * Audit-log emission is the caller's responsibility — this routine
 * doesn't log because it doesn't know whether the caller is dispatching
 * a SQL function, protocol-level message, or batched multi-scope clear.
 */
extern bool AuthLockClearWithCookie(AuthLockScope scope,
									const uint8 hash[AUTH_LOCK_COOKIE_HASH_LEN]);

/*
 * Helper: hash the raw cookie bytes (caller-supplied buffer of arbitrary
 * length) into the 32-byte output.  Centralised so both set-time and
 * clear-time use identical hashing.
 */
extern void AuthLockHashCookie(const uint8 *raw, size_t raw_len,
							   uint8 out_hash[AUTH_LOCK_COOKIE_HASH_LEN]);

/*
 * Inspectors.
 */
extern AuthLockKind AuthLockGetKind(AuthLockScope scope);
extern Oid	AuthLockGetCeiling(AuthLockScope scope);
extern bool AuthLockGetCeilingIsSuperuser(AuthLockScope scope);

/*
 * Test whether changing the given scope's effective role to roleid would
 * cross the lock's ceiling.  Returns false if no lock is in effect, or if
 * roleid is reachable from the ceiling under membership rules.
 *
 * "Reachable" is defined by member_can_set_role(ceiling, roleid).  Used by
 * GUC check hooks to decide whether to reject a user-initiated change.
 *
 * For SET ROLE NONE (roleid = InvalidOid), the effective role becomes the
 * session user; this is treated as a violation if and only if the session
 * user is not itself reachable from the ceiling.
 */
extern bool AuthLockWouldViolate(AuthLockScope scope, Oid roleid);

/*
 * Used by GUC assign hooks during transaction-abort / GUC unwind: if the
 * requested role would violate the lock's ceiling, silently overwrite
 * *roleid / *is_superuser with the ceiling.  Emits a LOG line so audit
 * pipelines can detect the attempted escalation-via-rollback.
 *
 * Must NOT raise an ERROR: errors during transaction abort PANIC the
 * session.  Clipping silently is the only safe option in that context.
 */
extern void AuthLockClipRole(AuthLockScope scope,
							 Oid *roleid, bool *is_superuser);

/*
 * Layer-2 helper used by set_config_option_ext: returns true iff `name`
 * is "role" or "session_authorization" AND the corresponding scope is
 * locked, indicating that RESET (value == NULL) of that GUC must be
 * refused with an ERROR rather than silently allowed to reach the
 * assign hook (which would clip via the Layer-1 chokepoint, leaving
 * the GUC string out of sync with the effective identity).
 */
extern bool AuthLockBlocksReset(const char *name);

/*
 * Layer-2 helper used by DiscardCommand: returns true iff any lock is
 * currently in effect on any scope.  Used to refuse DISCARD ALL when
 * locked.
 */
extern bool AuthLockIsAnyActive(void);

/*
 * SQL-callable entry points.  Declared here so they can be referenced
 * from pg_proc.dat / fmgrtab.
 */
extern Datum pg_set_role_irrevocable(PG_FUNCTION_ARGS);
extern Datum pg_set_session_authorization_irrevocable(PG_FUNCTION_ARGS);
extern Datum pg_set_role_with_cookie(PG_FUNCTION_ARGS);
extern Datum pg_set_session_authorization_with_cookie(PG_FUNCTION_ARGS);
extern Datum pg_reset_role_with_cookie(PG_FUNCTION_ARGS);
extern Datum pg_reset_session_authorization_with_cookie(PG_FUNCTION_ARGS);
extern Datum pg_auth_lock_status(PG_FUNCTION_ARGS);

/*
 * Protocol-level handlers (Phase 4 / design §16).  Invoked from
 * PostgresMain's top-level message loop in response to the
 * V / v / U / u (AuthSetRole / AuthSetSession / AuthResetRole /
 * AuthResetSession) frontend tags.  Each handler validates the
 * channel is enabled, performs the operation via the same C-level
 * AuthLock API used by the SQL functions, and emits an
 * AuthLockResponse (Y) message back to the client.
 *
 * StringInfo is the partially-parsed message buffer; handlers consume
 * remaining fields and validate framing.
 */
struct StringInfoData;
extern void HandleAuthSetRoleMessage(struct StringInfoData *input, bool sas);
extern void HandleAuthResetRoleMessage(struct StringInfoData *input, bool sas);

/* Response status codes (used in AuthLockResponse messages). */
typedef enum AuthLockResponseStatus
{
	AUTH_LOCK_RESP_OK = 0,					/* operation succeeded */
	AUTH_LOCK_RESP_OK_COOKIE = 1,			/* succeeded + cookie returned */
	AUTH_LOCK_RESP_LOCK_PROTECTED = 2,		/* lock blocks this operation */
	AUTH_LOCK_RESP_CEILING_VIOLATION = 3,	/* target above ceiling */
	AUTH_LOCK_RESP_PERMISSION_DENIED = 4,	/* membership / SAS check failed */
	AUTH_LOCK_RESP_BAD_COOKIE = 5,			/* presented cookie didn't match */
	AUTH_LOCK_RESP_UNAVAILABLE = 6,			/* channel not negotiated */
} AuthLockResponseStatus;

#endif							/* AUTH_LOCK_H */
