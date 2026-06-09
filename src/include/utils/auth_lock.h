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
	AUTH_LOCK_COOKIE = 2			/* not implemented in Phase 0 */
} AuthLockKind;

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
 * SQL-callable: pg_set_role_irrevocable(text), pg_set_session_authorization_irrevocable(text).
 * Declared here so they can be referenced from pg_proc.dat / fmgrtab.
 */
extern Datum pg_set_role_irrevocable(PG_FUNCTION_ARGS);
extern Datum pg_set_session_authorization_irrevocable(PG_FUNCTION_ARGS);
extern Datum pg_auth_lock_status(PG_FUNCTION_ARGS);

#endif							/* AUTH_LOCK_H */
