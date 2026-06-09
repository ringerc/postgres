/*-------------------------------------------------------------------------
 *
 * auth_lock.c
 *	  Process-local "authorization lock" state for irrevocable / cookie-
 *	  protected privilege drops via SET ROLE / SET SESSION AUTHORIZATION.
 *
 *	  The lock state lives intentionally OUTSIDE the GUC subsystem so
 *	  that it is not stacked by transaction / savepoint machinery and
 *	  cannot be unwound by ROLLBACK.  GUC unwind that would restore a
 *	  pre-lock role above the ceiling is clipped to the ceiling by the
 *	  assign hooks in src/backend/commands/variable.c calling into
 *	  AuthLockClipRole().
 *
 *	  Phase 0 implements only the IRREVOCABLE variant exposed via two
 *	  SQL-callable functions:
 *	    pg_set_role_irrevocable(text)
 *	    pg_set_session_authorization_irrevocable(text)
 *	  The cookie variant, SQL grammar, parallel-worker propagation, and
 *	  pooler ParameterStatus signal are deferred to subsequent phases.
 *
 *	  See "irrevocable-privilege-drop-design.md" in the project workspace
 *	  for the full design.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/init/auth_lock.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_authid.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/auth_lock.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/syscache.h"

typedef struct AuthLock
{
	AuthLockKind kind;
	Oid			ceiling_role;
	bool		ceiling_is_superuser;
} AuthLock;

/* Process-local; reset to zero by backend startup. */
static AuthLock auth_locks[AUTH_LOCK_NSCOPES];

static const char *
scope_name(AuthLockScope scope)
{
	switch (scope)
	{
		case AUTH_LOCK_SCOPE_ROLE:
			return "role";
		case AUTH_LOCK_SCOPE_SESSION_AUTH:
			return "session_authorization";
		default:
			return "unknown";
	}
}

void
AuthLockSetIrrevocable(AuthLockScope scope, Oid ceiling_role,
					   bool ceiling_is_superuser)
{
	Assert(scope < AUTH_LOCK_NSCOPES);
	Assert(OidIsValid(ceiling_role));

	if (auth_locks[scope].kind != AUTH_LOCK_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("authorization lock already in effect on %s scope",
						scope_name(scope))));

	auth_locks[scope].kind = AUTH_LOCK_IRREVOCABLE;
	auth_locks[scope].ceiling_role = ceiling_role;
	auth_locks[scope].ceiling_is_superuser = ceiling_is_superuser;

	ereport(LOG,
			(errmsg("auth_lock: installed kind=irrevocable scope=%s ceiling=%u",
					scope_name(scope), ceiling_role)));
}

AuthLockKind
AuthLockGetKind(AuthLockScope scope)
{
	Assert(scope < AUTH_LOCK_NSCOPES);
	return auth_locks[scope].kind;
}

Oid
AuthLockGetCeiling(AuthLockScope scope)
{
	Assert(scope < AUTH_LOCK_NSCOPES);
	return auth_locks[scope].ceiling_role;
}

bool
AuthLockGetCeilingIsSuperuser(AuthLockScope scope)
{
	Assert(scope < AUTH_LOCK_NSCOPES);
	return auth_locks[scope].ceiling_is_superuser;
}

bool
AuthLockWouldViolate(AuthLockScope scope, Oid roleid)
{
	Assert(scope < AUTH_LOCK_NSCOPES);

	if (auth_locks[scope].kind == AUTH_LOCK_NONE)
		return false;

	/*
	 * SET ROLE NONE / DEFAULT collapses to the session user.  Test against
	 * the session user instead of InvalidOid.
	 */
	if (!OidIsValid(roleid))
		roleid = GetSessionUserId();

	if (roleid == auth_locks[scope].ceiling_role)
		return false;

	/* Ceiling must be permitted to become target role under SET ROLE rules. */
	return !member_can_set_role(auth_locks[scope].ceiling_role, roleid);
}

void
AuthLockClipRole(AuthLockScope scope, Oid *roleid, bool *is_superuser)
{
	Assert(scope < AUTH_LOCK_NSCOPES);

	if (auth_locks[scope].kind == AUTH_LOCK_NONE)
		return;

	if (AuthLockWouldViolate(scope, *roleid))
	{
		/*
		 * Don't ereport(ERROR) here -- we may be running during transaction
		 * abort or GUC unwind, where any error escalates to PANIC.  Clip
		 * silently and emit a LOG line for audit pipelines.
		 */
		ereport(LOG,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("auth_lock: clipped %s assignment to ceiling",
						scope_name(scope)),
				 errdetail("requested role OID %u not reachable from ceiling OID %u",
						   *roleid, auth_locks[scope].ceiling_role)));

		*roleid = auth_locks[scope].ceiling_role;
		*is_superuser = auth_locks[scope].ceiling_is_superuser;
	}
}


/*
 * SQL-callable: pg_set_role_irrevocable(rolename text)
 *
 * Equivalent to SET ROLE x, but additionally installs an IRREVOCABLE
 * lock on both the role and session_authorization scopes, so that no
 * subsequent code path in this session can restore the pre-call role.
 *
 * Locking both scopes is deliberate: a lock on only the role scope
 * would leave SET SESSION AUTHORIZATION as an unrestricted back door.
 */
Datum
pg_set_role_irrevocable(PG_FUNCTION_ARGS)
{
	text	   *rolename_text;
	char	   *rolename;
	HeapTuple	roleTup;
	Form_pg_authid roleform;
	Oid			roleid;
	bool		is_superuser;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("role name must not be NULL")));

	rolename_text = PG_GETARG_TEXT_PP(0);
	rolename = text_to_cstring(rolename_text);

	roleTup = SearchSysCache1(AUTHNAME, PointerGetDatum(rolename));
	if (!HeapTupleIsValid(roleTup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("role \"%s\" does not exist", rolename)));

	roleform = (Form_pg_authid) GETSTRUCT(roleTup);
	roleid = roleform->oid;
	is_superuser = roleform->rolsuper;
	ReleaseSysCache(roleTup);

	if (!member_can_set_role(GetSessionUserId(), roleid))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to set role \"%s\"", rolename)));

	/*
	 * Apply the SET ROLE first (no lock in place yet, so the existing
	 * permission checks apply).  Then install the lock.  If the lock
	 * install fails, the session is left at the new role without a lock
	 * -- equivalent to a plain SET ROLE -- which is no worse than what the
	 * caller would have gotten without IRREVOCABLE.
	 */
	(void) set_config_option("role", rolename,
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SET,
							 true, 0, false);

	AuthLockSetIrrevocable(AUTH_LOCK_SCOPE_ROLE, roleid, is_superuser);
	AuthLockSetIrrevocable(AUTH_LOCK_SCOPE_SESSION_AUTH, roleid, is_superuser);

	PG_RETURN_VOID();
}

/*
 * SQL-callable: pg_set_session_authorization_irrevocable(rolename text)
 *
 * Like pg_set_role_irrevocable but operates via SET SESSION AUTHORIZATION:
 * the original session_user is overwritten and lost.  This is stronger
 * than the role variant: there is no path back to the original
 * authenticated identity within the session, even before considering the
 * lock.
 */
Datum
pg_set_session_authorization_irrevocable(PG_FUNCTION_ARGS)
{
	text	   *rolename_text;
	char	   *rolename;
	HeapTuple	roleTup;
	Form_pg_authid roleform;
	Oid			roleid;
	bool		is_superuser;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("role name must not be NULL")));

	rolename_text = PG_GETARG_TEXT_PP(0);
	rolename = text_to_cstring(rolename_text);

	roleTup = SearchSysCache1(AUTHNAME, PointerGetDatum(rolename));
	if (!HeapTupleIsValid(roleTup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("role \"%s\" does not exist", rolename)));

	roleform = (Form_pg_authid) GETSTRUCT(roleTup);
	roleid = roleform->oid;
	is_superuser = roleform->rolsuper;
	ReleaseSysCache(roleTup);

	/*
	 * Mirror the check in check_session_authorization (variable.c): only
	 * an authenticated superuser may SET SESSION AUTHORIZATION to a role
	 * other than itself.
	 */
	if (roleid != GetAuthenticatedUserId() &&
		!superuser_arg(GetAuthenticatedUserId()))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to set session authorization \"%s\"",
						rolename)));

	(void) set_config_option("session_authorization", rolename,
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SET,
							 true, 0, false);

	AuthLockSetIrrevocable(AUTH_LOCK_SCOPE_ROLE, roleid, is_superuser);
	AuthLockSetIrrevocable(AUTH_LOCK_SCOPE_SESSION_AUTH, roleid, is_superuser);

	PG_RETURN_VOID();
}

/*
 * SQL-callable: pg_auth_lock_status()
 *
 * Returns a 4-column row per scope describing whether a lock is in
 * effect and what its ceiling is.  Does NOT expose any cookie value.
 * Phase 0 implements only the IRREVOCABLE kind, but the result shape
 * is forward-compatible with the cookie variant.
 */
Datum
pg_auth_lock_status(PG_FUNCTION_ARGS)
{
	/* TODO: implement as a SRF.  Stubbed for Phase 0 — the C surface is
	 * sufficient for testing; the SQL inspector can land with the cookie
	 * patch. */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("pg_auth_lock_status() not yet implemented")));
	PG_RETURN_NULL();
}
