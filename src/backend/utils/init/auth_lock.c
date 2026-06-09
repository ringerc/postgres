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
#include "access/xact.h"
#include "catalog/pg_authid.h"
#include "common/cryptohash.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "port.h"				/* pg_strong_random */
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
	/*
	 * Cookie variant only.  SHA-256 of the raw cookie that was returned
	 * to the caller exactly once.  The raw cookie is never stored.
	 * Constant-time compared against the hash of any presented cookie.
	 */
	uint8		cookie_hash[AUTH_LOCK_COOKIE_HASH_LEN];
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

	/*
	 * If we are not currently in a transaction state, catalog lookups are
	 * unsafe.  This happens when AuthLockClipRole is invoked from
	 * SetCurrentRoleId / SetSessionAuthorization during transaction abort
	 * (via the GUC unwind path that calls the assign hooks).  In that
	 * narrow window we cannot evaluate the full ceiling rule, so be
	 * conservative: treat any non-equal target as a violation and let the
	 * caller clip to the ceiling.  This may over-clip a legitimate
	 * sibling-of-ceiling restore during abort, but the resulting identity
	 * is always at or below the ceiling — the security invariant holds.
	 *
	 * Normal (non-abort) paths reach this function inside a transaction
	 * state and get the full ceiling rule via member_can_set_role.
	 */
	if (!IsTransactionState())
		return true;

	/* Ceiling must be permitted to become target role under SET ROLE rules. */
	return !member_can_set_role(auth_locks[scope].ceiling_role, roleid);
}

void
AuthLockSetCookie(AuthLockScope scope, Oid ceiling_role,
				  bool ceiling_is_superuser,
				  const uint8 hash[AUTH_LOCK_COOKIE_HASH_LEN])
{
	Assert(scope < AUTH_LOCK_NSCOPES);
	Assert(OidIsValid(ceiling_role));
	Assert(hash != NULL);

	if (auth_locks[scope].kind != AUTH_LOCK_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("authorization lock already in effect on %s scope",
						scope_name(scope))));

	auth_locks[scope].kind = AUTH_LOCK_COOKIE;
	auth_locks[scope].ceiling_role = ceiling_role;
	auth_locks[scope].ceiling_is_superuser = ceiling_is_superuser;
	memcpy(auth_locks[scope].cookie_hash, hash, AUTH_LOCK_COOKIE_HASH_LEN);

	ereport(LOG,
			(errmsg("auth_lock: installed kind=cookie scope=%s ceiling=%u",
					scope_name(scope), ceiling_role)));
}

void
AuthLockHashCookie(const uint8 *raw, size_t raw_len,
				   uint8 out_hash[AUTH_LOCK_COOKIE_HASH_LEN])
{
	pg_cryptohash_ctx *ctx;

	Assert(raw != NULL || raw_len == 0);

	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL ||
		pg_cryptohash_init(ctx) < 0 ||
		pg_cryptohash_update(ctx, raw, raw_len) < 0 ||
		pg_cryptohash_final(ctx, out_hash, AUTH_LOCK_COOKIE_HASH_LEN) < 0)
	{
		const char *errmsg_str = ctx ? pg_cryptohash_error(ctx) : "out of memory";

		if (ctx)
			pg_cryptohash_free(ctx);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not compute auth_lock cookie hash: %s",
						errmsg_str)));
	}
	pg_cryptohash_free(ctx);
}

/*
 * Constant-time byte comparison: returns true iff both buffers contain
 * the same bytes for the full length.  Avoids branching on per-byte
 * outcomes so timing does not leak position-of-first-difference.
 */
static bool
auth_lock_constant_time_equal(const uint8 *a, const uint8 *b, size_t n)
{
	uint8		diff = 0;
	size_t		i;

	for (i = 0; i < n; i++)
		diff |= a[i] ^ b[i];
	return diff == 0;
}

bool
AuthLockClearWithCookie(AuthLockScope scope,
						const uint8 hash[AUTH_LOCK_COOKIE_HASH_LEN])
{
	Assert(scope < AUTH_LOCK_NSCOPES);
	Assert(hash != NULL);

	if (auth_locks[scope].kind != AUTH_LOCK_COOKIE)
		return false;

	if (!auth_lock_constant_time_equal(auth_locks[scope].cookie_hash, hash,
									   AUTH_LOCK_COOKIE_HASH_LEN))
		return false;

	/* Match.  Zero the hash before clearing so post-mortem inspection
	 * doesn't find a still-valid hash sitting in the struct. */
	memset(auth_locks[scope].cookie_hash, 0, AUTH_LOCK_COOKIE_HASH_LEN);
	auth_locks[scope].kind = AUTH_LOCK_NONE;
	auth_locks[scope].ceiling_role = InvalidOid;
	auth_locks[scope].ceiling_is_superuser = false;

	ereport(LOG,
			(errmsg("auth_lock: cleared scope=%s via cookie",
					scope_name(scope))));

	return true;
}

bool
AuthLockBlocksReset(const char *name)
{
	if (name == NULL)
		return false;

	if (strcmp(name, "role") == 0)
		return auth_locks[AUTH_LOCK_SCOPE_ROLE].kind != AUTH_LOCK_NONE;
	if (strcmp(name, "session_authorization") == 0)
		return auth_locks[AUTH_LOCK_SCOPE_SESSION_AUTH].kind != AUTH_LOCK_NONE;
	return false;
}

bool
AuthLockIsAnyActive(void)
{
	for (int i = 0; i < AUTH_LOCK_NSCOPES; i++)
		if (auth_locks[i].kind != AUTH_LOCK_NONE)
			return true;
	return false;
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
 * Common entry-point helpers shared between the IRREVOCABLE and COOKIE
 * SQL functions.  Looks up the role by name and validates the calling
 * session_user has permission to become it.  Errors on lookup failure
 * or permission denial.
 */
static void
auth_lock_resolve_role(const char *rolename, Oid *roleid_out,
					   bool *is_super_out)
{
	HeapTuple	roleTup;
	Form_pg_authid roleform;

	roleTup = SearchSysCache1(AUTHNAME, PointerGetDatum(rolename));
	if (!HeapTupleIsValid(roleTup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("role \"%s\" does not exist", rolename)));

	roleform = (Form_pg_authid) GETSTRUCT(roleTup);
	*roleid_out = roleform->oid;
	*is_super_out = roleform->rolsuper;
	ReleaseSysCache(roleTup);
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
 * Common implementation for the two pg_set_*_with_cookie SQL functions.
 *
 *  - sas: true for the SET SESSION AUTHORIZATION variant, false for the
 *    SET ROLE variant.
 *
 * Generates a 32-byte cookie from pg_strong_random, hashes it with SHA-256,
 * applies the role change via set_config_option (which routes through the
 * existing check/assign hooks), installs a COOKIE-protected lock on both
 * scopes with the same hash, and returns the raw cookie as a bytea.
 *
 * The returned bytea contains the only copy of the raw cookie that exists
 * after this call.  The hash is the only server-side trace.
 */
static Datum
auth_lock_set_with_cookie(PG_FUNCTION_ARGS, bool sas)
{
	text	   *rolename_text;
	char	   *rolename;
	Oid			roleid;
	bool		is_superuser;
	bytea	   *cookie_bytea;
	uint8	   *cookie_bytes;
	uint8		hash[AUTH_LOCK_COOKIE_HASH_LEN];

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("role name must not be NULL")));

	rolename_text = PG_GETARG_TEXT_PP(0);
	rolename = text_to_cstring(rolename_text);

	auth_lock_resolve_role(rolename, &roleid, &is_superuser);

	/* Membership / SAS permission, parallel to the IRREVOCABLE variants. */
	if (sas)
	{
		if (roleid != GetAuthenticatedUserId() &&
			!superuser_arg(GetAuthenticatedUserId()))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied to set session authorization \"%s\"",
							rolename)));
	}
	else
	{
		if (!member_can_set_role(GetSessionUserId(), roleid))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied to set role \"%s\"", rolename)));
	}

	/*
	 * Allocate the bytea result up front; the raw cookie lives in its
	 * VARDATA region for the rest of this function and is returned to the
	 * caller.  We never make a separate raw-cookie copy on the server side.
	 */
	cookie_bytea = (bytea *) palloc(VARHDRSZ + AUTH_LOCK_COOKIE_RAW_LEN);
	SET_VARSIZE(cookie_bytea, VARHDRSZ + AUTH_LOCK_COOKIE_RAW_LEN);
	cookie_bytes = (uint8 *) VARDATA(cookie_bytea);

	if (!pg_strong_random(cookie_bytes, AUTH_LOCK_COOKIE_RAW_LEN))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not generate auth_lock cookie from strong random source")));

	AuthLockHashCookie(cookie_bytes, AUTH_LOCK_COOKIE_RAW_LEN, hash);

	/*
	 * Apply the role change first (under the existing check/assign rules,
	 * with no lock in effect yet), then install the cookie lock on BOTH
	 * scopes with the same hash.  Locking both scopes means SET SESSION
	 * AUTHORIZATION cannot be used as a back door for SET ROLE locks, and
	 * vice versa.  Presenting the cookie via pg_reset_*_with_cookie clears
	 * the matching scope only — both can be cleared by two presentations
	 * of the same cookie, or in practice by the SQL-side reset functions
	 * which iterate scopes.
	 */
	(void) set_config_option(sas ? "session_authorization" : "role",
							 rolename,
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SET,
							 true, 0, false);

	AuthLockSetCookie(AUTH_LOCK_SCOPE_ROLE, roleid, is_superuser, hash);
	AuthLockSetCookie(AUTH_LOCK_SCOPE_SESSION_AUTH, roleid, is_superuser, hash);

	/* Scrub the local hash copy from stack before returning. */
	memset(hash, 0, sizeof(hash));

	PG_RETURN_BYTEA_P(cookie_bytea);
}

/*
 * SQL-callable: pg_set_role_with_cookie(rolename text) RETURNS bytea
 */
Datum
pg_set_role_with_cookie(PG_FUNCTION_ARGS)
{
	return auth_lock_set_with_cookie(fcinfo, false);
}

/*
 * SQL-callable: pg_set_session_authorization_with_cookie(rolename text) RETURNS bytea
 */
Datum
pg_set_session_authorization_with_cookie(PG_FUNCTION_ARGS)
{
	return auth_lock_set_with_cookie(fcinfo, true);
}

/*
 * Common implementation for the two pg_reset_*_with_cookie SQL functions.
 * Iterates both scopes and attempts to clear with the presented cookie.
 * Errors on mismatch / no-lock; succeeds quietly if at least one scope's
 * lock was cleared.
 */
static Datum
auth_lock_reset_with_cookie(PG_FUNCTION_ARGS)
{
	bytea	   *presented;
	const uint8 *raw;
	int			raw_len;
	uint8		hash[AUTH_LOCK_COOKIE_HASH_LEN];
	bool		cleared_role;
	bool		cleared_sa;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("cookie must not be NULL")));

	presented = PG_GETARG_BYTEA_PP(0);
	raw = (const uint8 *) VARDATA_ANY(presented);
	raw_len = VARSIZE_ANY_EXHDR(presented);

	if (raw_len != AUTH_LOCK_COOKIE_RAW_LEN)
	{
		/*
		 * Length mismatch is itself an attempted unlock attack: hash and
		 * fail uniformly so that the response timing does not distinguish
		 * "wrong length" from "wrong content".
		 */
		ereport(LOG,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				 errmsg("auth_lock: cookie reset failed (bad length)")));
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				 errmsg("invalid auth_lock cookie")));
	}

	AuthLockHashCookie(raw, raw_len, hash);

	cleared_role = AuthLockClearWithCookie(AUTH_LOCK_SCOPE_ROLE, hash);
	cleared_sa = AuthLockClearWithCookie(AUTH_LOCK_SCOPE_SESSION_AUTH, hash);

	/* Scrub the local hash before any error paths. */
	memset(hash, 0, sizeof(hash));

	if (!cleared_role && !cleared_sa)
	{
		ereport(LOG,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				 errmsg("auth_lock: cookie reset failed (mismatch or no lock)")));
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				 errmsg("invalid auth_lock cookie")));
	}

	PG_RETURN_VOID();
}

/*
 * SQL-callable: pg_reset_role_with_cookie(cookie bytea) RETURNS void
 * SQL-callable: pg_reset_session_authorization_with_cookie(cookie bytea) RETURNS void
 *
 * In Phase 2 the two are aliases — a single cookie clears both scopes'
 * locks because both were installed together by pg_set_*_with_cookie.
 * They are exposed as separate functions for API symmetry with the
 * IRREVOCABLE variants and to give the caller intent expression.
 */
Datum
pg_reset_role_with_cookie(PG_FUNCTION_ARGS)
{
	return auth_lock_reset_with_cookie(fcinfo);
}

Datum
pg_reset_session_authorization_with_cookie(PG_FUNCTION_ARGS)
{
	return auth_lock_reset_with_cookie(fcinfo);
}

/*
 * SQL-callable: pg_auth_lock_status()
 *
 * Returns one row per scope describing whether a lock is in effect and
 * what its ceiling is.  Does NOT expose any cookie value or hash —
 * only the presence/absence flag.
 */
Datum
pg_auth_lock_status(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("pg_auth_lock_status() must be called with a row-typed result")));

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		funcctx->max_calls = AUTH_LOCK_NSCOPES;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		AuthLockScope scope = (AuthLockScope) funcctx->call_cntr;
		AuthLockKind kind = AuthLockGetKind(scope);
		Oid			ceiling = AuthLockGetCeiling(scope);
		Datum		values[5];
		bool		nulls[5];
		HeapTuple	tuple;
		const char *kind_str;

		switch (kind)
		{
			case AUTH_LOCK_NONE:
				kind_str = "none";
				break;
			case AUTH_LOCK_IRREVOCABLE:
				kind_str = "irrevocable";
				break;
			case AUTH_LOCK_COOKIE:
				kind_str = "cookie";
				break;
			default:
				kind_str = "unknown";
				break;
		}

		values[0] = CStringGetTextDatum(scope_name(scope));
		nulls[0] = false;

		values[1] = CStringGetTextDatum(kind_str);
		nulls[1] = false;

		if (kind == AUTH_LOCK_NONE)
		{
			nulls[2] = true;
			nulls[3] = true;
		}
		else
		{
			char	   *role_name;

			values[2] = ObjectIdGetDatum(ceiling);
			nulls[2] = false;

			role_name = GetUserNameFromId(ceiling, true);
			if (role_name != NULL)
			{
				values[3] = DirectFunctionCall1(namein,
											  CStringGetDatum(role_name));
				nulls[3] = false;
			}
			else
				nulls[3] = true;
		}

		values[4] = BoolGetDatum(kind == AUTH_LOCK_COOKIE);
		nulls[4] = false;

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}

	SRF_RETURN_DONE(funcctx);
}
