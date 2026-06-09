/*-------------------------------------------------------------------------
 *
 * discard.c
 *	  The implementation of the DISCARD command
 *
 * Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/discard.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/async.h"
#include "commands/discard.h"
#include "commands/prepare.h"
#include "commands/sequence.h"
#include "storage/lock.h"
#include "utils/auth_lock.h"
#include "utils/guc.h"
#include "utils/portal.h"

static void DiscardAll(bool isTopLevel);

/*
 * DISCARD { ALL | SEQUENCES | TEMP | PLANS }
 */
void
DiscardCommand(DiscardStmt *stmt, bool isTopLevel)
{
	switch (stmt->target)
	{
		case DISCARD_ALL:
			DiscardAll(isTopLevel);
			break;

		case DISCARD_PLANS:
			ResetPlanCache();
			break;

		case DISCARD_SEQUENCES:
			ResetSequenceCaches();
			break;

		case DISCARD_TEMP:
			ResetTempTableNamespace();
			break;

		default:
			elog(ERROR, "unrecognized DISCARD target: %d", stmt->target);
	}
}

static void
DiscardAll(bool isTopLevel)
{
	/*
	 * Disallow DISCARD ALL in a transaction block. This is arguably
	 * inconsistent (we don't make a similar check in the command sequence
	 * that DISCARD ALL is equivalent to), but the idea is to catch mistakes:
	 * DISCARD ALL inside a transaction block would leave the transaction
	 * still uncommitted.
	 */
	PreventInTransactionBlock(isTopLevel, "DISCARD ALL");

	/*
	 * Refuse DISCARD ALL when an authorization lock is in effect.  The
	 * downstream SetPGVariable("session_authorization", NIL, false) below
	 * would also be refused by the Layer-2 set_config_option_ext hook,
	 * but checking here lets us fail early without performing any of the
	 * other DISCARD sub-operations (portals, prepared statements, etc.)
	 * — partial DISCARD would leave the session in an awkward state.
	 *
	 * If a session truly needs to recycle (e.g. behind a connection
	 * pooler), use the cookie-protected reset variant instead.
	 */
	if (AuthLockIsAnyActive())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("session is locked; cannot DISCARD ALL"),
				 errhint("Reconnect to release the lock, or use the cookie-protected reset variant.")));

	/* Closing portals might run user-defined code, so do that first. */
	PortalHashTableDeleteAll();
	SetPGVariable("session_authorization", NIL, false);
	ResetAllOptions();
	DropAllPreparedStatements();
	Async_UnlistenAll();
	LockReleaseAll(USER_LOCKMETHOD, true);
	ResetPlanCache();
	ResetTempTableNamespace();
	ResetSequenceCaches();
}
