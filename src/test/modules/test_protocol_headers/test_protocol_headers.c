/*-------------------------------------------------------------------------
 *
 * test_protocol_headers.c
 *	  Test module for the per-message protocol headers ('M') mechanism,
 *	  and a worked example of an extension that manages its own header
 *	  lifecycle.
 *
 * The core dispatcher is intentionally lifecycle-free: it routes each
 * (key, value) entry to whichever handler claims the longest matching
 * key prefix, and does nothing else.  Anything resembling "clear this
 * at COMMIT" or "clear this at backend exit" is the extension's
 * problem.
 *
 * This module demonstrates the intended pattern: one registration per
 * extension, demultiplexed internally to per-key lifetimes.
 *
 * It registers a single handler under prefix "test.".  Inside the
 * handler it dispatches on the full key:
 *
 *	  test.txn_scope	-> cleared by RegisterXactCallback (top-level
 *						   transaction end --- subtransaction aborts/
 *						   commits are intentionally NOT instrumented)
 *	  test.sess_scope	-> cleared by on_proc_exit (backend exit)
 *	  test.fail_on_set	-> raises ERROR every time it is set; exercised
 *						   by the TAP test to verify that a handler
 *						   ERROR fails the SQL operation the headers
 *						   were intended to prefix (deferred-apply
 *						   atomicity)
 *
 * Statement-scope support is added on top of pre_ready_for_query_hook
 * in a follow-up commit; until that lands, this module covers only
 * transaction and session scope.
 *
 * Any other key under "test." is logged as "unknown key" --- the TAP
 * test asserts on that to confirm unknown keys land at the registered
 * handler rather than being silently dropped by the dispatcher.
 *
 * Set / clear events are logged so the TAP test can assert on
 * timing by reading the server log:
 *
 *	  test_protocol_headers: set scope=<scope> key=<key> value=<value>
 *	  test_protocol_headers: clear scope=<scope>
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/test/modules/test_protocol_headers/test_protocol_headers.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "fmgr.h"
#include "libpq/protocol_headers.h"
#include "storage/ipc.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

/*
 * Per-scope storage.  Each slot is either NULL (not set) or a palloc'd
 * copy of the value last received under that key.  The pointers live
 * in TopMemoryContext so they survive transaction-context resets.
 *
 * "Touched" tracks whether the slot was set at least once during the
 * current scope window.  We use it to decide whether to fire the clear
 * log line --- a clear hook that fired on every scope boundary
 * regardless of state would make the TAP assertions noisy.
 */
typedef struct ScopeSlot
{
	char	   *value;
	bool		touched;
} ScopeSlot;

static ScopeSlot txn_slot;
static ScopeSlot sess_slot;

static void
slot_set(ScopeSlot *slot, const char *key, const char *value, const char *scope_name)
{
	MemoryContext oldcxt;

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	if (slot->value != NULL)
		pfree(slot->value);
	slot->value = pstrdup(value);
	MemoryContextSwitchTo(oldcxt);

	slot->touched = true;

	ereport(LOG,
			(errmsg("test_protocol_headers: set scope=%s key=%s value=%s",
					scope_name, key, value)));
}

static void
slot_clear(ScopeSlot *slot, const char *scope_name)
{
	if (!slot->touched)
		return;
	if (slot->value != NULL)
	{
		pfree(slot->value);
		slot->value = NULL;
	}
	slot->touched = false;

	ereport(LOG,
			(errmsg("test_protocol_headers: clear scope=%s",
					scope_name)));
}

/*
 * Transaction-scope cleanup.  Subtransactions are deliberately NOT
 * instrumented --- no RegisterSubXactCallback is installed.  A
 * transaction-scope key set inside a SAVEPOINT block survives a
 * ROLLBACK TO that savepoint; the single clear fires only at top-level
 * COMMIT/ROLLBACK/PREPARE.
 *
 * Rationale: per-key per-subxact-level snapshot/restore would mirror
 * the GUC machinery's stack, but it's not justified by any concrete
 * consumer yet, and the common case (trace context, tenant IDs,
 * correlation IDs) sets the header at the OUTER scope.  An extension
 * that genuinely needs subxact semantics can install its own
 * RegisterSubXactCallback alongside; this test module is the worked
 * example of the simple case.
 */
static void
my_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_PREPARE:
			slot_clear(&txn_slot, "transaction");
			break;
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
			/* nothing */
			break;
	}
}

/*
 * Session-scope cleanup at backend exit.
 */
static void
my_proc_exit(int code, Datum arg)
{
	slot_clear(&sess_slot, "session");
}

/*
 * The single registered handler.  Routes on the full key, which is
 * the documented pattern for an extension that wants to expose
 * multiple per-key lifetimes behind one prefix.
 */
static void
test_header_set_cb(const char *key, const char *value, void *ctx)
{
	if (strcmp(key, "test.txn_scope") == 0)
		slot_set(&txn_slot, key, value, "transaction");
	else if (strcmp(key, "test.sess_scope") == 0)
		slot_set(&sess_slot, key, value, "session");
	else if (strcmp(key, "test.fail_on_set") == 0)
	{
		/*
		 * Deliberately raise ERROR so the TAP test can verify that a
		 * handler failure becomes the prefixed SQL operation's failure
		 * (and that the operation does not run).  The value is logged
		 * at LOG before the ERROR so the test can also confirm we
		 * reached the handler --- the issue under test is *which*
		 * error boundary the ERROR rolls up into, not whether the
		 * handler was called at all.
		 */
		ereport(LOG,
				(errmsg("test_protocol_headers: set scope=fail key=%s value=%s",
						key, value)));
		ereport(ERROR,
				(errmsg("test_protocol_headers: handler asked to fail (value=%s)",
						value)));
	}
	else
		ereport(LOG,
				(errmsg("test_protocol_headers: unknown key \"%s\" under registered prefix",
						key)));
}

void		_PG_init(void);

void
_PG_init(void)
{
	RegisterProtocolHeaderHandler("test.", test_header_set_cb, NULL);

	RegisterXactCallback(my_xact_callback, NULL);
	on_proc_exit(my_proc_exit, (Datum) 0);
}
