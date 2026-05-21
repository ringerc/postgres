/*-------------------------------------------------------------------------
 *
 * test_protocol_headers.c
 *	  Test module for the per-message protocol headers ('M') mechanism.
 *
 * Registers three handlers --- one at each supported scope --- that log
 * every set and clear event so a TAP test can assert end-to-end
 * dispatch and scope-boundary behaviour by inspecting the server log.
 *
 * Prefixes registered:
 *	  "test_stmt."  -> PROTOCOL_HEADER_SCOPE_STATEMENT
 *	  "test_tx."	-> PROTOCOL_HEADER_SCOPE_TRANSACTION
 *	  "test_sess."  -> PROTOCOL_HEADER_SCOPE_SESSION
 *
 * Headers received under one of these prefixes produce log lines of
 * the form:
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

#include "fmgr.h"
#include "libpq/protocol_headers.h"

PG_MODULE_MAGIC;

static void
log_set(const char *key, const char *value, void *ctx)
{
	const char *scope_name = (const char *) ctx;

	ereport(LOG,
			(errmsg("test_protocol_headers: set scope=%s key=%s value=%s",
					scope_name, key, value)));
}

static void
log_clear(void *ctx)
{
	const char *scope_name = (const char *) ctx;

	ereport(LOG,
			(errmsg("test_protocol_headers: clear scope=%s",
					scope_name)));
}

void		_PG_init(void);

void
_PG_init(void)
{
	RegisterProtocolHeaderHandler("test_stmt.",
								  PROTOCOL_HEADER_SCOPE_STATEMENT,
								  log_set,
								  log_clear,
								  (void *) "statement");
	RegisterProtocolHeaderHandler("test_tx.",
								  PROTOCOL_HEADER_SCOPE_TRANSACTION,
								  log_set,
								  log_clear,
								  (void *) "transaction");
	RegisterProtocolHeaderHandler("test_sess.",
								  PROTOCOL_HEADER_SCOPE_SESSION,
								  log_set,
								  log_clear,
								  (void *) "session");
}
