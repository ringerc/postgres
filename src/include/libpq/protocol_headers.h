/*-------------------------------------------------------------------------
 *
 * protocol_headers.h
 *	  Registry and dispatch for the per-message protocol headers
 *	  (RequestHeaders, message type 'M').
 *
 * Extensions register interest in a key prefix and a scope at which the
 * effect of received headers should be cleared.  When the client sends
 * a RequestHeaders message, each entry is dispatched to the registered
 * handler whose prefix matches the longest prefix of the key; entries
 * with no matching handler are silently ignored.
 *
 * The wire-level header set is transient: it is parsed, dispatched, and
 * discarded as a unit.  The *effect* the handler installs lives at the
 * handler's declared scope and is torn down by its clear callback at
 * the scope boundary.
 *
 * Headers are advisory only.  They must not be used as the basis of
 * authorization decisions.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/protocol_headers.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PROTOCOL_HEADERS_H
#define PROTOCOL_HEADERS_H

#include "lib/stringinfo.h"

/*
 * Scope at which a header handler's effect is cleared.
 */
typedef enum ProtocolHeaderScope
{
	PROTOCOL_HEADER_SCOPE_STATEMENT,	/* cleared at next ReadyForQuery */
	PROTOCOL_HEADER_SCOPE_TRANSACTION,	/* cleared at COMMIT/ROLLBACK */
	PROTOCOL_HEADER_SCOPE_SESSION,		/* cleared at backend exit */
} ProtocolHeaderScope;

/*
 * Callbacks supplied by an extension at registration.
 *
 *	set_cb is invoked once per matching header entry.  An empty value
 *	is the documented "clear this key" convention; handlers should
 *	treat value=="" as a request to clear that key's effect.
 *
 *	clear_cb is invoked at the scope boundary to clear all effects the
 *	handler accumulated during that scope.  It may be NULL if the
 *	handler keeps no scope-local state.
 */
typedef void (*ProtocolHeaderSetCb) (const char *key,
									 const char *value,
									 void *ctx);
typedef void (*ProtocolHeaderClearCb) (void *ctx);

/*
 * Register interest in headers whose key has the given prefix.  Longest
 * prefix wins on dispatch.  Typically called from an extension's
 * _PG_init().  prefix and ctx must remain valid for the lifetime of
 * the backend.
 */
extern void RegisterProtocolHeaderHandler(const char *prefix,
										  ProtocolHeaderScope scope,
										  ProtocolHeaderSetCb set_cb,
										  ProtocolHeaderClearCb clear_cb,
										  void *ctx);

/*
 * Negotiation state.  Set during StartupPacket processing if the client
 * sent _pq_.headers=1 AND the server-side protocol_headers GUC is on.
 */
extern PGDLLIMPORT bool ProtocolHeadersNegotiated;

/*
 * GUCs.
 */
extern PGDLLIMPORT bool protocol_headers_enabled;
extern PGDLLIMPORT int	max_protocol_header_entries;
extern PGDLLIMPORT int	max_protocol_header_size;

/*
 * Called by PostgresMain when a 'M' message has arrived.  Parses the
 * message body from msg and dispatches each entry.  Reports a
 * protocol error (FATAL) if negotiation was not completed or if the
 * message exceeds the configured caps.
 */
extern void ProcessRequestHeadersMessage(StringInfo msg);

/*
 * Scope-boundary clear functions.  ClearStatementScopeHeaders is
 * called by PostgresMain right before sending ReadyForQuery; the other
 * two are wired up automatically via RegisterXactCallback and
 * on_proc_exit by ProtocolHeadersInit().
 */
extern void ClearStatementScopeHeaders(void);

/*
 * One-time initialization, called once per backend before the main
 * message loop.  Idempotent.
 */
extern void ProtocolHeadersInit(void);

#endif							/* PROTOCOL_HEADERS_H */
