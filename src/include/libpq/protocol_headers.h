/*-------------------------------------------------------------------------
 *
 * protocol_headers.h
 *	  Registry and dispatch for the per-message protocol headers
 *	  (RequestHeaders, message type 'M').
 *
 * Extensions register interest in a key prefix; when a RequestHeaders
 * message arrives, each (key, value) entry is bound to the handler
 * with the longest matching prefix.  Entries with no matching handler
 * are silently ignored.
 *
 * Dispatch is *deferred*.  ProcessRequestHeadersMessage parses 'M' and
 * stashes the parsed (key, value, handler) tuples on a backend-private
 * pending list; handler set_cb's run only at the start of the next
 * Query / Parse / Bind / Execute, via ApplyPendingRequestHeaders.
 * This binds a handler ERROR to the SQL operation the headers were
 * intended to prefix, so an extension that throws inside set_cb fails
 * that operation rather than letting it run with half-applied state.
 * See protocol_headers.c for the full lifecycle.
 *
 * Lifecycle of applied state is entirely the extension's
 * responsibility.  The dispatcher does not retain any state after
 * dispatch and does not call back at any scope boundary.  An extension
 * that needs scope-based cleanup wires up its own machinery:
 * pre_ready_for_query_hook for statement-scope effects,
 * RegisterXactCallback for transaction-scope effects, on_proc_exit for
 * session-scope effects.  See test_protocol_headers for a worked
 * example.
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
 * Callback supplied by an extension at registration.  Invoked once per
 * matching header entry, at the start of the operation the headers
 * prefix (Query / Parse / Bind / Execute).  An empty value is the
 * documented "clear this key" convention; handlers should treat
 * value=="" as a request to clear that key's effect.
 */
typedef void (*ProtocolHeaderSetCb) (const char *key,
									 const char *value,
									 void *ctx);

/*
 * Register interest in headers whose key has the given prefix.  Longest
 * prefix wins on dispatch.  Typically called from an extension's
 * _PG_init().  prefix and ctx must remain valid for the lifetime of
 * the backend.
 */
extern void RegisterProtocolHeaderHandler(const char *prefix,
										  ProtocolHeaderSetCb set_cb,
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
 * Called by PostgresMain when an 'M' message has arrived.  Parses the
 * message body from msg and stashes each entry on the pending list
 * for the next operation to apply.  Reports a protocol error (FATAL)
 * if negotiation was not completed or if the message exceeds the
 * configured caps.  Does NOT invoke handler set_cb's; see
 * ApplyPendingRequestHeaders.
 */
extern void ProcessRequestHeadersMessage(StringInfo msg);

/*
 * Called by PostgresMain at the top of each Query / Parse / Bind /
 * Execute, immediately before the SQL operation begins.  Drains the
 * pending list and invokes each matched handler's set_cb in receipt
 * order.  A handler ERROR propagates as the SQL operation's ERROR,
 * which is the point of the deferral.  No-op when nothing is pending.
 */
extern void ApplyPendingRequestHeaders(void);

/*
 * Drop any pending RequestHeaders entries without dispatching them.
 * Called from PostgresMain's error-recovery path so that a half-
 * dispatched or stranded 'M' does not survive an ERROR into the
 * next operation.  Safe to call when nothing is pending.
 */
extern void ResetPendingRequestHeaders(void);

/*
 * Emit a ParameterStatus message advertising the protocol-level features
 * negotiated for this connection.  Called by PostgresMain immediately
 * after BeginReportingGUCOptions(), so it travels with the rest of the
 * initial ParameterStatus burst that proxies are accustomed to relaying.
 *
 * The key is "protocol_features"; the value is a comma-separated list of
 * negotiated feature names.  Currently only "headers" can appear in the
 * list.  The message is sent only when the list is non-empty, so older
 * proxies that don't know to relay an unknown key don't carry an extra
 * empty message.
 *
 * The presence of this message --- not the absence of
 * NegotiateProtocolVersion --- is the client's only reliable signal that
 * the startup opt-in reached a supporting server.  See the commit
 * message for the proxy false-positive scenario this defends against.
 */
extern void SendProtocolFeaturesParameterStatus(void);

#endif							/* PROTOCOL_HEADERS_H */
