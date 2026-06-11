/*-------------------------------------------------------------------------
 *
 * protocol_headers.c
 *	  Registry and dispatch for the per-message protocol headers
 *	  (RequestHeaders, message type 'M').
 *
 * Extensions call RegisterProtocolHeaderHandler() to claim a key
 * prefix.  When a RequestHeaders message arrives, each (key, value)
 * entry is bound to the registered handler with the longest matching
 * prefix; unmatched entries are silently ignored.
 *
 * Dispatch is *deferred*.  ProcessRequestHeadersMessage parses 'M' and
 * stashes the (key, value, handler) tuples on a backend-private
 * pending list; handler set_cb's run only at the start of the next
 * Query / Parse / Bind / Execute, via ApplyPendingRequestHeaders.
 * The point of the deferral is to bind a handler ERROR to the
 * operation the headers were intended to prefix --- so an extension
 * that throws inside set_cb fails the SQL operation the client was
 * about to run, rather than failing a standalone 'M' message and
 * leaving the next pipelined operation to run with half-applied
 * state.  See ApplyPendingRequestHeaders for the lifecycle details.
 *
 * This module is otherwise lifecycle-free: it does not store applied
 * state and does not invoke any clear callback.  Each handler is
 * responsible for its own state and for wiring up whatever cleanup
 * it needs (pre_ready_for_query_hook for statement-scope effects,
 * RegisterXactCallback for transaction-scope, on_proc_exit for
 * session-scope).  Pushing lifecycle into the extension keeps the
 * dispatcher small and lets each extension choose its own semantics
 * --- including, for example, whether and how to honour subxact
 * rollback.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/libpq/protocol_headers.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/protocol.h"
#include "libpq/protocol_headers.h"
#include "nodes/pg_list.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"

/* Negotiation state set during StartupPacket processing. */
bool		ProtocolHeadersNegotiated = false;

/* GUCs (defaults are set here; entries are registered in guc_tables.c). */
bool		protocol_headers_enabled = true;
int			max_protocol_header_entries = 64;
int			max_protocol_header_size = 4096;

/*
 * One registered handler.
 */
typedef struct ProtocolHeaderHandler
{
	const char *prefix;			/* points at extension-owned storage */
	size_t		prefix_len;
	ProtocolHeaderSetCb set_cb;
	void	   *ctx;

	struct ProtocolHeaderHandler *next;
} ProtocolHeaderHandler;

/*
 * Singly-linked list of registered handlers.  Allocated in
 * TopMemoryContext so it survives for the life of the backend.
 */
static ProtocolHeaderHandler *handler_list = NULL;

/*
 * One pending entry parsed from an 'M' message but not yet dispatched.
 * The key and value strings are deep copies owned by PendingHeadersContext;
 * the handler pointer references handler_list which lives in
 * TopMemoryContext and persists for the lifetime of the backend.
 */
typedef struct PendingHeaderEntry
{
	const char *key;
	const char *value;
	ProtocolHeaderHandler *handler; /* may be NULL = no match, retain for symmetry */
} PendingHeaderEntry;

/*
 * Pending entries awaiting their next operation.  The List is itself
 * allocated in PendingHeadersContext, so a single context reset cleans
 * up all the strings, all the PendingHeaderEntry palloc-objects, and
 * the List cells in one shot.  Static pointer is cleared to NIL
 * BEFORE the reset.
 */
static List *pending_entries = NIL;
static MemoryContext PendingHeadersContext = NULL;

static ProtocolHeaderHandler *lookup_handler(const char *key);
static void ensure_pending_context(void);


/*
 * Public API: register a handler.
 */
void
RegisterProtocolHeaderHandler(const char *prefix,
							  ProtocolHeaderSetCb set_cb,
							  void *ctx)
{
	ProtocolHeaderHandler *h;
	MemoryContext oldcxt;
	size_t		prefix_len;

	if (prefix == NULL || prefix[0] == '\0')
		elog(ERROR, "protocol header prefix must be non-empty");
	if (set_cb == NULL)
		elog(ERROR, "protocol header handler must supply a set callback");

	prefix_len = strlen(prefix);

	/*
	 * Reject exact-prefix collisions at registration time rather than
	 * silently letting the second-loaded extension win the dispatch.
	 * Two extensions claiming the same prefix is always a configuration
	 * error: one of them has misappropriated the other's namespace.
	 * Subset/superset prefix relations (e.g. "otel." and "otel.metrics.")
	 * are fine --- the lookup picks the longest prefix unambiguously.
	 *
	 * Registration happens from each extension's _PG_init under
	 * shared_preload_libraries, i.e. in postmaster context.  ereport(ERROR)
	 * here aborts postmaster startup with a clear message, which is the
	 * right severity: the operator must resolve the conflict before any
	 * backend serves traffic.
	 */
	for (ProtocolHeaderHandler *existing = handler_list;
		 existing != NULL;
		 existing = existing->next)
	{
		if (existing->prefix_len == prefix_len &&
			memcmp(existing->prefix, prefix, prefix_len) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("protocol header prefix \"%s\" is already registered",
							prefix),
					 errhint("Two extensions cannot register handlers for identical prefixes; the registration order is silent and ambiguous.  Resolve by choosing distinct prefixes for the conflicting extensions.")));
	}

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	h = palloc0_object(ProtocolHeaderHandler);
	h->prefix = prefix;
	h->prefix_len = prefix_len;
	h->set_cb = set_cb;
	h->ctx = ctx;

	h->next = handler_list;
	handler_list = h;

	MemoryContextSwitchTo(oldcxt);
}

/*
 * Emit a ParameterStatus advertising the protocol features negotiated
 * for this connection.  See header for rationale; see commit log for
 * the proxy false-positive scenario this defends against.
 *
 * The message is only sent when at least one feature is active so
 * older clients/proxies that don't know to relay an unknown key are
 * not burdened with empty messages.
 */
void
SendProtocolFeaturesParameterStatus(void)
{
	StringInfoData buf;
	StringInfoData features;

	if (whereToSendOutput != DestRemote)
		return;

	initStringInfo(&features);
	if (ProtocolHeadersNegotiated)
		appendStringInfoString(&features, "headers");
	/* future negotiated features append here, comma-separated */

	if (features.len == 0)
	{
		pfree(features.data);
		return;
	}

	pq_beginmessage(&buf, PqMsg_ParameterStatus);
	pq_sendstring(&buf, "protocol_features");
	pq_sendstring(&buf, features.data);
	pq_endmessage(&buf);

	pfree(features.data);
}

/*
 * Process a freshly-arrived RequestHeaders ('M') message body.
 *
 * Wire format:
 *	  Int16  N (number of entries)
 *	  N × {
 *		  String key
 *		  String value
 *	  }
 *
 * msg should be positioned just past the message-type byte and length.
 *
 * Parse is two-pass and atomic:
 *	  1. Read every entry, enforce the per-entry size cap, and resolve
 *	     the matching handler once.  Per-entry data is stashed in a
 *	     small temporary array; the (key, value) pointers reference
 *	     storage inside msg, which remains live through the call.
 *	  2. After pq_getmsgend confirms the frame is well-formed, deep-
 *	     copy each parsed entry into PendingHeadersContext and append
 *	     it to pending_entries.
 *
 * Note: set_cb is NOT invoked here.  Dispatch is deferred to the next
 * Query / Parse / Bind / Execute via ApplyPendingRequestHeaders().
 * That binds handler errors to the operation the headers were intended
 * to prefix, so a handler ERROR fails the SQL operation rather than
 * leaving a successfully-completed SELECT to run with half-applied
 * header state.  See the file header for the broader rationale.
 *
 * Multiple 'M' messages before the next operation accumulate in
 * receipt order: ApplyPendingRequestHeaders walks the list in that
 * order, so M1.key=a then M2.key=a results in the second set_cb seeing
 * value=a-from-M2 last.  Per-key replace-vs-merge semantics live in
 * the handler.
 */
void
ProcessRequestHeadersMessage(StringInfo msg)
{
	typedef struct ParsedEntry
	{
		const char *key;
		const char *value;
		ProtocolHeaderHandler *handler; /* NULL = no match, retain for symmetry */
	} ParsedEntry;

	int			n;
	ParsedEntry *entries;
	MemoryContext oldcxt;

	/*
	 * If the GUC has been disabled at runtime, or if the client never
	 * negotiated _pq_.headers, receipt of an 'M' message is a protocol
	 * violation.  Report an ERROR rather than FATAL --- the message body
	 * has already been consumed by the framing layer, so there is no
	 * desynchronized protocol state to recover from, and the session
	 * remains usable via the standard sigsetjmp recovery path.  Killing
	 * the backend on receipt of one unexpected 'M' is disproportionate
	 * and inconsistent with how other in-band protocol parse errors are
	 * reported.
	 */
	if (!protocol_headers_enabled || !ProtocolHeadersNegotiated)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("RequestHeaders message received but protocol headers feature was not negotiated")));

	/*
	 * The total wire-level bound on the message is PQ_SMALL_MESSAGE_LIMIT
	 * (see PqMsg_RequestHeaders in PostgresMain); we don't re-check it
	 * here.  The per-message GUC caps below limit (a) how many entries
	 * can appear in one message and (b) how large any single (key, value)
	 * entry may be on the wire.
	 */

	n = pq_getmsgint(msg, 2);
	if (n < 0)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid RequestHeaders entry count: %d", n)));
	if (n > max_protocol_header_entries)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("RequestHeaders entry count exceeds max_protocol_header_entries (%d > %d)",
						n, max_protocol_header_entries)));

	entries = (n > 0) ? palloc_array(ParsedEntry, n) : NULL;

	for (int i = 0; i < n; i++)
	{
		size_t		wire_start = msg->cursor;
		const char *key = pq_getmsgstring(msg);
		const char *value = pq_getmsgstring(msg);
		size_t		entry_size;

		/*
		 * Per-entry size cap.  Counts the bytes that appear on the wire
		 * for this (key, value) pair --- the two NUL-terminated strings
		 * including their terminators.  Empty key + empty value is 2
		 * bytes; setting the GUC to 0 therefore rejects every entry.
		 *
		 * Compute the wire size from the cursor delta rather than
		 * strlen() on the returned pointers: (a) pq_getmsgstring has
		 * already walked the bytes once to find the NUL, so a second
		 * strlen() is wasteful (an attacker could provoke 2x the parse
		 * cost), and (b) the returned strings are after
		 * pq_client_to_server's encoding conversion --- their byte
		 * count can differ from the on-wire size, which is the thing
		 * the GUC is supposed to bound.
		 */
		entry_size = msg->cursor - wire_start;
		if (entry_size > (size_t) max_protocol_header_size)
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("RequestHeaders entry %d (key \"%s\") exceeds max_protocol_header_size (%zu > %d)",
							i, key, entry_size, max_protocol_header_size)));

		entries[i].key = key;
		entries[i].value = value;
		entries[i].handler = lookup_handler(key);
	}

	/*
	 * Validate the frame as a whole before stashing.  If the client
	 * sent trailing garbage or a truncated entry, this raises ERROR
	 * before any entry lands on the pending list, so a malformed frame
	 * cannot become observable to a later operation.
	 */
	pq_getmsgend(msg);

	/*
	 * Stash parsed entries on the pending list.  Deep-copy the key and
	 * value strings into PendingHeadersContext so they survive past
	 * the message-context reset that follows return.
	 */
	ensure_pending_context();
	oldcxt = MemoryContextSwitchTo(PendingHeadersContext);
	for (int i = 0; i < n; i++)
	{
		PendingHeaderEntry *pe = palloc_object(PendingHeaderEntry);

		pe->key = pstrdup(entries[i].key);
		pe->value = pstrdup(entries[i].value);
		pe->handler = entries[i].handler;
		pending_entries = lappend(pending_entries, pe);
	}
	MemoryContextSwitchTo(oldcxt);

	if (entries != NULL)
		pfree(entries);
}

/*
 * Apply any pending RequestHeaders entries by invoking each matched
 * handler's set_cb in receipt order.  Called by PostgresMain at the
 * top of each Query / Parse / Bind / Execute, immediately before the
 * SQL operation begins.
 *
 * Errors raised by a handler propagate as the operation's error,
 * which (a) makes the failure scope identical to the SQL operation
 * the headers were intended to prefix, and (b) means the operation
 * does not run.  Earlier handlers' effects in the same batch persist
 * --- per-handler rollback is the handler's responsibility, just as
 * with any other extension hook.
 *
 * The pending list is cleared BEFORE dispatching so that:
 *	  - A handler that recursively calls ProcessRequestHeadersMessage
 *	    (unlikely but possible) sees a clean state.
 *	  - A handler that throws ERROR leaves the cleared state behind,
 *	    so the next operation does not re-apply anything; the
 *	    sigsetjmp recovery in PostgresMain additionally calls
 *	    ResetPendingRequestHeaders() to free the context.
 */
void
ApplyPendingRequestHeaders(void)
{
	List	   *to_apply;
	ListCell   *lc;

	if (pending_entries == NIL)
		return;

	to_apply = pending_entries;
	pending_entries = NIL;

	foreach(lc, to_apply)
	{
		PendingHeaderEntry *pe = lfirst(lc);

		if (pe->handler == NULL)
			continue;			/* unmatched keys are silently ignored */

		pe->handler->set_cb(pe->key, pe->value, pe->handler->ctx);
	}

	/*
	 * All entries dispatched (or skipped) cleanly.  Reset the context
	 * to release the to_apply list cells and the pstrdup'd strings.
	 * On the ERROR path we never reach here; the sigsetjmp recovery in
	 * PostgresMain calls ResetPendingRequestHeaders() to do the same
	 * cleanup.
	 */
	MemoryContextReset(PendingHeadersContext);
}

/*
 * Drop any pending entries without dispatching.  Called from
 * PostgresMain's error-recovery block so a half-applied or
 * undispatched 'M' does not survive an ERROR into the next operation.
 *
 * Safe to call when no entries are pending (no-op) and before the
 * context has been created (no-op).
 */
void
ResetPendingRequestHeaders(void)
{
	pending_entries = NIL;
	if (PendingHeadersContext != NULL)
		MemoryContextReset(PendingHeadersContext);
}

/*
 * Lazily create PendingHeadersContext under TopMemoryContext.  The
 * context survives across resets; we just reuse it.
 */
static void
ensure_pending_context(void)
{
	if (PendingHeadersContext == NULL)
		PendingHeadersContext =
			AllocSetContextCreate(TopMemoryContext,
								  "RequestHeadersPending",
								  ALLOCSET_SMALL_SIZES);
}

/*
 * Find the handler whose prefix is the longest prefix of key.  Returns
 * NULL if no handler matches.
 */
static ProtocolHeaderHandler *
lookup_handler(const char *key)
{
	ProtocolHeaderHandler *best = NULL;
	size_t		best_len = 0;
	size_t		keylen = strlen(key);

	for (ProtocolHeaderHandler *h = handler_list; h != NULL; h = h->next)
	{
		if (h->prefix_len > keylen)
			continue;
		if (h->prefix_len <= best_len)
			continue;
		if (memcmp(key, h->prefix, h->prefix_len) != 0)
			continue;
		best = h;
		best_len = h->prefix_len;
	}
	return best;
}
