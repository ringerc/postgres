/*-------------------------------------------------------------------------
 *
 * protocol_headers.c
 *	  Registry and dispatch for the per-message protocol headers
 *	  (RequestHeaders, message type 'M').
 *
 * Extensions call RegisterProtocolHeaderHandler() to claim a key
 * prefix and a scope.  When a RequestHeaders message arrives, each
 * (key, value) entry is dispatched to the registered handler with the
 * longest matching prefix; unmatched entries are silently ignored.
 *
 * The set callback is invoked once per matching entry.  The clear
 * callback fires at the handler's declared scope boundary --- after
 * ReadyForQuery for statement scope, at COMMIT/ROLLBACK for
 * transaction scope, at backend exit for session scope.  The handler
 * is responsible for its own effect storage; this module only tracks
 * which handlers have been touched per scope so the clear callbacks
 * can be invoked correctly.
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

#include "access/xact.h"
#include "libpq/pqformat.h"
#include "libpq/protocol_headers.h"
#include "storage/ipc.h"
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
	ProtocolHeaderScope scope;
	ProtocolHeaderSetCb set_cb;
	ProtocolHeaderClearCb clear_cb;
	void	   *ctx;

	/*
	 * Per-scope "this handler has been touched" flags.  Indexed by scope
	 * value.  When a flag is true, the handler's clear_cb must be invoked
	 * at the corresponding scope boundary.
	 */
	bool		touched[3];

	struct ProtocolHeaderHandler *next;
} ProtocolHeaderHandler;

/*
 * Singly-linked list of registered handlers.  Allocated in
 * TopMemoryContext so it survives for the life of the backend.
 */
static ProtocolHeaderHandler *handler_list = NULL;

/* One-shot init flag. */
static bool initialized = false;

/* Forward decls for callback hookups. */
static void protocol_headers_xact_callback(XactEvent event, void *arg);
static void protocol_headers_proc_exit(int code, Datum arg);
static void clear_handlers_for_scope(ProtocolHeaderScope scope);
static ProtocolHeaderHandler *lookup_handler(const char *key);


/*
 * Public API: register a handler.
 */
void
RegisterProtocolHeaderHandler(const char *prefix,
							  ProtocolHeaderScope scope,
							  ProtocolHeaderSetCb set_cb,
							  ProtocolHeaderClearCb clear_cb,
							  void *ctx)
{
	ProtocolHeaderHandler *h;
	MemoryContext oldcxt;

	if (prefix == NULL || prefix[0] == '\0')
		elog(ERROR, "protocol header prefix must be non-empty");
	if (set_cb == NULL)
		elog(ERROR, "protocol header handler must supply a set callback");

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	h = palloc0_object(ProtocolHeaderHandler);
	h->prefix = prefix;
	h->prefix_len = strlen(prefix);
	h->scope = scope;
	h->set_cb = set_cb;
	h->clear_cb = clear_cb;
	h->ctx = ctx;

	h->next = handler_list;
	handler_list = h;

	MemoryContextSwitchTo(oldcxt);
}

/*
 * One-time initialization: hook the scope-boundary callbacks.
 */
void
ProtocolHeadersInit(void)
{
	if (initialized)
		return;
	RegisterXactCallback(protocol_headers_xact_callback, NULL);
	on_proc_exit(protocol_headers_proc_exit, (Datum) 0);
	initialized = true;
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
 * msg should be positioned just past the message-type byte and length;
 * pq_getmsgend is called before returning to enforce that no trailing
 * bytes remain.
 */
void
ProcessRequestHeadersMessage(StringInfo msg)
{
	int			n;

	/*
	 * If the GUC has been disabled at runtime, or if the client never
	 * negotiated _pq_.headers, receipt of an 'M' message is a protocol
	 * violation.  Drop the connection.
	 */
	if (!protocol_headers_enabled || !ProtocolHeadersNegotiated)
		ereport(FATAL,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("RequestHeaders message received but protocol headers feature was not negotiated")));

	/*
	 * Cheap top-line size cap: if the entire message body is already
	 * larger than the configured byte cap, reject before reading any of
	 * it.  msg->len is the body length excluding the message-type byte
	 * and Int32 length prefix.
	 */
	if (msg->len > max_protocol_header_size)
		ereport(FATAL,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("RequestHeaders message exceeds max_protocol_header_size (%d > %d)",
						msg->len, max_protocol_header_size)));

	n = pq_getmsgint(msg, 2);
	if (n < 0)
		ereport(FATAL,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid RequestHeaders entry count: %d", n)));
	if (n > max_protocol_header_entries)
		ereport(FATAL,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("RequestHeaders entry count exceeds max_protocol_header_entries (%d > %d)",
						n, max_protocol_header_entries)));

	for (int i = 0; i < n; i++)
	{
		const char *key = pq_getmsgstring(msg);
		const char *value = pq_getmsgstring(msg);
		ProtocolHeaderHandler *h = lookup_handler(key);

		if (h == NULL)
			continue;			/* unmatched keys are silently ignored */

		h->set_cb(key, value, h->ctx);
		h->touched[h->scope] = true;
	}

	pq_getmsgend(msg);
}

/*
 * Called by PostgresMain just before emitting ReadyForQuery.
 */
void
ClearStatementScopeHeaders(void)
{
	clear_handlers_for_scope(PROTOCOL_HEADER_SCOPE_STATEMENT);
}

/*
 * XactCallback wired up at init time.  Fires for top-level transaction
 * boundaries only; sub-transactions are intentionally not handled in
 * this first cut.
 *
 * Statement-scope effects are also cleared on transaction end as a
 * defensive measure --- if a statement scope was somehow left dirty
 * (e.g. an error skipped the ReadyForQuery path), tx end is a safe
 * net.
 */
static void
protocol_headers_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_PREPARE:
			clear_handlers_for_scope(PROTOCOL_HEADER_SCOPE_TRANSACTION);
			clear_handlers_for_scope(PROTOCOL_HEADER_SCOPE_STATEMENT);
			break;
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
			/* nothing */
			break;
	}
}

/*
 * on_proc_exit handler.  Clears session-scope effects (and as a
 * defensive measure, any still-live transaction- and statement-scope
 * effects).
 */
static void
protocol_headers_proc_exit(int code, Datum arg)
{
	clear_handlers_for_scope(PROTOCOL_HEADER_SCOPE_SESSION);
	clear_handlers_for_scope(PROTOCOL_HEADER_SCOPE_TRANSACTION);
	clear_handlers_for_scope(PROTOCOL_HEADER_SCOPE_STATEMENT);
}

/*
 * Invoke clear_cb on every handler whose scope is `scope` and which has
 * been touched.  Handlers that have not been touched, or that have no
 * clear callback, are skipped.
 */
static void
clear_handlers_for_scope(ProtocolHeaderScope scope)
{
	for (ProtocolHeaderHandler *h = handler_list; h != NULL; h = h->next)
	{
		if (!h->touched[scope])
			continue;
		if (h->clear_cb != NULL)
			h->clear_cb(h->ctx);
		h->touched[scope] = false;
	}
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
