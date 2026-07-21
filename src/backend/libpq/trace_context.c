/*-------------------------------------------------------------------------
 *
 * trace_context.c
 *	  Single-purpose trace-context protocol message ('M' / TraceContext).
 *
 * A single handler is registered via RegisterTraceContextHandler().
 * When a TraceContext ('M') message arrives, ProcessTraceContextMessage
 * parses the two fixed wire fields (traceparent, tracestate) and
 * immediately invokes the handler's apply_cb.  There is no deferred
 * dispatch, no pending list, no handler registry.
 *
 * At the next ReadyForQuery boundary, ClearTraceContext invokes the
 * handler's clear_cb so the context does not persist into the next
 * pipeline.  An override 'M' within the same window calls apply_cb
 * again with the new values (last-write-wins, one active context).
 *
 * Trace context is advisory: malformed values in apply_cb must be
 * swallowed at LOG level so the operation proceeds untagged.  Protocol-
 * violation reports (wrong version, kill-switch off) are ERROR, not FATAL.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/libpq/trace_context.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq/libpq.h"
#include "libpq/libpq-be.h"
#include "libpq/pqcomm.h"
#include "libpq/pqformat.h"
#include "libpq/trace_context.h"

/* Kill-switch GUC (default true; registered in guc_parameters.dat). */
bool		trace_context_enabled = true;

/* Single registered handler - NULL until RegisterTraceContextHandler fires. */
static TraceContextApplyCb registered_apply_cb = NULL;
static TraceContextClearCb registered_clear_cb = NULL;
static void *registered_handler_ctx = NULL;

/* Is a context currently active (apply_cb was called since last clear)? */
static bool context_active = false;


/*
 * Register the single trace-context consumer.  Errors if called twice.
 * Typically called from the consumer's _PG_init().
 */
void
RegisterTraceContextHandler(TraceContextApplyCb apply_cb,
							TraceContextClearCb clear_cb,
							void *handler_ctx)
{
	if (registered_apply_cb != NULL)
		elog(ERROR, "a trace-context handler is already registered");
	if (apply_cb == NULL)
		elog(ERROR, "trace-context handler must supply an apply callback");
	if (clear_cb == NULL)
		elog(ERROR, "trace-context handler must supply a clear callback");

	registered_apply_cb = apply_cb;
	registered_clear_cb = clear_cb;
	registered_handler_ctx = handler_ctx;
}

/*
 * Process a freshly-arrived TraceContext ('M') message body.
 *
 * Wire format:
 *	  String  traceparent
 *	  String  tracestate
 *
 * Parses the two fixed fields and immediately invokes apply_cb.
 * Reports ERROR (not FATAL) for protocol violations (version gate,
 * kill-switch).  Malformed field values are the handler's problem;
 * the advisory contract means the handler should swallow them at LOG.
 */
void
ProcessTraceContextMessage(StringInfo msg)
{
	const char *traceparent;
	const char *tracestate;

	/*
	 * Gate: protocol 3.3 required.  FrontendProtocol is set from the
	 * negotiated version during startup; PG_PROTOCOL_MINOR 3 means 3.3.
	 */
	if (!trace_context_enabled ||
		PG_PROTOCOL_MINOR(FrontendProtocol) < 3)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("TraceContext message received but trace-context protocol is not available on this connection")));

	traceparent = pq_getmsgstring(msg);
	tracestate = pq_getmsgstring(msg);
	pq_getmsgend(msg);

	if (registered_apply_cb != NULL)
	{
		registered_apply_cb(traceparent, tracestate, registered_handler_ctx);
		context_active = true;
	}
}

/*
 * Clear the active trace context.  Called at the ReadyForQuery emission
 * point and from the error-recovery path.  Idempotent.
 */
void
ClearTraceContext(void)
{
	if (!context_active)
		return;

	if (registered_clear_cb != NULL)
		registered_clear_cb(registered_handler_ctx);

	context_active = false;
}
