/*-------------------------------------------------------------------------
 *
 * trace_context.h
 *	  Single-purpose trace-context protocol message ('M' / TraceContext).
 *
 * Extensions register a single handler via RegisterTraceContextHandler().
 * When a TraceContext message arrives from the client, the handler's
 * apply_cb is invoked immediately with the two fixed fields (traceparent,
 * tracestate).  At the next ReadyForQuery boundary, clear_cb is called so
 * the context does not leak into the next pipeline.
 *
 * Trace context is advisory only.  apply_cb MUST NOT take observable
 * action beyond recording into backend-private state.  It must not be
 * used as the basis of authorization decisions.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/trace_context.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TRACE_CONTEXT_H
#define TRACE_CONTEXT_H

#include "lib/stringinfo.h"

/*
 * apply_cb: record the received trace context.  Record-only (see
 * contracts).  Fires on 'M' receipt, and again on each override 'M'
 * within the same RFQ window.  tracestate may be "" (never NULL on
 * the wire, but may be empty).
 *
 * clear_cb: reset the recorded context.  Fires at the RFQ that ends
 * the window.
 */
typedef void (*TraceContextApplyCb)(const char *traceparent,
									const char *tracestate,
									void *handler_ctx);

typedef void (*TraceContextClearCb)(void *handler_ctx);

/*
 * Register the (single) trace-context consumer.  No prefix, no scope
 * argument - scope is fixed (until-RFQ-or-override) and the field set
 * is fixed (traceparent + tracestate).  Typically called from the
 * consumer's _PG_init().  Only one handler may register; a second
 * registration is an error.
 */
extern void RegisterTraceContextHandler(TraceContextApplyCb apply_cb,
										TraceContextClearCb clear_cb,
										void *handler_ctx);

/*
 * Kill-switch GUC.  When false, any incoming 'M' message on a >=3.3
 * connection is a protocol violation (ERROR).  Default true.
 */
extern PGDLLIMPORT bool trace_context_enabled;

/*
 * Called by PostgresMain when an 'M' message has arrived.  Parses the
 * fixed two-field wire body and invokes the registered apply_cb.
 * Reports a protocol error (ERROR, not FATAL) if the protocol version
 * is <3.3 or if the kill-switch GUC is off.
 */
extern void ProcessTraceContextMessage(StringInfo msg);

/*
 * Called by PostgresMain at the ReadyForQuery point (and from the
 * error-recovery path).  Invokes clear_cb if a context is currently
 * active.  Idempotent - safe to call when nothing is active.
 */
extern void ClearTraceContext(void);

#endif							/* TRACE_CONTEXT_H */
