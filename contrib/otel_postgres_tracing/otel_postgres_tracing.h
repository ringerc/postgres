/*-------------------------------------------------------------------------
 *
 * otel_postgres_tracing.h
 *	  Internal declarations shared between the otel_postgres_tracing
 *	  translation units.  NOT installed.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/otel_postgres_tracing/otel_postgres_tracing.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONTRIB_OTEL_POSTGRES_TRACING_H
#define CONTRIB_OTEL_POSTGRES_TRACING_H

#include "utils/elog.h"

#include <otel_api/otel.h>


/*
 * Cached pointer to the OtelTracingApi, lazily populated by
 * otel_pg_ensure().  NULL until the provider is first seen.
 * otel_trace.c / otel_log.c use otel_pg_ensure() on hot paths
 * rather than reading this directly.
 */
extern const OtelTracingApi *otel_api;

/*
 * InstrumentationScope handle for this module.  Populated lazily
 * on the first otel_pg_ensure() success.
 */
extern const OtelInstrumentationScope *otel_pg_tracer;

/*
 * Lazy provider resolution.  Returns the OtelTracingApi pointer
 * (or NULL when absent/incompatible).  Also registers the tracer
 * scope on first success.  Safe to call on every hot-path invocation.
 */
extern const OtelTracingApi *otel_pg_ensure(void);

/* Behaviour GUCs owned by this module. */
extern bool otel_trace_all_queries;

/* Defined in otel_trace.c.  Called once from _PG_init. */
extern void otel_trace_install_hooks(void);

/* Defined in otel_log.c.  Called once from _PG_init. */
extern void otel_log_install_hooks(void);

/* Called from otel_log.c's emit_log_hook to record an ereport as a
 * span event when a span is active.  No-op otherwise. */
extern void otel_span_record_log_event(ErrorData *edata);

/* Defined in otel_sdt_bridge.c.  Called once from _PG_init. */
extern void otel_sdt_install(void);

#ifdef PG_HAVE_SDT_PROBE_HOOK
/*
 * Cross-module link wiring between the SDT-bridge transaction span (pg.txn)
 * and the hook-based statement spans in otel_trace.c.  Defined in
 * otel_sdt_bridge.c; only available when the core advertises the SDT probe
 * hook.  otel_sdt_get_txn_context() snapshots the live pg.txn identity (false
 * if no transaction span is active); otel_sdt_link_stmt_to_txn() adds a link
 * from the active pg.txn span back to a statement span.
 */
extern bool otel_sdt_get_txn_context(OtelSpanContext *out);
extern void otel_sdt_link_stmt_to_txn(const char *trace_id,
									  const char *span_id,
									  const char *trace_flags);
#endif							/* PG_HAVE_SDT_PROBE_HOOK */


#endif							/* CONTRIB_OTEL_POSTGRES_TRACING_H */
