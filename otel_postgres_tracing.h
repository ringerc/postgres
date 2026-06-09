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


/* Cached pointer to the OtelTracingApi rendezvous struct,
 * resolved once at _PG_init time.  Never NULL after _PG_init
 * returns successfully (the lookup ereport(ERROR)s on failure). */
extern const OtelTracingApi *otel_api;

/* InstrumentationScope handle for this module, registered at
 * _PG_init.  Tagged onto every OtelSpan produced by otel_trace.c. */
extern const OtelInstrumentationScope *otel_pg_tracer;

/* Behaviour GUCs owned by this module. */
extern bool otel_trace_all_queries;

/* Defined in otel_trace.c.  Called once from _PG_init. */
extern void otel_trace_install_hooks(void);

/* Defined in otel_log.c.  Called once from _PG_init. */
extern void otel_log_install_hooks(void);

/* Called from otel_log.c's emit_log_hook to record an ereport as a
 * span event when a span is active.  No-op otherwise. */
extern void otel_span_record_log_event(ErrorData *edata);


#endif							/* CONTRIB_OTEL_POSTGRES_TRACING_H */
