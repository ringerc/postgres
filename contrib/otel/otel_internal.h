/*-------------------------------------------------------------------------
 *
 * otel_internal.h
 *	  Internal declarations shared between the contrib/otel translation
 *	  units.  NOT installed; not part of any public ABI.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/otel/otel_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONTRIB_OTEL_INTERNAL_H
#define CONTRIB_OTEL_INTERNAL_H

#include "utils/elog.h"

/* W3C Trace Context lengths. */
#define OTEL_TRACE_ID_LEN		32
#define OTEL_SPAN_ID_LEN		16
#define OTEL_TRACE_FLAGS_LEN	2
#define OTEL_TRACEPARENT_LEN	(2 + 1 + OTEL_TRACE_ID_LEN + 1 + \
								 OTEL_SPAN_ID_LEN + 1 + OTEL_TRACE_FLAGS_LEN)

/*
 * In-memory derived trace context populated by the otel.traceparent
 * assign-hook in otel.c; read by the emit_log_hook in otel_log.c.
 * tracestate, when present, lives in the otel.tracestate GUC's own
 * string storage and is not duplicated here.
 */
typedef struct OtelContext
{
	bool		is_set;
	char		trace_id[OTEL_TRACE_ID_LEN + 1];
	char		span_id[OTEL_SPAN_ID_LEN + 1];
	char		trace_flags[OTEL_TRACE_FLAGS_LEN + 1];
} OtelContext;

extern OtelContext otel_ctx;

/* Defined in otel.c. */
extern char *otel_tracestate_guc;
extern char *otel_current_span_id_guc;
extern bool otel_emit_spans_to_log;
extern bool otel_trace_all_queries;

/* Defined in otel_log.c.  Called once from _PG_init. */
extern void otel_log_install_hooks(void);

/* Defined in otel_trace.c.  Called once from _PG_init. */
extern void otel_trace_install_hooks(void);

/* Called from otel_log.c's emit_log_hook to record an ereport as a
 * span event when a span is active.  No-op otherwise. */
extern void otel_span_record_log_event(ErrorData *edata);

#endif							/* CONTRIB_OTEL_INTERNAL_H */
