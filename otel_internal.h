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

#include "otel.h"

/* W3C Trace Context lengths are now declared in the public otel.h
 * since OtelSpanContext uses them. */
#define OTEL_TRACEPARENT_LEN	(2 + 1 + OTEL_TRACE_ID_LEN + 1 + \
								 OTEL_SPAN_ID_LEN + 1 + OTEL_TRACE_FLAGS_LEN)

/*
 * In-memory derived trace context populated by the otel_api.traceparent
 * assign-hook in otel.c; read by the emit_log_hook in otel_log.c.
 * tracestate, when present, lives in the otel_api.tracestate GUC's own
 * string storage and is not duplicated here.
 */
typedef struct OtelContext
{
	bool		is_set;
	bool		sampled_flag_set;
	char		trace_id[OTEL_TRACE_ID_LEN + 1];
	char		span_id[OTEL_SPAN_ID_LEN + 1];
	char		trace_flags[OTEL_TRACE_FLAGS_LEN + 1];
} OtelContext;

extern OtelContext otel_ctx;
extern bool otel_ctx_from_comment;

/* Defined in otel.c. */
extern char *otel_tracestate_guc;
extern bool otel_emit_spans_to_log;
extern bool otel_parse_sqlcommenter;
/* otel_trace_all_queries moved to contrib/otel_postgres_tracing
 * in Phase 4. */

extern void otel_ctx_reset(void);
extern bool try_apply_sqlcommenter_context(const char *sql);

/* Defined in otel_api.c. */
extern void otel_api_publish_rendezvous(void);
extern otel_span_emit_hook_type otel_get_span_emit_hook(void);
extern otel_sampler_hook_type	otel_get_sampler_hook(void);
extern OtelSamplerHookPolicy	otel_get_sampler_hook_policy(void);

/* otel_log_install_hooks / otel_trace_install_hooks /
 * otel_span_record_log_event moved to contrib/otel_postgres_tracing
 * in Phase 4. */

/* Defined in otel_resource.c.  Resource attributes describing the
 * postmaster process, populated at _PG_init from GUCs and runtime
 * facts (cluster system id, hostname).  Exposed to exporters via the
 * OtelTracingApi rendezvous struct.  Also home to the
 * InstrumentationScope registry (otel_tracer_register). */
extern void otel_resource_init(void);
extern const OtelResourceAttribute *otel_resource_attrs_get(int *n_out);
extern OtelInstrumentationScope *otel_tracer_register(const char *name,
													  const char *version,
													  const char *schema_url);

extern char *otel_service_name_guc;
extern char *otel_service_instance_id_guc;

/* Defined in otel_parallel.c.  Per-backend shared-memory slots
 * carrying the leader's currently-active SpanContext for parallel
 * workers to read.  Replaces the otel.current_span_id GUC that the
 * Phase 3 split refactor removed. */
extern void otel_parallel_init(void);
extern void otel_parallel_publish_leader_context(const char *trace_id,
												 const char *span_id,
												 const char *trace_flags);
extern void otel_parallel_clear_leader_context(void);
extern bool otel_parallel_get_leader_context(OtelParallelContext *out);

/* Defined in otel_producer.c.  Producer-side API plumbed into the
 * OtelTracingApi struct in otel_api.c. */
extern void otel_producer_init(void);
extern void otel_producer_span_link_to_active_and_push(OtelSpan *span);
extern void otel_producer_span_set_parent_explicit(OtelSpan *span,
												   const OtelSpanContext *parent);
extern const OtelSpanContext *otel_producer_span_current_context(void);
extern const OtelSpanContext *otel_producer_span_root_context(void);
extern int otel_producer_span_stack_depth(void);
extern void otel_producer_span_emit(OtelSpan *span);

/* Producer-side convenience helpers (also in otel_producer.c) ---
 * not exposed via otel.h as extern symbols because peer-extension
 * symbol resolution is not portable.  Consumers reach them via the
 * OtelTracingApi function pointers; the rendezvous-struct
 * initialiser in otel_api.c uses the names below. */
extern void otel_span_init(OtelSpan *span,
						   const OtelInstrumentationScope *scope,
						   const char *name,
						   OtelSpanKind kind);
extern bool otel_span_add_attribute_string(OtelSpan *span,
										   const char *key,
										   const char *value);

/* Internal-only push (no parent-context fetch).  Used by
 * otel_trace.c during Phase 2 migration so the existing
 * start_span / start_utility_span code can populate span fields
 * (including the parallel-worker leader-span-id parent linkage)
 * itself and just push the result onto the active stack.  External
 * consumers use api->span_link_to_active_and_push instead, which
 * fetches the parent from the active stack / root context. */
extern void otel_producer_span_push(OtelSpan *span);

/* JSON-log fallback emitter; lives in otel_trace.c for now.  Phase
 * 2 exposes it so otel_producer.c's dispatch can call it too,
 * giving the producer-API emit path the same log-line emission as
 * the legacy finalize_span path. */
extern void otel_emit_span_as_log_line(const OtelSpan *span);

#endif							/* CONTRIB_OTEL_INTERNAL_H */
