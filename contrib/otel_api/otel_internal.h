/*-------------------------------------------------------------------------
 *
 * otel_internal.h
 *	  Declarations shared between otel_api's own translation units.
 *	  Not installed; not part of any API.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * otel_api/otel_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef OTEL_API_INTERNAL_H
#define OTEL_API_INTERNAL_H

#include "otel.h"

/*
 * otel_api's own sampler policy: the OTEL_TRACES_SAMPLER names, plus
 * the ratio argument for the two ratio-based samplers.  Not part of
 * any audience's API table --- otel_api.sampler / otel_api.sampler_arg
 * are ordinary GUCs, defined in otel.c and consumed only by
 * otel_run_sampler() in otel_api.c.
 */
typedef enum OtelApiSampler
{
	OTEL_SAMPLER_ALWAYS_ON = 0,
	OTEL_SAMPLER_ALWAYS_OFF,
	OTEL_SAMPLER_TRACEIDRATIO,
	OTEL_SAMPLER_PARENTBASED_ALWAYS_ON,
	OTEL_SAMPLER_PARENTBASED_ALWAYS_OFF,
	OTEL_SAMPLER_PARENTBASED_TRACEIDRATIO,
} OtelApiSampler;

/* otel.c: GUCs */
extern char *otel_tracestate_guc;
extern bool otel_emit_spans_to_log;
extern bool otel_parse_sqlcommenter;
extern int	otel_max_open_spans;
extern int	otel_max_session_spans;
extern int	otel_attr_value_max;
extern int	otel_max_span_bytes;
extern char *otel_service_name_guc;
extern char *otel_service_instance_id_guc;
extern int	otel_sampler_mode;		/* OtelApiSampler */
extern double otel_sampler_arg;

/* otel.c: the backend's root trace context */
extern OtelRootContext otel_root_ctx;
extern void otel_root_ctx_reset(void);
extern bool otel_try_apply_sqlcommenter_context(const char *sql);

/* otel_api.c: hooks and the published tables */
extern void otel_api_publish_rendezvous(void);
extern otel_span_emit_hook_type otel_get_span_emit_hook(void);
extern OtelSamplerDecision otel_run_sampler(const OtelTraceId *trace_id,
											bool new_root, bool remote_sampled);
extern bool otel_recording_possible_flag;
extern void otel_update_recording_possible(void);

/* otel_resource.c */
extern void otel_resource_init(void);
extern void otel_resource_attr_add(const char *key, const char *value);
extern const OtelResourceAttribute *otel_resource_attrs_get(int *n_out);
extern OtelInstrumentationScope *otel_tracer_register(const char *name,
													  const char *version,
													  const char *schema_url);

/* otel_parallel.c */
extern void otel_parallel_init(void);
extern void otel_parallel_publish_leader_context(const OtelSpanContext *ctx);
extern void otel_parallel_clear_leader_context(void);
extern bool otel_parallel_get_leader_context(OtelSpanContext *out);

/* otel_producer.c: the producer table and span machinery */
extern const OtelProducerApi otel_producer_api_table;
extern OtelApiCounters otel_counters;
extern void otel_producer_init(void);
extern void otel_emit_span_as_log_line(const OtelSpan *span);
extern bool otel_span_context_of_internal(OtelSpanRef s, OtelSpanContext *out);

#endif							/* OTEL_API_INTERNAL_H */
