/*-------------------------------------------------------------------------
 *
 * otel_api.c
 *	  The published API tables, hook registration and sampling policy.
 *
 * otel_api publishes one root table (OtelApi) through a rendezvous
 * variable, pointing to one table per audience.  The producer table
 * lives in otel_producer.c with the span machinery; the exporter and
 * internal tables are here.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  otel_api/otel_api.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "fmgr.h"

#include "otel_internal.h"

#ifdef PG_HAVE_XACT_TRACE_CONTEXT
#include "access/xact.h"
#endif

static otel_span_emit_hook_type otel_span_emit_hook = NULL;

/*
 * True when a finished span has somewhere to go: an emit hook, or log
 * emission --- and otel_api.sampler isn't always_off, folded in on
 * purpose as a full off switch.  See README "Sampling".  Producers
 * read it inline through the producer table, so the untraced case
 * costs no call per span.
 */
bool		otel_recording_possible_flag = false;

void
otel_update_recording_possible(void)
{
	otel_recording_possible_flag = (otel_span_emit_hook != NULL ||
									otel_emit_spans_to_log) &&
		otel_sampler_mode != OTEL_SAMPLER_ALWAYS_OFF;
}

static void
api_register_emit_hook(otel_span_emit_hook_type new_hook,
					   otel_span_emit_hook_type *prev_out)
{
	if (prev_out)
		*prev_out = otel_span_emit_hook;
	otel_span_emit_hook = new_hook;
	otel_update_recording_possible();
}

otel_span_emit_hook_type
otel_get_span_emit_hook(void)
{
	return otel_span_emit_hook;
}

/*
 * OTel consistent probability sampling (W3C Trace Context level 2):
 * randomness R is the trace ID's low 56 bits (the last 7 bytes, taken
 * as a big-endian uint56); the rejection threshold T is
 * round((1 - ratio) * 2^56).  Sample iff R >= T.  ratio <= 0 always
 * rejects; ratio >= 1 always samples (T == 0, and R >= 0 always holds).
 */
static bool
otel_traceidratio_sample(const OtelTraceId *trace_id, double ratio)
{
	uint64		r = 0;
	uint64		threshold;

	if (ratio >= 1.0)
		return true;
	if (ratio <= 0.0)
		return false;

	for (int i = OTEL_TRACE_ID_BYTES - 7; i < OTEL_TRACE_ID_BYTES; i++)
		r = (r << 8) | trace_id->b[i];

	/* 2^56, as a double; round-half-to-even is fine here. */
	threshold = (uint64) rint((1.0 - ratio) * 72057594037927936.0);
	return r >= threshold;
}

/*
 * otel_api's own sampling policy (otel_api.sampler / otel_api.sampler_arg).
 * Called only where otel_producer.c has no local parent to inherit a
 * decision from: a brand-new root (new_root = true, remote_sampled
 * ignored) or a remote parent (new_root = false, remote_sampled is that
 * parent's W3C sampled bit).  trace_id is the span's own trace ID (the
 * freshly generated one for a new root, or the remote parent's for a
 * remote parent) --- the only input traceidratio needs.
 */
OtelSamplerDecision
otel_run_sampler(const OtelTraceId *trace_id, bool new_root, bool remote_sampled)
{
	switch (otel_sampler_mode)
	{
		case OTEL_SAMPLER_ALWAYS_ON:
			return OTEL_SAMPLE_RECORD_AND_SAMPLE;
		case OTEL_SAMPLER_ALWAYS_OFF:
			return OTEL_SAMPLE_DROP;
		case OTEL_SAMPLER_TRACEIDRATIO:
			return otel_traceidratio_sample(trace_id, otel_sampler_arg)
				? OTEL_SAMPLE_RECORD_AND_SAMPLE : OTEL_SAMPLE_DROP;
		case OTEL_SAMPLER_PARENTBASED_ALWAYS_ON:
			if (!new_root)
				return remote_sampled
					? OTEL_SAMPLE_RECORD_AND_SAMPLE : OTEL_SAMPLE_DROP;
			return OTEL_SAMPLE_RECORD_AND_SAMPLE;
		case OTEL_SAMPLER_PARENTBASED_ALWAYS_OFF:
			if (!new_root)
				return remote_sampled
					? OTEL_SAMPLE_RECORD_AND_SAMPLE : OTEL_SAMPLE_DROP;
			return OTEL_SAMPLE_DROP;
		case OTEL_SAMPLER_PARENTBASED_TRACEIDRATIO:
			if (!new_root)
				return remote_sampled
					? OTEL_SAMPLE_RECORD_AND_SAMPLE : OTEL_SAMPLE_DROP;
			return otel_traceidratio_sample(trace_id, otel_sampler_arg)
				? OTEL_SAMPLE_RECORD_AND_SAMPLE : OTEL_SAMPLE_DROP;
	}
	return OTEL_SAMPLE_RECORD_AND_SAMPLE;	/* unreachable */
}

static void
api_get_root_context(OtelRootContext *out)
{
	*out = otel_root_ctx;
	out->ctx.tracestate = (otel_tracestate_guc && otel_tracestate_guc[0])
		? otel_tracestate_guc : NULL;
}

static void
api_reset_root_context(void)
{
	otel_root_ctx_reset();
}

static void
api_get_counters(OtelApiCounters *out)
{
	*out = otel_counters;
}

#ifdef PG_HAVE_XACT_TRACE_CONTEXT
/*
 * Commit-record trace context: record the context of the span active at
 * commit, if it is sampled.
 */
static bool
otel_commit_trace_context_cb(xl_xact_trace_context *tc)
{
	OtelSpanContext ctx;

	if (!otel_span_context_of_internal(OTEL_SPAN_NONE, &ctx) ||
		!otel_span_context_sampled(&ctx))
		return false;
	StaticAssertStmt(sizeof(tc->trace_id) == OTEL_TRACE_ID_BYTES &&
					 sizeof(tc->span_id) == OTEL_SPAN_ID_BYTES,
					 "xl_xact_trace_context ID sizes");
	memcpy(tc->trace_id, ctx.trace_id.b, OTEL_TRACE_ID_BYTES);
	memcpy(tc->span_id, ctx.span_id.b, OTEL_SPAN_ID_BYTES);
	tc->trace_flags = ctx.trace_flags;
	memset(tc->pad, 0, sizeof(tc->pad));
	return true;
}
#endif							/* PG_HAVE_XACT_TRACE_CONTEXT */

static const OtelExporterApi otel_exporter_api_table = {
	.version = OTEL_EXPORTER_API_VERSION,
	.struct_size = sizeof(OtelExporterApi),
	.register_emit_hook = api_register_emit_hook,
	.get_resource_attributes = otel_resource_attrs_get,
};

static const OtelInternalApi otel_internal_api_table = {
	.version = OTEL_INTERNAL_API_VERSION,
	.struct_size = sizeof(OtelInternalApi),
	.get_root_context = api_get_root_context,
	.reset_root_context = api_reset_root_context,
	.try_apply_sqlcommenter_context = otel_try_apply_sqlcommenter_context,
	.parallel_publish_leader_context = otel_parallel_publish_leader_context,
	.parallel_clear_leader_context = otel_parallel_clear_leader_context,
	.parallel_get_leader_context = otel_parallel_get_leader_context,
	.get_counters = api_get_counters,
};

static const OtelApi otel_api_root = {
	.version = OTEL_ROOT_API_VERSION,
	.struct_size = sizeof(OtelApi),
	.producer = &otel_producer_api_table,
	.exporter = &otel_exporter_api_table,
	.internal = &otel_internal_api_table,
};

/*
 * Publish the root table and register everything exporters queued
 * before otel_api loaded.  Called from _PG_init.
 */
void
otel_api_publish_rendezvous(void)
{
	void	  **slot = find_rendezvous_variable(OTEL_API_RENDEZVOUS_NAME);
	void	  **pending = find_rendezvous_variable(OTEL_EXPORTER_PENDING_NAME);
	OtelPendingRegistration *req = (OtelPendingRegistration *) *pending;

	*slot = (void *) &otel_api_root;
	*pending = NULL;

	/* The list is LIFO; the last-queued registration ends up outermost. */
	for (; req != NULL; req = req->next)
	{
		if (req->emit_hook)
			api_register_emit_hook(req->emit_hook, req->emit_prev_out);
	}

#ifdef PG_HAVE_XACT_TRACE_CONTEXT
	commit_trace_context_hook = otel_commit_trace_context_cb;
#endif
}
