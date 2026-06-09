/*-------------------------------------------------------------------------
 *
 * otel_api.c
 *	  Extension API surface for contrib/otel.
 *
 * Out-of-tree exporter / SDK modules consume contrib/otel via the
 * OtelTracingApi struct, looked up at _PG_init time through the
 * rendezvous variable named OTEL_TRACING_API_RENDEZVOUS_NAME.  See
 * the public API documentation in otel.h.
 *
 * This translation unit owns:
 *	  * the storage for the registered hooks
 *		(otel_span_emit_hook, otel_sampler_hook);
 *	  * the api_register_* functions plumbed through the
 *		OtelTracingApi struct;
 *	  * the OtelTracingApi singleton and its publication into the
 *		rendezvous slot.
 *
 * Internal getters (otel_get_*) are exposed via otel_internal.h so
 * otel_trace.c can read the currently-registered hooks on the hot
 * path without taking a direct symbol dependency on this file's
 * static state.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/otel/otel_api.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"

#include "otel.h"
#include "otel_internal.h"

/*
 * Internal storage for the registered hooks.  External modules do
 * NOT touch these directly; they call through the OtelTracingApi
 * function pointers.  file-static --- these are not part of the
 * contrib/otel ABI.
 */
static otel_span_emit_hook_type otel_span_emit_hook = NULL;
static otel_sampler_hook_type	otel_sampler_hook = NULL;

/*
 * Sampler-hook invocation policy.  Default is OTel-SDK-ParentBased
 * compliant: call the hook only when the propagated W3C sampled bit
 * is unset.  Exporters that want different semantics override via
 * api->set_sampler_policy.  See OtelSamplerHookPolicy in otel.h for
 * the four allowed values + their rationale.
 */
static OtelSamplerHookPolicy otel_sampler_hook_policy =
	OTEL_SAMPLER_HOOK_ON_UNSAMPLED_BIT;


/*
 * Registration functions exposed via OtelTracingApi.  They record
 * the previously-registered hook into *prev_out (if non-NULL) and
 * install the new one.  Not thread-safe; documented as _PG_init only.
 */
static void
api_register_emit_hook(otel_span_emit_hook_type new_hook,
					   otel_span_emit_hook_type *prev_out)
{
	if (prev_out)
		*prev_out = otel_span_emit_hook;
	otel_span_emit_hook = new_hook;
}

static void
api_register_sampler_hook(otel_sampler_hook_type new_hook,
						  otel_sampler_hook_type *prev_out)
{
	if (prev_out)
		*prev_out = otel_sampler_hook;
	otel_sampler_hook = new_hook;
}

static void
api_set_sampler_policy(OtelSamplerHookPolicy policy)
{
	otel_sampler_hook_policy = policy;
}


/* ----------------------------------------------------------------
 * Phase 4 helpers --- expose the bits the split-out query-tracing
 * module needs to reach back into contrib/otel.  All thin wrappers
 * around existing internal state / functions.
 * ----------------------------------------------------------------
 */

static void
api_get_root_context_snapshot(OtelRootContextSnapshot *out)
{
	if (out == NULL)
		return;

	out->is_set = otel_ctx.is_set;
	out->sampled_flag_set = otel_ctx.sampled_flag_set;
	out->from_comment = otel_ctx_from_comment;
	memcpy(out->trace_id, otel_ctx.trace_id, sizeof(out->trace_id));
	memcpy(out->span_id, otel_ctx.span_id, sizeof(out->span_id));
	memcpy(out->trace_flags, otel_ctx.trace_flags, sizeof(out->trace_flags));
	out->tracestate = (otel_tracestate_guc && otel_tracestate_guc[0])
		? otel_tracestate_guc : NULL;
}

static void
api_reset_root_context(void)
{
	otel_ctx_reset();
	otel_ctx_from_comment = false;
}

static bool
api_try_apply_sqlcommenter_context(const char *sql)
{
	/* Gated by the otel.parse_sqlcommenter GUC.  Returning false
	 * without parsing matches the behaviour of "no comment
	 * contained a traceparent" --- the caller can safely treat both
	 * cases identically. */
	if (!otel_parse_sqlcommenter)
		return false;
	return try_apply_sqlcommenter_context(sql);
}

/*
 * Apply the registered sampler hook + the configured invocation
 * policy and return a decision.  Caller has already done the
 * "any consumer present?" + "trace_all_queries?" gates --- this
 * function is invoked only when those gates passed.
 */
static OtelSamplerDecision
api_compute_sampler_decision(const OtelSamplerInput *in, bool sampled_flag_set)
{
	otel_sampler_hook_type sampler_hook = otel_sampler_hook;
	OtelSamplerHookPolicy policy = otel_sampler_hook_policy;

	switch (policy)
	{
		case OTEL_SAMPLER_HOOK_NEVER_ALWAYS_SAMPLE:
			return OTEL_SAMPLE_RECORD_AND_SAMPLE;

		case OTEL_SAMPLER_HOOK_NEVER_RESPECT_BIT:
			return sampled_flag_set
				? OTEL_SAMPLE_RECORD_AND_SAMPLE
				: OTEL_SAMPLE_DROP;

		case OTEL_SAMPLER_HOOK_ALWAYS:
			if (sampler_hook == NULL)
				return OTEL_SAMPLE_RECORD_AND_SAMPLE;
			break;					/* fall through to the hook call */

		case OTEL_SAMPLER_HOOK_ON_UNSAMPLED_BIT:
		default:
			if (sampled_flag_set)
				return OTEL_SAMPLE_RECORD_AND_SAMPLE;
			if (sampler_hook == NULL)
				return OTEL_SAMPLE_DROP;
			break;					/* fall through to the hook call */
	}

	/* Hook-call path. */
	return sampler_hook(in);
}

static bool
api_any_emit_consumer_present(void)
{
	return otel_span_emit_hook != NULL || otel_emit_spans_to_log;
}

/*
 * The api table installed into the rendezvous slot.  Static storage
 * duration means it lives forever and external consumers can cache
 * the pointer.
 */
static const OtelTracingApi otel_tracing_api = {
	.version = OTEL_TRACING_API_VERSION,
	.register_emit_hook = api_register_emit_hook,
	.register_sampler_hook = api_register_sampler_hook,
	.set_sampler_policy = api_set_sampler_policy,

	/* Producer-side API; implementations live in otel_producer.c. */
	.span_link_to_active_and_push = otel_producer_span_link_to_active_and_push,
	.span_set_parent_explicit = otel_producer_span_set_parent_explicit,
	.span_current_context = otel_producer_span_current_context,
	.span_root_context = otel_producer_span_root_context,
	.span_stack_depth = otel_producer_span_stack_depth,
	.span_emit = otel_producer_span_emit,

	/* Producer-side convenience helpers, routed through the
	 * rendezvous-struct so they remain reachable across the
	 * cross-extension symbol-resolution boundary. */
	.span_init = otel_span_init,
	.span_add_attribute_string = otel_span_add_attribute_string,

	/* Phase 4: surface needed by the split-out query-tracing
	 * module. */
	.span_push = otel_producer_span_push,
	.parallel_publish_leader_context = otel_parallel_publish_leader_context,
	.parallel_clear_leader_context = otel_parallel_clear_leader_context,
	.parallel_get_leader_context = otel_parallel_get_leader_context,
	.get_root_context_snapshot = api_get_root_context_snapshot,
	.reset_root_context = api_reset_root_context,
	.try_apply_sqlcommenter_context = api_try_apply_sqlcommenter_context,
	.compute_sampler_decision = api_compute_sampler_decision,
	.any_emit_consumer_present = api_any_emit_consumer_present,

	/* Resource attributes for the postmaster process + per-producer
	 * InstrumentationScope registration. */
	.get_resource_attributes = otel_resource_attrs_get,
	.tracer_register = otel_tracer_register,
};


/*
 * Publish the OtelTracingApi via a rendezvous variable so that
 * out-of-tree exporter / SDK modules can register callbacks without
 * taking a direct symbol-level link dependency on contrib/otel.
 * Called once from _PG_init.
 */
void
otel_api_publish_rendezvous(void)
{
	void	  **slot;

	slot = find_rendezvous_variable(OTEL_TRACING_API_RENDEZVOUS_NAME);
	Assert(slot != NULL);		/* HASH_ENTER ereports on OOM, never returns NULL */
	*slot = (void *) &otel_tracing_api;
}


/* ---- Internal getters used by otel_trace.c -------------------- */

otel_span_emit_hook_type
otel_get_span_emit_hook(void)
{
	return otel_span_emit_hook;
}

otel_sampler_hook_type
otel_get_sampler_hook(void)
{
	return otel_sampler_hook;
}

OtelSamplerHookPolicy
otel_get_sampler_hook_policy(void)
{
	return otel_sampler_hook_policy;
}
