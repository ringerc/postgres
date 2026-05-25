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
