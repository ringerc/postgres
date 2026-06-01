/*-------------------------------------------------------------------------
 *
 * otel_api.h
 *	  Extension registration API for contrib/otel.
 *
 * Out-of-tree exporter / SDK modules look up the OtelTracingApi
 * struct (defined here) at _PG_init time via the rendezvous variable
 * named OTEL_TRACING_API_RENDEZVOUS_NAME, then call its registration
 * functions to install a span emit hook and sampler hook.
 *
 * The data model that a span emit hook receives lives in the
 * companion header `otel.h`; this header pulls it in for you, so
 * `#include <otel/otel_api.h>` alone is sufficient for an exporter.
 *
 * The umbrella header `otel.h` also re-includes this file, so legacy
 * consumers that include only `<otel/otel.h>` continue to compile
 * unchanged.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/otel/otel_api.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONTRIB_OTEL_API_H
#define CONTRIB_OTEL_API_H

#include "otel.h"

/*
 * Version of the OtelTracingApi struct exposed via the rendezvous
 * variable named OTEL_TRACING_API_RENDEZVOUS_NAME.
 *
 * Versioning rules (split major/minor in a single uint32):
 *
 *	 The 32-bit version is split into two halfwords:
 *
 *	   * High halfword (bits 31..16) -- MAJOR version, bumped on any
 *	     incompatible layout change: a removed, retyped, reordered,
 *	     or semantically-repurposed field.  Strict-equality required.
 *	   * Low halfword (bits 15..0) -- MINOR version, a monotonic
 *	     extension counter.  Bumped on each additive change (new
 *	     field or function pointer APPENDED at the END of the
 *	     struct).  The invariant is "it must be safe to interpret a
 *	     (MAJOR, MINOR+k) struct as a (MAJOR, MINOR) struct" -- the
 *	     layout prefix up to MINOR is identical; only suffix fields
 *	     are added.
 *	   * Bug fixes that do not change the ABI do not bump anything.
 *
 * External modules MUST verify both:
 *
 *	   OTEL_API_MAJOR(api->version) == OTEL_TRACING_API_MAJOR   // strict
 *	   OTEL_API_MINOR(api->version) >= OTEL_TRACING_API_MINOR   // >=
 *
 * Strict equality on MAJOR is intentional: an exporter built against
 * MAJOR=N has no way to know whether MAJOR=N+1 moved a function
 * pointer, changed a struct layout, or repurposed a field.  Force
 * the rebuild.
 *
 * MINOR is asymmetric: a producer at (M, N+k) is fine for a consumer
 * built at (M, N) because additive changes only add fields after the
 * prefix the consumer reads.  The other direction (consumer minor >
 * producer minor) is not safe -- the consumer would read past the
 * end of the producer's struct, hence the >= check.
 *
 * Use OTEL_MAKE_VERSION(maj, min) to construct version literals.
 * Use OTEL_API_MAJOR(v) and OTEL_API_MINOR(v) to extract halfwords.
 */
#define OTEL_MAKE_VERSION(maj, min)	(((uint32) (maj) << 16) | (uint16) (min))
#define OTEL_API_MAJOR(v)			((v) >> 16)
#define OTEL_API_MINOR(v)			((v) & 0xFFFFu)

#define OTEL_TRACING_API_MAJOR		2
#define OTEL_TRACING_API_MINOR		0
#define OTEL_TRACING_API_VERSION	OTEL_MAKE_VERSION(OTEL_TRACING_API_MAJOR, \
													  OTEL_TRACING_API_MINOR)

/*
 * Rendezvous variable name (subject to NAMEDATALEN, currently 64).
 * The variable's value is a `OtelTracingApi *` installed by
 * contrib/otel's _PG_init.  External consumers retrieve it via
 *
 *	 void **slot = find_rendezvous_variable(OTEL_TRACING_API_RENDEZVOUS_NAME);
 *	 const OtelTracingApi *api = (const OtelTracingApi *) *slot;
 *
 * The slot is NULL until contrib/otel has been preloaded.  An
 * exporter loaded WITHOUT contrib/otel in shared_preload_libraries
 * MUST ereport(ERROR) on a NULL api pointer and tell the user to
 * add 'otel' before this module in the preload list.
 */
#define OTEL_TRACING_API_RENDEZVOUS_NAME	"OtelTracingApi"

/*
 * The api table itself.  All function pointers are populated by
 * contrib/otel and never become NULL during a backend's lifetime.
 *
 * Registration functions are NOT thread-safe and MUST be called
 * from _PG_init, before any backend has begun executing queries.
 * They install hooks process-wide for the backend.
 */
typedef struct OtelTracingApi
{
	/*
	 * Set to OTEL_TRACING_API_VERSION at module init.  External
	 * consumers must verify both halfwords match what they were
	 * compiled against (strict on MAJOR, >= on MINOR); see the
	 * comment on OTEL_TRACING_API_VERSION.
	 */
	uint32		version;

	/*
	 * Register a span emit callback.  If prev_out is non-NULL, the
	 * previously-registered hook (or NULL if first) is written there.
	 * The new hook is responsible for forwarding to *prev_out after
	 * doing its own work, to allow multiple consumers to chain:
	 *
	 *	 static otel_span_emit_hook_type prev_emit;
	 *
	 *	 static void my_emit(const OtelSpan *s) {
	 *	   ... do work ...
	 *	   if (prev_emit) prev_emit(s);
	 *	 }
	 *
	 *	 void _PG_init(void) {
	 *	   void **slot = find_rendezvous_variable(OTEL_TRACING_API_RENDEZVOUS_NAME);
	 *	   const OtelTracingApi *api = *slot;
	 *	   ... check api != NULL, OTEL_API_MAJOR(api->version) ==
	 *	   OTEL_TRACING_API_MAJOR, OTEL_API_MINOR(api->version) >=
	 *	   OTEL_TRACING_API_MINOR ...
	 *	   api->register_emit_hook(my_emit, &prev_emit);
	 *	 }
	 *
	 * Pass NULL as new_hook to detach (rare; mostly useful for tests).
	 */
	void	  (*register_emit_hook) (otel_span_emit_hook_type new_hook,
									 otel_span_emit_hook_type *prev_out);

	/*
	 * Register a sampler hook.  Semantics mirror register_emit_hook.
	 * See the comment on otel_sampler_hook_type for what the hook
	 * is expected to do and when it is called.
	 *
	 * When the hook is called depends on the policy set via
	 * set_sampler_policy below; the default is to call the hook only
	 * when the propagated sampled bit is unset.
	 */
	void	  (*register_sampler_hook) (otel_sampler_hook_type new_hook,
										otel_sampler_hook_type *prev_out);

	/*
	 * Configure the sampler-hook invocation policy.  See
	 * OtelSamplerHookPolicy for the enum and rationale.  Default is
	 * OTEL_SAMPLER_HOOK_ON_UNSAMPLED_BIT (W3C ParentBased compliance).
	 *
	 * Typically called once at _PG_init time.  Subsequent calls are
	 * permitted but have no defined synchronization with in-flight
	 * queries.
	 */
	void	  (*set_sampler_policy) (OtelSamplerHookPolicy policy);
} OtelTracingApi;

#endif							/* CONTRIB_OTEL_API_H */
