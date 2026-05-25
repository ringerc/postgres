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
 * Versioning rules (semver-ish, single integer):
 *
 *	 * The major number is bumped on any breaking change: a removed
 *	   or repurposed function pointer, a renamed/restructured field
 *	   in an input or output type that affects ABI.
 *	 * Appending new function pointers at the END of OtelTracingApi
 *	   is NOT a breaking change.  Old consumers do not dereference
 *	   past where they expect data and will continue to work.
 *	 * Bug fixes that do not change the ABI do not bump the version.
 *
 * External modules MUST check `api->version == OTEL_TRACING_API_VERSION`
 * (exact match, not >=) at registration time, and ereport(ERROR) on
 * mismatch.  This is intentionally stricter than "compatible with"
 * because an exporter built against v1 has no way to know whether a
 * later major's struct layout broke its assumptions.
 */
#define OTEL_TRACING_API_VERSION		1

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
	 * consumers must verify this matches what they were compiled
	 * against; see the comment on OTEL_TRACING_API_VERSION.
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
	 *	   ... check api != NULL, api->version == OTEL_TRACING_API_VERSION ...
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
	 */
	void	  (*register_sampler_hook) (otel_sampler_hook_type new_hook,
										otel_sampler_hook_type *prev_out);
} OtelTracingApi;

#endif							/* CONTRIB_OTEL_API_H */
