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
 *	   OTEL_API_MAJOR(api->version) == OTEL_TRACING_API_MAJOR
 *	   api->struct_size >= sizeof(struct OtelTracingApi)
 *
 * Strict equality on MAJOR is intentional: an exporter built against
 * MAJOR=N has no way to know whether MAJOR=N+1 moved a function
 * pointer, changed a struct layout, or repurposed a field.  Force
 * the rebuild.
 *
 * struct_size guards against the consumer being newer than the
 * producer (consumer's compile-time sizeof is larger than what the
 * producer actually exposes) --- reading past the producer's end is
 * UB.  The other direction (producer newer than consumer) is fine:
 * the consumer reads the prefix it knows about and ignores any
 * appended fields.
 *
 * The MINOR halfword is informational only.  It used to be the
 * load-bearing additive-extension check, but struct_size is a
 * stronger guarantee (it survives header-drift bugs, LD_PRELOAD
 * mismatches, and the .so-vs-header skew cases where two parties
 * disagree on the struct layout without disagreeing on the version
 * number).
 *
 * Use OTEL_MAKE_VERSION(maj, min) to construct version literals.
 * Use OTEL_API_MAJOR(v) and OTEL_API_MINOR(v) to extract halfwords.
 */
/* Halfword split for the 32-bit version field.  These are object-
 * like macros (plain integer constants) rather than embedded
 * literals inside the function-like macros below so bindgen and
 * other FFI-binding generators that don't expand function-like
 * macros can still see the shift / mask values. */
#define OTEL_API_MAJOR_SHIFT		16
#define OTEL_API_MINOR_MASK			0xFFFFu

#define OTEL_MAKE_VERSION(maj, min)	(((uint32) (maj) << OTEL_API_MAJOR_SHIFT) | \
									 (uint16) (min))
#define OTEL_API_MAJOR(v)			((v) >> OTEL_API_MAJOR_SHIFT)
#define OTEL_API_MINOR(v)			((v) & OTEL_API_MINOR_MASK)

#define OTEL_TRACING_API_MAJOR		2
#define OTEL_TRACING_API_MINOR		2
#define OTEL_TRACING_API_VERSION	OTEL_MAKE_VERSION(OTEL_TRACING_API_MAJOR, \
													  OTEL_TRACING_API_MINOR)

/*
 * Rendezvous variable name (subject to NAMEDATALEN, currently 64).
 * The variable's value is a `OtelTracingApi *` installed by
 * contrib/otel's _PG_init.  External consumers retrieve it via
 * otel_api_get() (see below) rather than directly.
 *
 * A second rendezvous variable (OTEL_TRACING_API_PENDING_NAME) holds
 * the head of a linked list of OtelPendingRegistration nodes enqueued
 * by exporters/producers that load before otel_api.  The provider
 * drains this list when it publishes the API slot.
 */
#define OTEL_TRACING_API_RENDEZVOUS_NAME	"OtelTracingApi"
#define OTEL_TRACING_API_PENDING_NAME		"OtelTracingApiPending"

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
	 * Set to OTEL_TRACING_API_VERSION at module init.  Halfword-
	 * packed MAJOR/MINOR.  Today the MAJOR halfword must match
	 * strictly; the MINOR halfword is informational (the load-bearing
	 * compatibility check is struct_size below).
	 */
	uint32		version;

	/*
	 * sizeof(OtelTracingApi) at the producer's compile time.  Consumers
	 * compare this against their own compile-time sizeof to detect
	 * struct-layout mismatch independently of the version halfwords.
	 *
	 * The two fields together form the load-bearing compatibility
	 * check:
	 *
	 *	   OTEL_API_MAJOR(api->version) != OTEL_TRACING_API_MAJOR
	 *	       -> incompatible struct layout / repurposed fields.
	 *	   api->struct_size < sizeof(struct OtelTracingApi)
	 *	       -> producer is older than my header; reading past the
	 *	          producer's end is UB.
	 *	   otherwise -> safe to use up to sizeof(*api); any further
	 *	                fields the producer exposes are ignored.
	 *
	 * Placed immediately after version so its offset stays at a
	 * fixed, layout-independent position (4 bytes in) forever ---
	 * a consumer can read version + struct_size before relying on
	 * any compile-time knowledge of the rest of the struct.
	 *
	 * Pre-1.0 status note: until the API is declared stable (1.0+),
	 * we use struct_size for layout validation without bumping
	 * MAJOR on additive changes.  Consumers must rebuild whenever
	 * the struct grows; struct_size catches "you forgot to rebuild"
	 * at runtime.
	 */
	uint32		struct_size;

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

	/*
	 * --------------------------------------------------------------
	 * Producer-side API (added in Phase 1 of the contrib/otel split).
	 * --------------------------------------------------------------
	 *
	 * These entry points let any postgres extension that wants to
	 * emit OTel spans plug into contrib/otel's active-span stack
	 * and exporter dispatch without taking a hard build-time
	 * dependency on the OTel SDK.  Statement-level instrumentation,
	 * PL handlers, replication apply workers, custom SPI callers
	 * --- all use the same surface.
	 *
	 * --------------------------------------------------------------
	 * Push-time requirements
	 * --------------------------------------------------------------
	 *
	 * The push functions capture two things and remember them
	 * until span_emit or unwind: the OtelSpan pointer, and the
	 * active CurrentMemoryContext.
	 *
	 * The OtelSpan storage:
	 *
	 *   * OTEL_UNWIND_DROP (default): on-stack or anywhere else
	 *     is fine; the unwind path does not read the span.  The
	 *     stack entry stores NULL for the span pointer under
	 *     this policy, so even a stale dangling address is never
	 *     observed.
	 *   * OTEL_UNWIND_ERROR: must NOT be on the C stack.  Use a
	 *     static slab or palloc / malloc.  Storage must stay
	 *     valid until span_emit returns OR the unwind callback
	 *     finishes dispatching the span.
	 *
	 * The CurrentMemoryContext at push:
	 *
	 *   * Defines when the unwind safety net fires --- otel_api
	 *     registers a MemoryContextResetCallback against the
	 *     active CurrentMemoryContext at push time, and that
	 *     context's reset (e.g. on an ereport that unwinds
	 *     through it) is what triggers OTEL_UNWIND_*.
	 *   * Typically this is already the right per-statement /
	 *     per-function context you're in.  If you need a wider
	 *     or narrower scope, MemoryContextSwitchTo before
	 *     pushing; the binding is captured at push and later
	 *     switches don't move it.
	 *   * Do NOT push under TopMemoryContext, CacheMemoryContext,
	 *     or ErrorContext: those don't reset on ereport, so under
	 *     OTEL_UNWIND_ERROR the safety net never fires.  This is
	 *     enforced with a LOG-level server message at push time;
	 *     cassert builds Assert() on the misuse.
	 *
	 * After span_emit (success path) the storage can be freed and
	 * the binding is forgotten.  After ereport(ERROR) no cleanup
	 * is required: the callback pops the stack entry and applies
	 * the unwind_policy automatically.
	 *
	 * Three variants for starting a span:
	 *
	 *	  1. Implicit-fetch + push (the common case):
	 *	     api->span_link_to_active_and_push(&span)
	 *	       - parent identity from top-of-stack, or root context
	 *	         if stack empty, or none (root span on a new trace);
	 *	       - new span pushed onto active stack.
	 *
	 *	  2. Explicit parent, no push (the independent-trace case):
	 *	     api->span_set_parent_explicit(&span, &parent_ctx)
	 *	       - parent identity from caller-supplied SpanContext;
	 *	       - active stack untouched.  Used for traces that must
	 *	         not appear as children of the call-stack-based
	 *	         trace --- background apply work, etc.
	 *
	 *	  3. Neither: caller calls no link function.  fresh trace_id
	 *	     and span_id from otel_span_init (Commit D); parent
	 *	     stays zero.  Brand-new root span on its own trace.
	 *
	 * Inspection: api->span_current_context / span_root_context /
	 * span_stack_depth let consumers reason about the active trace
	 * without affecting it.
	 *
	 * Emit: api->span_emit dispatches the span to registered
	 * exporter hooks; pops it from the stack if pushed.
	 */
	void	  (*span_link_to_active_and_push) (OtelSpan *span);
	void	  (*span_set_parent_explicit) (OtelSpan *span,
										   const OtelSpanContext *parent);
	const OtelSpanContext *(*span_current_context) (void);
	const OtelSpanContext *(*span_root_context) (void);
	int		  (*span_stack_depth) (void);
	void	  (*span_emit) (OtelSpan *span);

	/*
	 * Producer-side convenience helpers exposed via the rendezvous
	 * struct (not as extern functions) so they remain reachable from
	 * consumer modules across the cross-extension symbol-resolution
	 * boundary on every supported platform.
	 *
	 *	   api->span_init(&span, scope, "operation.name", KIND);
	 *	     Generates fresh span_id, sets start_time = now, scope,
	 *	     name, kind; zeroes other fields including unwind_policy =
	 *	     OTEL_UNWIND_DROP and sampler_decision =
	 *	     RECORD_AND_SAMPLE.  `scope` must be a handle previously
	 *	     returned by api->tracer_register (see below); NULL is
	 *	     accepted only for compatibility with old producers and
	 *	     causes exporters to fall back to a Resource-derived
	 *	     default scope.
	 *
	 *	   api->span_add_attribute_string(&span, "key", "value");
	 *	     Appends to the inline attrs[] array if room, else
	 *	     allocates / repalloc's the overflow array via
	 *	     MCXT_ALLOC_NO_OOM (silent drop on OOM).  Returns true
	 *	     on success, false on overflow allocation failure.
	 *	     Neither key nor value is copied --- caller must keep
	 *	     them alive until api->span_emit returns.
	 */
	void	  (*span_init) (OtelSpan *span,
							const OtelInstrumentationScope *scope,
						    const char *name,
						    OtelSpanKind kind);
	bool	  (*span_add_attribute_string) (OtelSpan *span,
											const char *key,
											const char *value);

	/*
	 * Attach a string attribute to the top-of-stack span without requiring
	 * a pointer to it.  Intended for contrib modules (e.g. auto_explain)
	 * that hook ExecutorEnd and want to enrich the active statement span.
	 *
	 * Returns true on success; false when no suitable active span is present
	 * (empty stack, or the top entry is OTEL_UNWIND_DROP and stores no
	 * pointer).
	 *
	 * The key and value pointers must remain valid until the active span is
	 * emitted --- the same lifetime requirement as span_add_attribute_string.
	 */
	bool	  (*span_add_attribute_string_to_active) (const char *key,
													  const char *value);

	/*
	 * --------------------------------------------------------------
	 * Phase 4: surface needed by the split-out query-tracing module
	 * (contrib/otel_postgres_tracing).  Other consumers won't
	 * normally call these directly --- they're internal-style
	 * helpers that must cross the module boundary because the
	 * query-tracing module pre-populates parent fields itself
	 * (parallel-worker leader override) and needs to read the
	 * root-context snapshot, dispatch the sampler, etc.
	 * --------------------------------------------------------------
	 */

	/* Push a span onto the active stack WITHOUT fetching a parent.
	 * The caller is responsible for populating trace_id /
	 * parent_span_id / trace_flags themselves.  External
	 * consumers that just want "nest under whatever is active"
	 * should use span_link_to_active_and_push instead. */
	void	  (*span_push) (OtelSpan *span);

	/* Parallel-worker leader context publishing / lookup.  See
	 * OtelParallelContext above. */
	void	  (*parallel_publish_leader_context) (const char *trace_id,
												  const char *span_id,
												  const char *trace_flags);
	void	  (*parallel_clear_leader_context) (void);
	bool	  (*parallel_get_leader_context) (OtelParallelContext *out);

	/* Root-context (client-supplied trace context) inspection +
	 * lifecycle.  See OtelRootContextSnapshot above.  Fills *out
	 * with the current backend's root context.  reset clears it
	 * (used by the statement-tracing module after consuming a
	 * sqlcommenter-derived context). */
	void	  (*get_root_context_snapshot) (OtelRootContextSnapshot *out);
	void	  (*reset_root_context) (void);

	/* Sqlcommenter parsing.  Parses trace-context out of a SQL
	 * comment in `sql` and applies it to the backend root context.
	 * Returns true iff a traceparent was found and applied. */
	bool	  (*try_apply_sqlcommenter_context) (const char *sql);

	/* Sampler dispatch.  Consumer fills `in` with the propagated
	 * context bits + a name hint; this function applies the
	 * registered sampler hook + policy and returns the decision.
	 * Note: this does NOT do the "no exporter registered" or
	 * "trace_all_queries" early-outs --- those are decisions for
	 * the query-tracing module to make based on its own GUCs.
	 * `sampled_flag_set` is the W3C `sampled=1` wire bit. */
	OtelSamplerDecision (*compute_sampler_decision) (const OtelSamplerInput *in,
													  bool sampled_flag_set);

	/* True iff a consumer has registered an emit hook OR the
	 * built-in JSON-log emission is enabled.  The query-tracing
	 * module uses this for the "no consumer -> drop" early-out. */
	bool	  (*any_emit_consumer_present) (void);

	/* --------------------------------------------------------------
	 * Resource + InstrumentationScope identity.
	 *
	 * get_resource_attributes returns a pointer to a process-local
	 * array populated at _PG_init and writes the count into *n_out.
	 * Resource describes the postmaster process; exporters apply it
	 * to every span batch / metric stream they emit so the downstream
	 * collector can group telemetry by its emitting process.
	 * Consumers that want richer Resource (host.arch, os.type, ...)
	 * merge their own attributes on top.  See OtelResourceAttribute
	 * in otel.h.
	 *
	 * tracer_register is the producer-side InstrumentationScope
	 * constructor.  Each producer extension calls it once from
	 * _PG_init, caches the returned handle module-statically, and
	 * passes the handle as the `scope` argument to every
	 * api->span_init call.  Returns a pointer owned by contrib/otel;
	 * valid for the backend's lifetime.  See OtelInstrumentationScope
	 * in otel.h.  name must be non-empty; version and schema_url may
	 * be NULL.
	 * -------------------------------------------------------------- */
	const OtelResourceAttribute *(*get_resource_attributes) (int *n_out);

	OtelInstrumentationScope *(*tracer_register) (const char *name,
												  const char *version,
												  const char *schema_url);
} OtelTracingApi;


/* -----------------------------------------------------------------------
 * Order-independent rendezvous helpers (T8)
 * -----------------------------------------------------------------------
 *
 * These are header-only so every consumer TU gets them without adding
 * link-time dependencies on otel_api's internal symbols.
 *
 * otel_api_get() — static-first lazy getter
 * -----------------------------------------
 * Returns the cached OtelTracingApi pointer, or NULL when the provider
 * is absent or incompatible.  Safe to call at any time after _PG_init
 * completes (i.e., on any hot path).
 *
 * Design:
 *   - First call (cache == NULL) walks the rendezvous variable, validates
 *     the version + struct_size, and caches the result — either the real
 *     pointer or the OTEL_API_MISSING sentinel.
 *   - Subsequent calls cost one pointer-compare (the branch predictor
 *     learns the common value almost immediately).
 *   - Correct because all _PG_init callbacks finish before the first
 *     query executes; by the time any hot-path code calls this function
 *     every preloaded extension has already published its rendezvous slot.
 *   - A function-local static avoids the file-scope unused-variable
 *     warning in TUs that include the header but never call the getter.
 *
 * OTEL_API_MISSING sentinel
 * -------------------------
 * A non-NULL sentinel stored in the per-TU cache when the provider is
 * absent or incompatible.  Subsequent calls short-circuit and return NULL
 * without touching the rendezvous table again.  The macro is a cast
 * expression (no storage) so it never triggers an unused-variable warning.
 *
 * OtelPendingRegistration / otel_api_register_when_ready()
 * ---------------------------------------------------------
 * Exporters call otel_api_register_when_ready() from _PG_init instead
 * of reaching directly into find_rendezvous_variable.  If the provider
 * is already present, registration happens immediately; otherwise the
 * request is pushed onto a pending list stored in the
 * OTEL_TRACING_API_PENDING_NAME rendezvous slot.  The provider drains
 * that list when it publishes its own slot in otel_api_publish_rendezvous().
 *
 * This supports both orderings:
 *   Provider first  → immediate registration on the exporter's _PG_init.
 *   Exporter first  → deferred registration, drained at provider _PG_init.
 * ----------------------------------------------------------------------- */

/*
 * Sentinel for "looked up once; provider absent or incompatible."
 * Cast to const OtelTracingApi * so it is assignment-compatible with
 * the cache variable, but it is never a real pointer.
 */
#define OTEL_API_MISSING  ((const OtelTracingApi *) (uintptr_t) -1)

/*
 * otel_api_get() — header-only lazy getter.
 *
 * Returns a valid OtelTracingApi pointer, or NULL when the provider is
 * absent or incompatible.  A WARNING is emitted once (per TU's cache
 * slot) on the incompatible case; the absent case is silent.
 *
 * Callers MUST include "fmgr.h" (declares find_rendezvous_variable) and
 * "utils/elog.h" (declares ereport / errmsg / errdetail) before this
 * header, OR simply include "postgres.h" which pulls both transitively.
 * In practice every .c file that uses PostgreSQL APIs already includes
 * "postgres.h", so no additional include is needed.
 */
static inline const OtelTracingApi *
otel_api_get(void)
{
	/*
	 * Per-TU static cache.  NULL = not yet resolved; OTEL_API_MISSING =
	 * resolved and absent/incompatible; anything else = the real pointer.
	 *
	 * Function-local rather than file-scope to suppress -Wunused-variable
	 * in TUs that include the header but never call this function.
	 */
	static const OtelTracingApi *cache = NULL;

	if (cache != NULL)
		return (cache == OTEL_API_MISSING) ? NULL : cache;

	{
		void	  **slot = find_rendezvous_variable(OTEL_TRACING_API_RENDEZVOUS_NAME);
		const OtelTracingApi *api = slot ? (const OtelTracingApi *) *slot : NULL;

		if (api == NULL)
		{
			/* Provider absent — silent no-op; cache sentinel. */
			cache = OTEL_API_MISSING;
			return NULL;
		}
		if (OTEL_API_MAJOR(api->version) != OTEL_TRACING_API_MAJOR ||
			api->struct_size < sizeof(OtelTracingApi))
		{
			/* Present but incompatible — warn ONCE, then no-op. */
			ereport(WARNING,
					(errmsg("OtelTracingApi compatibility check failed"),
					 errdetail("Loaded otel_api exposes api version %u.%u"
							   " (struct_size %u);"
							   " this module was built against version %u.%u"
							   " (struct_size %zu).",
							   OTEL_API_MAJOR(api->version),
							   OTEL_API_MINOR(api->version),
							   api->struct_size,
							   OTEL_TRACING_API_MAJOR,
							   OTEL_TRACING_API_MINOR,
							   sizeof(OtelTracingApi))));
			cache = OTEL_API_MISSING;
			return NULL;
		}
		cache = api;
		return cache;
	}
}


/*
 * Pending-registration node.  Exporters (and producers, if they want to
 * use this path) allocate one in TopMemoryContext and push it onto the
 * pending list before the provider drains it.
 *
 * Only the fields relevant to each caller need to be non-NULL.  The
 * provider calls the non-NULL ones in push order.
 */
typedef struct OtelPendingRegistration
{
	/* Emit hook to register, or NULL. */
	otel_span_emit_hook_type emit_hook;
	otel_span_emit_hook_type *emit_prev_out;

	/* Sampler hook to register, or NULL. */
	otel_sampler_hook_type sampler_hook;
	otel_sampler_hook_type *sampler_prev_out;

	/* tracer_register() arguments, or NULL name to skip. */
	const char *tracer_name;
	const char *tracer_version;
	const char *tracer_schema_url;
	/* Output slot for the returned tracer handle; NULL to discard. */
	const OtelInstrumentationScope **tracer_out;

	struct OtelPendingRegistration *next;
} OtelPendingRegistration;


/*
 * otel_api_register_when_ready() — order-independent registration helper.
 *
 * If the provider is already present (otel_api_get() non-NULL), register
 * immediately via the real API entry points.  Otherwise push the request
 * onto the pending list; the provider drains it when it publishes its
 * rendezvous slot.
 *
 * *req must have process/static lifetime: either a file-static node (BSS)
 * or one allocated in TopMemoryContext.  Both are correct because the
 * provider may drain the pending list long after the caller's _PG_init
 * stack frame has returned; the node must remain valid until then.
 * All pointer fields in *req that the caller does not use must be NULL.
 */
static inline void
otel_api_register_when_ready(OtelPendingRegistration *req)
{
	const OtelTracingApi *api = otel_api_get();

	if (api != NULL)
	{
		/* Provider already present: register immediately. */
		if (req->emit_hook)
			api->register_emit_hook(req->emit_hook, req->emit_prev_out);
		if (req->sampler_hook)
			api->register_sampler_hook(req->sampler_hook, req->sampler_prev_out);
		if (req->tracer_name)
		{
			const OtelInstrumentationScope *scope =
				api->tracer_register(req->tracer_name,
									 req->tracer_version,
									 req->tracer_schema_url);

			if (req->tracer_out)
				*req->tracer_out = scope;
		}
		return;
	}

	/*
	 * Provider not yet present: push onto the pending list stored in the
	 * second rendezvous variable.  The provider drains it at its own
	 * _PG_init (otel_api_publish_rendezvous).
	 */
	{
		void	  **pending_slot =
			find_rendezvous_variable(OTEL_TRACING_API_PENDING_NAME);

		/* find_rendezvous_variable never returns NULL (OOM = ereport). */
		req->next = (OtelPendingRegistration *) *pending_slot;
		*pending_slot = (void *) req;
	}
}

#endif							/* CONTRIB_OTEL_API_H */
