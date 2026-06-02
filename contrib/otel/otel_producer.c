/*-------------------------------------------------------------------------
 *
 * otel_producer.c
 *	  Producer-side API for contrib/otel: the active-span stack,
 *	  parent-context management, and the span_emit dispatch entry point
 *	  used by both contrib/otel's own query-tracing hooks and any
 *	  external consumer (PGD, PGAA, PL handlers, etc.) that wants to
 *	  emit spans via the OtelTracingApi rendezvous interface.
 *
 * Phase 1 of the contrib/otel split (see contrib-otel-split.md in
 * the parent workspace): this file owns the producer API surface
 * that will, in Phase 4, become the boundary between the API
 * module (this file stays in contrib/otel) and the collector module
 * (contrib/otel_postgres_tracing, which will consume this API for
 * statement-span construction).
 *
 * State model
 * -----------
 *	  * Root context: per-backend (trace_id, root_span_id, trace_flags,
 *	    tracestate), set when the client supplies trace context via the
 *	    'M' protocol header or via the otel.traceparent GUC.  The legacy
 *	    `OtelContext otel_ctx` in otel.c is the canonical storage; this
 *	    file reads it via the existing assign-hook-populated state.
 *
 *	  * Active stack: bounded array of OtelSpanStackEntry, one per
 *	    currently-open span pushed by a consumer.  All entries share
 *	    one trace_id by construction (push variants only chain to the
 *	    existing top); explicit-parent variants do not touch the stack.
 *
 * Lifecycle of a pushed span
 * --------------------------
 *	  1. Consumer allocates OtelSpan in its own MemoryContext (typically
 *	     a per-statement context, or a static slab).
 *	  2. Consumer calls otel_span_init() (inline, in Commit D) or fills
 *	     fields directly.
 *	  3. Consumer calls api->span_link_to_active_and_push(span):
 *	      - parent identity fetched from top-of-stack, or root context
 *	        if stack empty, or stays zero if neither set;
 *	      - new entry pushed at top of span_stack;
 *	      - unwind_policy captured into the stack entry at push time.
 *	  4. Consumer does work, sets attributes, etc.
 *	  5. Consumer calls api->span_emit(span):
 *	      - dispatch to registered emit hook + JSON-log emitter;
 *	      - if span is at top of stack, pop;
 *	      - if span is on the stack but not at top, WARNING and pop
 *	        down to it (entries above pop as well; their unwind_policy
 *	        decides whether they emit-as-ERROR or silently drop).
 *
 * Phase 1 (this commit) scope
 * ---------------------------
 *	  * Active stack + push/inspect/emit machinery.
 *	  * Producer-API function pointers in OtelTracingApi.
 *	  * Root context read via the existing OtelContext.
 *
 * Phase 1 deferred to subsequent commits
 * --------------------------------------
 *	  * MemoryContextCallback-driven cleanup on ereport unwind
 *	    (Commit C).  For now, if the consumer's allocation is freed
 *	    without an emit, the stack retains a stale entry until the
 *	    next push reaches it.  This is benign in the current usage
 *	    (existing query-tracing path doesn't push yet) but must be
 *	    fixed before external consumers rely on it.
 *	  * Stack-overflow telemetry (Commit C).  This commit silently
 *	    declines to push past MAX_SPAN_STACK_DEPTH; Commit C adds
 *	    WARNING + counter.
 *	  * Inline helpers in otel.h (Commit D).
 *	  * TAP coverage (Commit E).
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/otel/otel_producer.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "miscadmin.h"
#include "utils/elog.h"
#include "utils/timestamp.h"

#include "otel.h"
#include "otel_internal.h"


/*
 * Maximum depth of the active-span stack.  Hard-coded for now; promoted
 * to a GUC if/when real workloads need tuning.  At ~88 bytes per entry
 * (OtelSpanStackEntry size below), 64 deep is ~5.5 KB of per-backend
 * static memory --- negligible.
 *
 * Beyond this depth, new pushes still link parent_span_id to the
 * current top-of-stack for correctness, but are not themselves
 * pushed.  See api_span_link_to_active_and_push.
 */
#define MAX_SPAN_STACK_DEPTH	64


/*
 * One entry on the active-span stack.  Inline IDs make
 * span_current_context() cache-friendly --- no pointer chase through
 * the consumer's OtelSpan on the hot path.  The pointer to the
 * consumer's OtelSpan is used only at unwind time when this entry's
 * unwind_policy is OTEL_UNWIND_ERROR; for OTEL_UNWIND_DROP entries
 * the pointer is never dereferenced after push, so its post-push
 * validity is not required.
 */
typedef struct OtelSpanStackEntry
{
	/* Identity, inline for fast inspection.  No tracestate here: it
	 * lives in the shared otel_tracestate_guc and is constant across
	 * the lifetime of a trace within a backend. */
	char		span_id[OTEL_SPAN_ID_LEN + 1];
	char		trace_flags[OTEL_TRACE_FLAGS_LEN + 1];

	/* Unwind policy captured at push time --- changes to the
	 * underlying OtelSpan's policy after push do not affect this
	 * stack entry. */
	OtelSpanUnwindPolicy unwind_policy;

	/* Borrowed pointer to the consumer's OtelSpan.  Read only at
	 * unwind time for OTEL_UNWIND_ERROR entries.  Memory ownership
	 * stays with the consumer. */
	OtelSpan   *span;
} OtelSpanStackEntry;


/*
 * Per-backend storage for the active-span stack.  Static, zero-
 * initialised at backend start.  Single-threaded by construction
 * (each backend has its own copy), no locking required.
 */
static OtelSpanStackEntry span_stack[MAX_SPAN_STACK_DEPTH];
static int	span_stack_top = -1;	/* index of topmost entry; -1 == empty */


/*
 * Backend-local storage backing the OtelSpanContext * returned by
 * api->span_current_context() and api->span_root_context().  The
 * docs guarantee the returned pointer is valid until the next call
 * that may modify the active stack or root context, which is
 * trivially satisfied by single-threaded per-backend access plus
 * "never reuse the buffer until something changes" --- which we
 * implement by simply having one buffer per call site.
 */
static OtelSpanContext current_ctx_buf;
static OtelSpanContext root_ctx_buf;


/*
 * Helper: dispatch a span to the registered emit hook + the
 * built-in JSON-log emitter.  Both code paths (the existing
 * finalize_span in otel_trace.c, and the new api->span_emit
 * below) need this; for now we duplicate the small block in
 * the two sites rather than refactoring, since Commit B's goal
 * is purely additive.
 *
 * The PG_TRY/PG_CATCH wrapper ensures an exporter that ereports
 * doesn't disrupt the producer.  Tracing failures must not break
 * the query.
 */
static void
dispatch_span(const OtelSpan *span)
{
	otel_span_emit_hook_type emit_hook = otel_get_span_emit_hook();

	if (emit_hook == NULL && !otel_emit_spans_to_log)
		return;

	PG_TRY();
	{
		if (emit_hook)
			emit_hook(span);
		/* The internal JSON-log emitter is declared in otel_internal.h
		 * once we factor it out in Commit C; for now it lives in
		 * otel_trace.c and is reachable only via finalize_span().
		 * External consumers that want the log-line emission can set
		 * otel.emit_spans_to_log and register a no-op hook.  This is
		 * a known gap, addressed in Commit C. */
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
}


/* ====================================================================
 * Producer API functions exposed via OtelTracingApi function pointers.
 * Bound into the api struct in otel_api.c.
 * ==================================================================== */

/*
 * api_span_link_to_active_and_push --- the common-case "start a new
 * nested span" entry point.  Sets span->trace_id and
 * span->parent_span_id from the current top-of-stack (or root
 * context if the stack is empty), then pushes the new span onto
 * the active stack.
 *
 * If neither the stack nor the root context is set, the new span
 * starts a brand-new trace: span->trace_id stays whatever the
 * caller pre-populated (typically a freshly-generated one from
 * otel_span_init), span->parent_span_id stays zeroed.  The new
 * span IS still pushed in that case --- it becomes the root of
 * the active call-stack-based trace for this backend.
 *
 * If the stack is already at MAX_SPAN_STACK_DEPTH, parent linkage
 * is still computed correctly (preserving trace topology) but the
 * span is NOT pushed.  Subsequent pushes will all share the same
 * deepest-pushed parent --- approximately right since they share
 * the same logical scope.  Commit C adds WARNING + counter for
 * observable overflow.
 */
void
otel_producer_span_link_to_active_and_push(OtelSpan *span)
{
	if (span == NULL)
		return;

	/* Fetch parent: top-of-stack > root context > none. */
	if (span_stack_top >= 0)
	{
		const OtelSpanStackEntry *top = &span_stack[span_stack_top];

		/* trace_id is shared across the stack by construction; read
		 * it from the root context if set (or leave caller's
		 * pre-populated value alone if not). */
		if (otel_ctx.is_set)
			memcpy(span->trace_id, otel_ctx.trace_id, sizeof(span->trace_id));
		memcpy(span->parent_span_id, top->span_id, sizeof(span->parent_span_id));
		memcpy(span->trace_flags, top->trace_flags, sizeof(span->trace_flags));
	}
	else if (otel_ctx.is_set)
	{
		memcpy(span->trace_id, otel_ctx.trace_id, sizeof(span->trace_id));
		memcpy(span->parent_span_id, otel_ctx.span_id, sizeof(span->parent_span_id));
		memcpy(span->trace_flags, otel_ctx.trace_flags, sizeof(span->trace_flags));
	}
	/* else: root span of a brand-new trace; caller's pre-populated
	 * trace_id/span_id are used as-is, parent_span_id stays zero. */

	/* Push onto stack if there's room. */
	if (span_stack_top + 1 < MAX_SPAN_STACK_DEPTH)
	{
		OtelSpanStackEntry *entry;

		span_stack_top++;
		entry = &span_stack[span_stack_top];
		memcpy(entry->span_id, span->span_id, sizeof(entry->span_id));
		memcpy(entry->trace_flags, span->trace_flags, sizeof(entry->trace_flags));
		entry->unwind_policy = span->unwind_policy;
		entry->span = span;
	}
	/* else overflow: see comment above; Commit C adds WARNING +
	 * counter.  Parent linkage was set above so traces stay
	 * connected even when nesting exceeds the stack bound. */
}

/*
 * api_span_set_parent_explicit --- caller provides parent
 * SpanContext directly; stack is untouched.  Used for spans that
 * belong to a trace maintained independently of the active
 * call-stack-based trace (background work, sibling spans, etc.).
 *
 * If `parent` is NULL, span identity stays as the caller
 * pre-populated it.
 */
void
otel_producer_span_set_parent_explicit(OtelSpan *span, const OtelSpanContext *parent)
{
	if (span == NULL || parent == NULL)
		return;

	memcpy(span->trace_id, parent->trace_id, sizeof(span->trace_id));
	memcpy(span->parent_span_id, parent->span_id, sizeof(span->parent_span_id));
	memcpy(span->trace_flags, parent->trace_flags, sizeof(span->trace_flags));
	/* tracestate is read from the otel_tracestate_guc at emit time
	 * --- not stored per-span.  Callers that need a span-specific
	 * tracestate divergence should set the GUC before emit. */
}

/*
 * api_span_current_context --- return SpanContext of the topmost
 * stack entry, or of the root context if the stack is empty, or
 * NULL if neither is set.  The returned pointer is valid until
 * the next API call that may modify the stack or root context.
 */
const OtelSpanContext *
otel_producer_span_current_context(void)
{
	if (span_stack_top >= 0)
	{
		const OtelSpanStackEntry *top = &span_stack[span_stack_top];

		/* trace_id is shared across the stack; read from root context
		 * if set, else fall back to zeros (which signals "no trace"
		 * but should be impossible if anything is on the stack). */
		if (otel_ctx.is_set)
			memcpy(current_ctx_buf.trace_id, otel_ctx.trace_id, sizeof(current_ctx_buf.trace_id));
		else
			memset(current_ctx_buf.trace_id, 0, sizeof(current_ctx_buf.trace_id));
		memcpy(current_ctx_buf.span_id, top->span_id, sizeof(current_ctx_buf.span_id));
		memcpy(current_ctx_buf.trace_flags, top->trace_flags, sizeof(current_ctx_buf.trace_flags));
		current_ctx_buf.tracestate = otel_tracestate_guc;
		return &current_ctx_buf;
	}
	else if (otel_ctx.is_set)
	{
		memcpy(root_ctx_buf.trace_id, otel_ctx.trace_id, sizeof(root_ctx_buf.trace_id));
		memcpy(root_ctx_buf.span_id, otel_ctx.span_id, sizeof(root_ctx_buf.span_id));
		memcpy(root_ctx_buf.trace_flags, otel_ctx.trace_flags, sizeof(root_ctx_buf.trace_flags));
		root_ctx_buf.tracestate = otel_tracestate_guc;
		return &root_ctx_buf;
	}
	return NULL;
}

/*
 * api_span_root_context --- return the client-supplied root
 * SpanContext directly, bypassing the active stack.  For consumers
 * that want to start a sibling of the root operation rather than
 * a child of the current nested span.  Returns NULL if no root
 * context is set.
 */
const OtelSpanContext *
otel_producer_span_root_context(void)
{
	if (!otel_ctx.is_set)
		return NULL;

	memcpy(root_ctx_buf.trace_id, otel_ctx.trace_id, sizeof(root_ctx_buf.trace_id));
	memcpy(root_ctx_buf.span_id, otel_ctx.span_id, sizeof(root_ctx_buf.span_id));
	memcpy(root_ctx_buf.trace_flags, otel_ctx.trace_flags, sizeof(root_ctx_buf.trace_flags));
	root_ctx_buf.tracestate = otel_tracestate_guc;
	return &root_ctx_buf;
}

/*
 * api_span_stack_depth --- number of entries currently on the
 * active stack.  Useful for recursion guards, conditional
 * instrumentation, and tests.
 */
int
otel_producer_span_stack_depth(void)
{
	return span_stack_top + 1;
}

/*
 * api_span_emit --- finalize and dispatch a span.  If the span is
 * on the active stack, pop it (and entries above, per the
 * out-of-order-emit semantics described in the file header).
 *
 * For Commit B the out-of-order case is handled simply: a WARNING
 * is logged, popped entries above the target are silently
 * dropped, and the explicitly-emitted span dispatches normally.
 * Commit C adds per-entry unwind_policy handling (DROP silently /
 * ERROR emits-with-ERROR-status).
 */
void
otel_producer_span_emit(OtelSpan *span)
{
	int			i;

	if (span == NULL)
		return;

	/* Locate the span on the stack (if pushed).  Search from top down
	 * since the common case is "emit the most recently pushed". */
	for (i = span_stack_top; i >= 0; i--)
	{
		if (memcmp(span_stack[i].span_id, span->span_id,
				   sizeof(span_stack[i].span_id)) == 0)
		{
			if (i != span_stack_top)
				ereport(WARNING,
						(errmsg("otel: span emitted out of stack order; %d span(s) above will be silently dropped",
								span_stack_top - i)));
			/* Pop down to (and including) this entry. */
			span_stack_top = i - 1;
			break;
		}
	}

	dispatch_span(span);
}


/*
 * otel_producer_init --- called from contrib/otel's _PG_init.
 * Currently a no-op placeholder; Commit C will populate this with
 * MemoryContextCallback registration setup and the stack-overflow
 * counter SQL function.
 */
void
otel_producer_init(void)
{
	/* Zero-initialise active-stack state; static storage is already
	 * zero, so this is effectively a documentation site. */
	span_stack_top = -1;
}
