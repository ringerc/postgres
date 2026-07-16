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
 *	    'M' protocol header or via the otel_api.traceparent GUC.  The legacy
 *	    `OtelContext otel_ctx` in otel.c is the canonical storage; this
 *	    file reads it via the existing assign-hook-populated state.
 *
 *	  * Active stack: bounded array of OtelSpanStackEntry, one per
 *	    currently-open span pushed by a consumer.  Each entry stores
 *	    enough W3C identity to reconstitute the current context; push
 *	    variants chain to the existing top, while explicit-parent
 *	    variants do not touch the stack.
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

#include "common/cryptohash.h"
#include "miscadmin.h"
#include "port.h"				/* pg_strong_random */
#include "utils/builtins.h"		/* escape_json */
#include "utils/elog.h"
#include "utils/json.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include "otel.h"
#include "otel_internal.h"


/*
 * Maximum depth of the active-span stack.  Hard-coded for now; promoted
 * to a GUC if/when real workloads need tuning.  At ~120 bytes per entry
 * (OtelSpanStackEntry size below), 64 deep is ~7.5 KB of per-backend
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
	char		trace_id[OTEL_TRACE_ID_LEN + 1];
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
 * Backend-local flag --- set the first time we decline to push due
 * to stack overflow.  Used to emit at most one WARNING per backend
 * (the postmaster log fills up fast otherwise on pathological
 * recursive PL/pgSQL).  Reset never; we'd rather miss a second
 * warning than spam.
 */
static bool stack_overflow_warned = false;


/*
 * span_id_on_stack --- return true if a span with the given span_id is
 * already present anywhere on the active stack.
 *
 * Used as a defensive uniqueness check at push time.  The unwind
 * lookup in both on_memory_context_reset() and otel_producer_span_emit()
 * relies on span_id uniqueness across the entire active stack: the first
 * matching entry is treated as the canonical one.  A duplicate span_id
 * would cause the lookup to pop the WRONG entry, silently corrupting
 * stack state.  Catching it at push time is cheaper than diagnosing the
 * resulting mis-pop.
 */
static bool
span_id_on_stack(const char span_id[OTEL_SPAN_ID_LEN + 1])
{
	int			i;

	for (i = 0; i <= span_stack_top; i++)
	{
		if (memcmp(span_stack[i].span_id, span_id, OTEL_SPAN_ID_LEN + 1) == 0)
			return true;
	}
	return false;
}


/*
 * MemoryContextCallback support.  When a consumer pushes a span via
 * api->span_link_to_active_and_push, we allocate a small node in
 * CurrentMemoryContext and register it as a reset callback.  When
 * that context is reset or deleted (typically by ereport unwinding
 * past the producer's PG_TRY) the callback pops the matching stack
 * entry, applying its unwind_policy.
 *
 * The node is freed automatically with the context; we never need
 * to free it explicitly.  If the consumer's span_emit pops the
 * entry first, the callback later finds nothing to do and harmless-
 * ly returns.
 *
 * The node lives in the same MemoryContext as the consumer's
 * CurrentMemoryContext at push time --- typically a per-statement
 * or per-function context, but the consumer can choose otherwise
 * by switching MemoryContext before calling
 * span_link_to_active_and_push.
 */
typedef struct OtelSpanUnwindNode
{
	MemoryContextCallback cb;
	char		span_id[OTEL_SPAN_ID_LEN + 1];
} OtelSpanUnwindNode;


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
/*
 * Zero-config JSON-log fallback emitter.  Gated by
 * otel_api.emit_spans_to_log.  Used by dispatch_span below.  Moved
 * here from otel_trace.c when the query-tracing module split out
 * in Phase 4 --- the log-line emitter is producer-side
 * infrastructure, not query-tracing-specific.
 *
 * The JSON shape carries headline span identity, name, kind,
 * status, timing, attributes, and events.  Emitted as a single
 * LOG line prefixed with "otel-span: " for log-pipeline
 * filtering.
 */
void
otel_emit_span_as_log_line(const OtelSpan *span)
{
	StringInfoData buf;
	int			i;
	bool		first;

	initStringInfo(&buf);

	appendStringInfoChar(&buf, '{');

	appendStringInfoString(&buf, "\"trace_id\":");
	escape_json(&buf, span->trace_id);
	appendStringInfoString(&buf, ",\"span_id\":");
	escape_json(&buf, span->span_id);
	appendStringInfoString(&buf, ",\"parent_span_id\":");
	escape_json(&buf, span->parent_span_id);
	appendStringInfoString(&buf, ",\"trace_flags\":");
	escape_json(&buf, span->trace_flags);
	if (span->tracestate)
	{
		appendStringInfoString(&buf, ",\"tracestate\":");
		escape_json(&buf, span->tracestate);
	}
	appendStringInfoString(&buf, ",\"name\":");
	escape_json(&buf, span->name ? span->name : "");
	appendStringInfo(&buf, ",\"kind\":%d", (int) span->kind);
	appendStringInfo(&buf, ",\"status\":%d", (int) span->status);
	appendStringInfo(&buf, ",\"start_time\":%" PRId64,
					 (int64) span->start_time);
	appendStringInfo(&buf, ",\"end_time\":%" PRId64,
					 (int64) span->end_time);

	appendStringInfoString(&buf, ",\"attributes\":{");
	first = true;
	for (i = 0; i < span->n_attrs; i++)
	{
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;
		escape_json(&buf, span->attrs[i].key ? span->attrs[i].key : "");
		appendStringInfoChar(&buf, ':');
		escape_json(&buf, span->attrs[i].value ? span->attrs[i].value : "");
	}
	for (i = 0; i < span->n_overflow_attrs; i++)
	{
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;
		escape_json(&buf, span->overflow_attrs[i].key ? span->overflow_attrs[i].key : "");
		appendStringInfoChar(&buf, ':');
		escape_json(&buf, span->overflow_attrs[i].value ? span->overflow_attrs[i].value : "");
	}
	appendStringInfoChar(&buf, '}');

	/*
	 * Events: the unified generic list.  Any ereport-derived
	 * "exception" event has already been lowered into span->events by
	 * otel_producer_span_emit before dispatch, so this path is fully
	 * generic --- name, time, and attrs.  There is no longer any
	 * elevel/sqlstate/filename/... special-casing here.
	 */
	appendStringInfoString(&buf, ",\"events\":[");
	for (i = 0; i < span->n_events; i++)
	{
		const OtelSpanEvent *e = &span->events[i];
		int			j;

		if (i > 0)
			appendStringInfoChar(&buf, ',');
		appendStringInfoChar(&buf, '{');
		appendStringInfoString(&buf, "\"name\":");
		escape_json(&buf, e->name ? e->name : "");
		appendStringInfo(&buf, ",\"time\":%" PRId64, (int64) e->time);
		appendStringInfoString(&buf, ",\"attributes\":{");
		for (j = 0; j < e->n_attrs; j++)
		{
			if (j > 0)
				appendStringInfoChar(&buf, ',');
			escape_json(&buf, e->attrs[j].key ? e->attrs[j].key : "");
			appendStringInfoChar(&buf, ':');
			escape_json(&buf, e->attrs[j].value ? e->attrs[j].value : "");
		}
		appendStringInfoChar(&buf, '}');
		appendStringInfoChar(&buf, '}');
	}
	appendStringInfoChar(&buf, ']');

	/* Span links (associations with spans in other traces). */
	if (span->n_links > 0)
	{
		appendStringInfoString(&buf, ",\"links\":[");
		for (i = 0; i < span->n_links; i++)
		{
			if (i > 0)
				appendStringInfoChar(&buf, ',');
			appendStringInfoString(&buf, "{\"trace_id\":");
			escape_json(&buf, span->links[i].trace_id);
			appendStringInfoString(&buf, ",\"span_id\":");
			escape_json(&buf, span->links[i].span_id);
			appendStringInfoString(&buf, ",\"trace_flags\":");
			escape_json(&buf, span->links[i].trace_flags);
			appendStringInfoChar(&buf, '}');
		}
		appendStringInfoChar(&buf, ']');
	}

	if (span->scope)
	{
		appendStringInfoString(&buf, ",\"scope\":{\"name\":");
		escape_json(&buf, span->scope->name ? span->scope->name : "");
		if (span->scope->version)
		{
			appendStringInfoString(&buf, ",\"version\":");
			escape_json(&buf, span->scope->version);
		}
		if (span->scope->schema_url)
		{
			appendStringInfoString(&buf, ",\"schema_url\":");
			escape_json(&buf, span->scope->schema_url);
		}
		appendStringInfoString(&buf, "}");
	}

	appendStringInfoChar(&buf, '}');

	ereport(LOG,
			(errmsg_internal("otel-span: %s", buf.data)));

	pfree(buf.data);
}


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
		if (otel_emit_spans_to_log)
			otel_emit_span_as_log_line(span);
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
}


/*
 * Pop entries from the top of the stack down to (but excluding) a
 * target index, applying each entry's unwind_policy.  Used by both
 * the MemoryContextCallback (which is then called with target -1
 * to drain everything matching the callback) and by span_emit on
 * the out-of-order path.
 *
 * For OTEL_UNWIND_ERROR entries with a non-NULL span pointer:
 *	   * status is set to OTEL_STATUS_ERROR (unless already set);
 *	   * status_description is set to the supplied reason string;
 *	   * end_time is set to now;
 *	   * dispatch_span is called.
 *
 * For OTEL_UNWIND_DROP entries: nothing besides the pop.
 */
static void
unwind_to(int target_top, const char *reason)
{
	while (span_stack_top > target_top)
	{
		OtelSpanStackEntry *e = &span_stack[span_stack_top];

		if (e->unwind_policy == OTEL_UNWIND_ERROR && e->span != NULL)
		{
			if (e->span->status == OTEL_STATUS_UNSET)
				e->span->status = OTEL_STATUS_ERROR;
			e->span->status_description = reason;
			e->span->end_time = GetCurrentTimestamp();
			dispatch_span(e->span);
			e->span->on_active_stack = false;
		}
		/* Clear before decrementing so a future re-push to this slot
		 * starts with a clean slate. */
		e->span = NULL;
		span_stack_top--;
	}
}

/*
 * MemoryContextCallback driver.  Called when the consumer's
 * CurrentMemoryContext (at push time) is reset or deleted ---
 * typically because ereport unwound through it.  Find the matching
 * span_id in the stack and unwind THAT ENTRY ONLY.
 *
 * Critically: we do NOT touch entries above the matched one.  A
 * producer that pushes A under context ctx-A, then
 * MemoryContextSwitchTo(ctx-B) and pushes B (with ctx-B's reset
 * scope), produces a stack with A at a lower index than B but a
 * callback registered against ctx-A.  When ctx-A resets, this
 * callback fires for A's entry.  B was pushed under ctx-B, which
 * has not been reset; B's stack entry must survive.
 *
 * Implementation: remove the matched entry in place and slide
 * entries above it down by one.  span_stack_top decrements by one.
 * Lookup keys (span_id) are stable across the move because each
 * entry is a value-copy.  Callbacks for entries that moved still
 * find their span_id via memcmp at the new index.
 */
static void
on_memory_context_reset(void *arg)
{
	OtelSpanUnwindNode *node = (OtelSpanUnwindNode *) arg;
	int			i;

	/*
	 * Unwind-stack lookup invariant: span_id values on the active stack are
	 * unique (enforced by the duplicate-check in both push functions).
	 * Stopping at the first match is therefore correct and sufficient ---
	 * there can be at most one matching entry.
	 */
	for (i = span_stack_top; i >= 0; i--)
	{
		OtelSpanStackEntry *e = &span_stack[i];

		if (memcmp(e->span_id, node->span_id,
				   sizeof(node->span_id)) == 0)
		{
			/* Apply unwind_policy to THIS entry only. */
			if (e->unwind_policy == OTEL_UNWIND_ERROR && e->span != NULL)
			{
				if (e->span->status == OTEL_STATUS_UNSET)
					e->span->status = OTEL_STATUS_ERROR;
				e->span->status_description = "unwound by ereport";
				e->span->end_time = GetCurrentTimestamp();
				dispatch_span(e->span);
				e->span->on_active_stack = false;
			}

			/* Remove the matched entry; slide entries above down by
			 * one to keep the stack contiguous.  For a top-of-stack
			 * match the loop body executes zero times --- a
			 * straightforward pop. */
			for (int j = i; j < span_stack_top; j++)
				span_stack[j] = span_stack[j + 1];
			memset(&span_stack[span_stack_top], 0,
				   sizeof(span_stack[span_stack_top]));
			span_stack_top--;
			return;
		}
	}
	/* Not found: span_emit already popped it, so this callback is a
	 * no-op.  This is the common success path: explicit emit beats
	 * the callback every time. */
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
/*
 * check_unwind_context --- defensive misuse check for push time.
 *
 * The push functions register a MemoryContextResetCallback against
 * the active CurrentMemoryContext.  Under OTEL_UNWIND_ERROR that
 * callback is the safety net that emits an aborted span on ereport
 * unwind --- but only if the context actually resets in the normal
 * course of operation.  TopMemoryContext, CacheMemoryContext, and
 * ErrorContext don't, so binding the safety net to them silently
 * defeats it.
 *
 * Emit a LOG-level message (server log only --- never delivered to
 * the client connection, which is the right severity for an API
 * misuse the SQL caller had no way to cause) and Assert() so cassert
 * builds crash where the bug is rather than later when the missing
 * span causes a confusing absence in trace output.
 *
 * Only fires under OTEL_UNWIND_ERROR; under OTEL_UNWIND_DROP the
 * callback isn't load-bearing, so the context choice is harmless.
 */
static void
check_unwind_context(const OtelSpan *span)
{
	if (span->unwind_policy != OTEL_UNWIND_ERROR)
		return;

	if (CurrentMemoryContext == TopMemoryContext ||
		CurrentMemoryContext == CacheMemoryContext ||
		CurrentMemoryContext == ErrorContext)
	{
		ereport(LOG,
				(errmsg("otel_api: span pushed under OTEL_UNWIND_ERROR with a long-lived MemoryContext"),
				 errdetail("CurrentMemoryContext = \"%s\"; the MemoryContextResetCallback that drives the unwind safety net will not fire as expected.",
						   CurrentMemoryContext->name),
				 errhint("MemoryContextSwitchTo a per-statement or per-executor context before calling the push function.")));
		Assert(false);
	}
}

/*
 * otel_producer_span_push --- push the span onto the active stack
 * without fetching a parent context.  Used internally by
 * otel_trace.c during Phase 2 migration so the existing
 * start_span / start_utility_span code can populate parent fields
 * themselves (including the parallel-worker leader-span-id logic
 * that takes priority over otel_ctx.span_id) and just push the
 * result onto the stack.
 *
 * Caller is responsible for populating span->span_id,
 * span->trace_flags, and span->unwind_policy before calling.
 *
 * Behaviour on overflow / MemoryContextCallback registration is
 * identical to otel_producer_span_link_to_active_and_push.
 */
void
otel_producer_span_push(OtelSpan *span)
{
	if (span == NULL)
		return;

	check_unwind_context(span);

	if (span_stack_top + 1 < MAX_SPAN_STACK_DEPTH)
	{
		OtelSpanStackEntry *entry;
		OtelSpanUnwindNode *node;

		/*
		 * Uniqueness invariant: every span_id on the active stack must be
		 * distinct.  The unwind lookup (on_memory_context_reset and
		 * otel_producer_span_emit) does a linear scan and treats the first
		 * match as canonical --- a duplicate would silently pop the wrong
		 * entry and corrupt stack state.
		 *
		 * In assert builds, die loudly so the caller can fix the ID
		 * generation bug.  In production builds, emit a WARNING and refuse
		 * to push the duplicate rather than silently corrupt the stack.
		 */
		if (span_id_on_stack(span->span_id))
		{
			Assert(false);		/* duplicate span_id in push path */
			ereport(WARNING,
					(errmsg("otel: duplicate span_id \"%s\" already on active stack; push refused",
							span->span_id),
					 errdetail("The span was not pushed. span_id uniqueness is required for correct unwind-stack operation."),
					 errhint("Check span_id generation; pg_strong_random failure may be producing colliding fallback IDs.")));
			return;
		}

		span_stack_top++;
		entry = &span_stack[span_stack_top];
		memcpy(entry->trace_id, span->trace_id, sizeof(entry->trace_id));
		memcpy(entry->span_id, span->span_id, sizeof(entry->span_id));
		memcpy(entry->trace_flags, span->trace_flags, sizeof(entry->trace_flags));
		entry->unwind_policy = span->unwind_policy;
		/*
		 * Only OTEL_UNWIND_ERROR ever needs to dereference the
		 * borrowed pointer at unwind time.  Under OTEL_UNWIND_DROP
		 * store NULL outright so there is structurally no
		 * dangling-pointer hazard --- even an on-stack OtelSpan
		 * cannot become a stale dereference target.
		 */
		entry->span = (span->unwind_policy == OTEL_UNWIND_ERROR) ? span : NULL;

		node = (OtelSpanUnwindNode *) MemoryContextAllocExtended(CurrentMemoryContext,
																 sizeof(*node),
																 MCXT_ALLOC_NO_OOM);
		if (node != NULL)
		{
			memcpy(node->span_id, span->span_id, sizeof(node->span_id));
			node->cb.func = on_memory_context_reset;
			node->cb.arg = node;
			MemoryContextRegisterResetCallback(CurrentMemoryContext, &node->cb);
		}

		span->on_active_stack = true;
	}
	else
	{
		if (!stack_overflow_warned)
		{
			ereport(WARNING,
					(errmsg("otel: span-stack overflow at depth %d",
							MAX_SPAN_STACK_DEPTH),
					 errdetail("Further over-cap spans in this backend will still link to the deepest-pushed parent for correctness but will not be pushed onto the active stack."),
					 errhint("Reduce instrumentation nesting or, if the workload genuinely needs deeper nesting, increase MAX_SPAN_STACK_DEPTH and rebuild contrib/otel.")));
			stack_overflow_warned = true;
		}
	}
}

void
otel_producer_span_link_to_active_and_push(OtelSpan *span)
{
	if (span == NULL)
		return;

	check_unwind_context(span);

	/* Fetch parent: top-of-stack > root context > none. */
	if (span_stack_top >= 0)
	{
		const OtelSpanStackEntry *top = &span_stack[span_stack_top];

		memcpy(span->trace_id, top->trace_id, sizeof(span->trace_id));
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
		OtelSpanUnwindNode *node;

		/*
		 * Uniqueness invariant: every span_id on the active stack must be
		 * distinct.  See the identical check in otel_producer_span_push for
		 * the full rationale.
		 */
		if (span_id_on_stack(span->span_id))
		{
			Assert(false);		/* duplicate span_id in push path */
			ereport(WARNING,
					(errmsg("otel: duplicate span_id \"%s\" already on active stack; push refused",
							span->span_id),
					 errdetail("The span was not pushed. span_id uniqueness is required for correct unwind-stack operation."),
					 errhint("Check span_id generation; pg_strong_random failure may be producing colliding fallback IDs.")));
			return;
		}

		span_stack_top++;
		entry = &span_stack[span_stack_top];
		memcpy(entry->trace_id, span->trace_id, sizeof(entry->trace_id));
		memcpy(entry->span_id, span->span_id, sizeof(entry->span_id));
		memcpy(entry->trace_flags, span->trace_flags, sizeof(entry->trace_flags));
		entry->unwind_policy = span->unwind_policy;
		/* DROP entries store NULL --- see otel_producer_span_push. */
		entry->span = (span->unwind_policy == OTEL_UNWIND_ERROR) ? span : NULL;

		/*
		 * Register a MemoryContextCallback against CurrentMemoryContext
		 * so that an ereport unwind through the producer's context
		 * correctly pops this entry and applies its unwind_policy.
		 * The node lives in CurrentMemoryContext and is freed
		 * automatically when that context is reset/deleted (after the
		 * callback fires).
		 *
		 * On allocation failure the call is downgraded: we still push
		 * the entry, but no callback is installed.  An ereport-unwind
		 * in that case leaves a stale stack entry that span_emit /
		 * span_link_to_active_and_push will eventually find and step
		 * past (the next emit with a matching span_id pops it; pushes
		 * past stack-overflow keep parent linkage correct).  The
		 * cost is one missing emit-as-ERROR on unwind for an
		 * OTEL_UNWIND_ERROR span --- acceptable under OOM.
		 */
		node = (OtelSpanUnwindNode *) MemoryContextAllocExtended(CurrentMemoryContext,
																 sizeof(*node),
																 MCXT_ALLOC_NO_OOM);
		if (node != NULL)
		{
			memcpy(node->span_id, span->span_id, sizeof(node->span_id));
			node->cb.func = on_memory_context_reset;
			node->cb.arg = node;
			MemoryContextRegisterResetCallback(CurrentMemoryContext, &node->cb);
		}

		span->on_active_stack = true;
	}
	else
	{
		/* Stack overflow.  Parent linkage was set above so the new
		 * span still threads correctly into the trace; we just don't
		 * push it.  Subsequent pushes beyond MAX_SPAN_STACK_DEPTH
		 * will all chain to the same deepest-pushed parent ---
		 * approximately right since they share the same logical
		 * scope.
		 *
		 * Emit one WARNING per backend session; otherwise pathologic-
		 * ally recursive instrumentation could spam the log. */
		if (!stack_overflow_warned)
		{
			ereport(WARNING,
					(errmsg("otel: span-stack overflow at depth %d",
							MAX_SPAN_STACK_DEPTH),
					 errdetail("Further over-cap spans in this backend will still link to the deepest-pushed parent for correctness but will not be pushed onto the active stack."),
					 errhint("Reduce instrumentation nesting or, if the workload genuinely needs deeper nesting, increase MAX_SPAN_STACK_DEPTH and rebuild contrib/otel.")));
			stack_overflow_warned = true;
		}
	}
}

/*
 * api_span_set_parent_explicit --- caller provides parent
 * SpanContext directly; stack is untouched.  Used for spans that
 * belong to a trace maintained independently of the active
 * call-stack-based trace (background work, sibling spans, etc.).
 *
 * If `parent` is NULL, span identity stays as the caller
 * pre-populated it.
 *
 * IMPORTANT: must NOT be called on a span that has been pushed
 * via any _and_push verb.  Doing so rewrites the span's trace_id
 * but leaves the LIFO stack entry holding the old pre-push
 * snapshot; children pushed while this span is active will
 * inherit the stale trace_id and appear in a disconnected trace.
 * Under USE_ASSERT_CHECKING this misuse is caught here.
 */
void
otel_producer_span_set_parent_explicit(OtelSpan *span, const OtelSpanContext *parent)
{
	if (span == NULL || parent == NULL)
		return;

	/*
	 * Guard against the "push then reparent" bug class.  If this span is
	 * already on the active LIFO stack, the stack entry holds a snapshot
	 * of the pre-push trace_id; rewriting trace_id here leaves the snapshot
	 * stale so children pushed while this span is active inherit the wrong
	 * trace.  Use span_link_to_ctx_and_push / span_link_to_span_and_push
	 * instead to get a consistent snapshot from construction time.
	 */
	Assert(!span->on_active_stack);

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

		memcpy(current_ctx_buf.trace_id, top->trace_id, sizeof(current_ctx_buf.trace_id));
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
/*
 * lower_error_event --- synthesize the generic "exception" OtelSpanEvent
 * from a span's producer-private OtelErrorCapture and append it to
 * span->events.  This is the single place the ereport->attribute mapping
 * lives; exporters see only the resulting generic event.
 *
 * Attribute keys match what downstream translators expect
 * (postgres.sqlstate / postgres.elevel / code.filepath / code.lineno /
 * code.function / postgres.detail / postgres.hint) plus exception.message
 * for the message.  Numeric fields (elevel, lineno) are stringified ---
 * OtelKeyValue is string-only for now.
 *
 * On OOM the full event may be dropped; we then degrade to a literal
 * name="exception" event with zero attrs (no allocation for name in the
 * degraded path beyond the events-array slot itself).
 */
static void
lower_error_event(OtelSpan *span)
{
	const OtelErrorCapture *ec = &span->error_event;
	OtelKeyValue attrs[8];
	int			n = 0;
	char		elevel_buf[16];
	char		lineno_buf[16];
	int			before;

	/* exception.message (best-effort; may be NULL) */
	if (ec->message != NULL)
	{
		attrs[n].key = "exception.message";
		attrs[n].value = ec->message;
		n++;
	}
	if (ec->sqlstate[0] != '\0')
	{
		attrs[n].key = "postgres.sqlstate";
		attrs[n].value = ec->sqlstate;
		n++;
	}
	snprintf(elevel_buf, sizeof(elevel_buf), "%d", ec->elevel);
	attrs[n].key = "postgres.elevel";
	attrs[n].value = elevel_buf;
	n++;
	if (ec->filename != NULL)
	{
		attrs[n].key = "code.filepath";
		attrs[n].value = ec->filename;
		n++;
	}
	snprintf(lineno_buf, sizeof(lineno_buf), "%d", ec->lineno);
	attrs[n].key = "code.lineno";
	attrs[n].value = lineno_buf;
	n++;
	if (ec->funcname != NULL)
	{
		attrs[n].key = "code.function";
		attrs[n].value = ec->funcname;
		n++;
	}
	if (ec->detail != NULL)
	{
		attrs[n].key = "postgres.detail";
		attrs[n].value = ec->detail;
		n++;
	}
	if (ec->hint != NULL)
	{
		attrs[n].key = "postgres.hint";
		attrs[n].value = ec->hint;
		n++;
	}

	before = span->n_events;
	otel_producer_span_add_event(span, "exception", ec->time, attrs, n);

	/*
	 * If the full event could not be allocated (attrs copy or array grow
	 * failed under memory pressure), degrade to a literal name="exception"
	 * event with zero attrs.
	 */
	if (span->n_events == before)
		otel_producer_span_add_event(span, "exception", ec->time, NULL, 0);
}

void
otel_producer_span_emit(OtelSpan *span)
{
	int			i;

	if (span == NULL)
		return;

	/*
	 * Lower any producer-private ereport capture into a generic
	 * "exception" event before dispatch, so span->events is the complete
	 * list every exporter reads.  Guarded so a span emitted twice (rare)
	 * doesn't lower twice.
	 */
	if (span->error_event_used)
	{
		span->error_event_used = false;
		lower_error_event(span);
	}

	/*
	 * Locate the span on the stack (if pushed).  Search from top down
	 * since the common case is "emit the most recently pushed".
	 *
	 * Relies on the uniqueness invariant: span_id values on the active stack
	 * are unique (enforced at push time by span_id_on_stack checks in both
	 * push functions).  Stopping at the first match is correct --- there is
	 * at most one matching entry.
	 */
	for (i = span_stack_top; i >= 0; i--)
	{
		if (memcmp(span_stack[i].span_id, span->span_id,
				   sizeof(span_stack[i].span_id)) == 0)
		{
			if (i != span_stack_top)
			{
				ereport(WARNING,
						(errmsg("otel: span emitted out of stack order; %d span(s) above will be unwound",
								span_stack_top - i)));
				/* Drain entries above the target, honouring each
				 * one's unwind_policy.  Drain target is i so that i
				 * itself stays on top after this call. */
				unwind_to(i, "unwound by out-of-order emit");
			}
			/* Pop the target entry itself. */
			span_stack[span_stack_top].span = NULL;
			span_stack_top = i - 1;
			span->on_active_stack = false;
			break;
		}
	}

	dispatch_span(span);
}


/* ====================================================================
 * Producer-side convenience helpers --- out-of-line companions to
 * the static inlines in otel.h.
 * ==================================================================== */

/*
 * Convert raw bytes to lowercase-hex.  Local copy because the
 * matching helper in otel_trace.c is module-static.
 */
static void
bytes_to_lower_hex(const unsigned char *src, size_t n, char *dst)
{
	static const char hex[] = "0123456789abcdef";
	size_t		i;

	for (i = 0; i < n; i++)
	{
		dst[i * 2] = hex[(src[i] >> 4) & 0xF];
		dst[i * 2 + 1] = hex[src[i] & 0xF];
	}
	dst[n * 2] = '\0';
}

void
otel_span_init(OtelSpan *span,
			   const OtelInstrumentationScope *scope,
			   const char *name,
			   OtelSpanKind kind)
{
	unsigned char span_buf[OTEL_SPAN_ID_LEN / 2];
	unsigned char trace_buf[OTEL_TRACE_ID_LEN / 2];

	memset(span, 0, sizeof(*span));

	/* Generate fresh span_id.  pg_strong_random is overkill for
	 * span IDs (8 random bytes is enough collision resistance for
	 * any realistic trace volume) but it's the available API and
	 * does the right thing. */
	if (!pg_strong_random(span_buf, sizeof(span_buf)))
	{
		/* Random source unavailable --- degrade gracefully.  Use a
		 * timestamp + pid mix for at least some uniqueness within
		 * the backend. */
		uint64		fallback = (uint64) GetCurrentTimestamp() ^ (uint64) MyProcPid;
		memcpy(span_buf, &fallback, sizeof(span_buf));
	}
	bytes_to_lower_hex(span_buf, sizeof(span_buf), span->span_id);

	/* Generate a fresh trace_id as well.  If no active/root context
	 * exists when the span is pushed, this span becomes the root of a
	 * new valid trace rather than carrying an all-zero trace_id. */
	if (!pg_strong_random(trace_buf, sizeof(trace_buf)))
	{
		uint64		fallback_a = (uint64) GetCurrentTimestamp() ^ (uint64) MyProcPid;
		uint64		fallback_b = (uint64) MyStartTimestamp ^ ((uint64) MyProcPid << 32);

		memcpy(trace_buf, &fallback_a, sizeof(fallback_a));
		memcpy(trace_buf + sizeof(fallback_a), &fallback_b, sizeof(fallback_b));
	}
	bytes_to_lower_hex(trace_buf, sizeof(trace_buf), span->trace_id);

	span->scope = scope;
	span->name = name;
	span->kind = kind;
	span->status = OTEL_STATUS_UNSET;
	span->sampler_decision = OTEL_SAMPLE_RECORD_AND_SAMPLE;
	span->unwind_policy = OTEL_UNWIND_DROP;
	span->start_time = GetCurrentTimestamp();
}

/* otel_span_finalize is a static inline in otel.h (single struct
 * write to end_time = now); no out-of-line definition needed. */

bool
otel_span_add_attribute_string(OtelSpan *span, const char *key, const char *value)
{
	if (span->n_attrs < OTEL_INLINE_ATTRS)
	{
		span->attrs[span->n_attrs].key = key;
		span->attrs[span->n_attrs].value = value;
		span->n_attrs++;
		return true;
	}

	/* Inline slots full; grow the overflow array by one.  Matches the
	 * existing pattern in otel_trace.c's span_add_attr.  repalloc is
	 * routed through the pointer's owning MemoryContext, so this is
	 * safe across CurrentMemoryContext switches between calls.  On
	 * OOM we silently drop the attribute --- best-effort
	 * instrumentation. */
	{
		int			newcnt = span->n_overflow_attrs + 1;
		OtelKeyValue *newarr;

		if (span->overflow_attrs == NULL)
			newarr = (OtelKeyValue *)
				MemoryContextAllocExtended(CurrentMemoryContext,
										   sizeof(OtelKeyValue) * newcnt,
										   MCXT_ALLOC_NO_OOM);
		else
			newarr = (OtelKeyValue *)
				repalloc_extended(span->overflow_attrs,
								  sizeof(OtelKeyValue) * newcnt,
								  MCXT_ALLOC_NO_OOM);
		if (newarr == NULL)
			return false;
		newarr[newcnt - 1].key = key;
		newarr[newcnt - 1].value = value;
		span->overflow_attrs = newarr;
		span->n_overflow_attrs = newcnt;
		return true;
	}
}


/*
 * otel_producer_span_add_attribute_string_to_active --- add a string attribute
 * to the top-of-stack span without requiring a pointer to it.  Intended for
 * contrib modules that hook ExecutorEnd (e.g. auto_explain) and want to
 * enrich the active statement span without managing the span themselves.
 *
 * Returns true when the attribute was attached, false when there is no active
 * span with a stored pointer (no stack entry, or top entry is OTEL_UNWIND_DROP).
 */
bool
otel_producer_span_add_attribute_string_to_active(const char *key,
												   const char *value)
{
	OtelSpanStackEntry *top;

	if (span_stack_top < 0)
		return false;

	top = &span_stack[span_stack_top];

	/*
	 * The span pointer is stored only for OTEL_UNWIND_ERROR entries; for
	 * OTEL_UNWIND_DROP the slot holds NULL by design (see otel_producer_span_push).
	 */
	if (top->span == NULL)
		return false;

	return otel_span_add_attribute_string(top->span, key, value);
}


/*
 * otel_producer_span_add_event --- append a named generic event to a span.
 *
 * COPY semantics: the event name and every attribute key/value string,
 * plus the OtelKeyValue array itself, are pstrdup'd / copied into the
 * CurrentMemoryContext so the caller may free or reuse its transient
 * buffers immediately.  (This is the per-node/transient-attrs case; it
 * intentionally diverges from span_add_attribute_string's borrow rule.)
 *
 * ts == 0 means "now" (GetCurrentTimestamp()).  attrs may be NULL when
 * n_attrs <= 0.
 *
 * The span->events array grows one entry at a time via
 * palloc/repalloc with MCXT_ALLOC_NO_OOM (matching the overflow-attr
 * pattern); on any allocation failure the event is silently dropped ---
 * best-effort instrumentation, never breaks the query.
 */
void
otel_producer_span_add_event(OtelSpan *span, const char *name,
							 TimestampTz ts, const OtelKeyValue *attrs,
							 int n_attrs)
{
	int			newcnt;
	OtelSpanEvent *newarr;
	OtelSpanEvent *ev;
	char	   *name_copy;
	OtelKeyValue *attrs_copy = NULL;

	if (span == NULL)
		return;
	if (n_attrs < 0)
		n_attrs = 0;
	if (attrs == NULL)
		n_attrs = 0;

	if (ts == 0)
		ts = GetCurrentTimestamp();

	/*
	 * Copy the name.  On OOM we drop the whole event (best-effort).
	 */
	name_copy = (char *) MemoryContextAllocExtended(CurrentMemoryContext,
													(name ? strlen(name) : 0) + 1,
													MCXT_ALLOC_NO_OOM);
	if (name_copy == NULL)
		return;
	if (name != NULL)
		strcpy(name_copy, name);
	else
		name_copy[0] = '\0';

	/*
	 * Copy the attribute array + each key/value string.  On any OOM we
	 * abandon the whole event (the already-allocated name_copy is left
	 * for the span context to reclaim; harmless).
	 */
	if (n_attrs > 0)
	{
		int			k;

		attrs_copy = (OtelKeyValue *)
			MemoryContextAllocExtended(CurrentMemoryContext,
									   sizeof(OtelKeyValue) * n_attrs,
									   MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
		if (attrs_copy == NULL)
			return;

		for (k = 0; k < n_attrs; k++)
		{
			const char *key = attrs[k].key;
			const char *val = attrs[k].value;
			char	   *keyc = NULL;
			char	   *valc = NULL;

			if (key != NULL)
			{
				keyc = (char *) MemoryContextAllocExtended(CurrentMemoryContext,
														   strlen(key) + 1,
														   MCXT_ALLOC_NO_OOM);
				if (keyc == NULL)
					return;		/* drop event */
				strcpy(keyc, key);
			}
			if (val != NULL)
			{
				valc = (char *) MemoryContextAllocExtended(CurrentMemoryContext,
														   strlen(val) + 1,
														   MCXT_ALLOC_NO_OOM);
				if (valc == NULL)
					return;		/* drop event */
				strcpy(valc, val);
			}
			attrs_copy[k].key = keyc;
			attrs_copy[k].value = valc;
		}
	}

	/* Grow the events array by one. */
	newcnt = span->n_events + 1;
	if (span->events == NULL)
		newarr = (OtelSpanEvent *)
			MemoryContextAllocExtended(CurrentMemoryContext,
									   sizeof(OtelSpanEvent) * newcnt,
									   MCXT_ALLOC_NO_OOM);
	else
		newarr = (OtelSpanEvent *)
			repalloc_extended(span->events,
							  sizeof(OtelSpanEvent) * newcnt,
							  MCXT_ALLOC_NO_OOM);
	if (newarr == NULL)
		return;					/* drop event */

	ev = &newarr[newcnt - 1];
	ev->name = name_copy;
	ev->time = ts;
	ev->n_attrs = n_attrs;
	ev->attrs = attrs_copy;

	span->events = newarr;
	span->n_events = newcnt;
}

/*
 * otel_producer_span_add_event_to_active --- append a named event to the
 * top-of-stack active span without a pointer to it.  Mirrors
 * otel_producer_span_add_attribute_string_to_active.
 *
 * Returns true when the event was appended (or attempted best-effort on
 * a valid active span), false when there is no active span with a
 * stored pointer (empty stack, or top entry is OTEL_UNWIND_DROP).
 */
bool
otel_producer_span_add_event_to_active(const char *name, TimestampTz ts,
									   const OtelKeyValue *attrs, int n_attrs)
{
	OtelSpanStackEntry *top;

	if (span_stack_top < 0)
		return false;

	top = &span_stack[span_stack_top];

	/*
	 * The span pointer is stored only for OTEL_UNWIND_ERROR entries; for
	 * OTEL_UNWIND_DROP the slot holds NULL by design (see
	 * otel_producer_span_push).
	 */
	if (top->span == NULL)
		return false;

	otel_producer_span_add_event(top->span, name, ts, attrs, n_attrs);
	return true;
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


/* ====================================================================
 * Explicit-parent-and-push constructors (MINOR 4).
 *
 * These replace the bug-prone sequence:
 *
 *	   api->span_link_to_active_and_push(&span);   // snapshots throwaway
 *	   api->span_set_parent_explicit(&span, &p);   // rewrites span,
 *	                                               // stack entry stale
 *
 * ...which silently broke child LIFO span lineage because the LIFO
 * stack entry kept the throwaway trace_id captured at push time.
 * ==================================================================== */

/*
 * otel_producer_span_link_to_ctx_and_push --- construct a LIFO stack
 * span whose parent identity is taken from `parent_ctx`.
 *
 * Behaviour is the union of span_set_parent_explicit (rewrite
 * trace_id / parent_span_id / trace_flags from the supplied context)
 * and span_push (push the result onto the active stack), executed
 * atomically before the stack snapshot is taken.  This guarantees the
 * pushed entry sees the same trace_id as the span itself, so children
 * pushed while this span is active correctly inherit the explicit
 * parent's trace.
 *
 * `parent_ctx` may be NULL: the span keeps its api->span_init-generated
 * trace_id with zero parent_span_id (fresh root trace).
 *
 * Caller must have called api->span_init on `span` first (same idiom
 * as span_link_to_active_and_push).
 */
void
otel_producer_span_link_to_ctx_and_push(OtelSpan *span,
										const OtelSpanContext *parent_ctx)
{
	if (span == NULL)
		return;

	/* Apply the explicit parent before push so the stack entry snapshot
	 * is consistent with the span from the start. */
	if (parent_ctx != NULL)
	{
		memcpy(span->trace_id, parent_ctx->trace_id, sizeof(span->trace_id));
		memcpy(span->parent_span_id, parent_ctx->span_id, sizeof(span->parent_span_id));
		memcpy(span->trace_flags, parent_ctx->trace_flags, sizeof(span->trace_flags));
	}
	/* else: parent_ctx == NULL -> root span; keep span_init's trace_id,
	 * leave parent_span_id as zeroed by span_init. */

	otel_producer_span_push(span);
}

/*
 * otel_producer_span_link_to_span_and_push --- convenience wrapper
 * around span_link_to_ctx_and_push that takes another OtelSpan as
 * the parent (e.g. a heap-allocated apply.txn / output.txn span).
 *
 * Copies the parent's trace_id / span_id / trace_flags into a
 * temporary OtelSpanContext and delegates.  `parent` may be NULL
 * (root behaviour identical to span_link_to_ctx_and_push(span, NULL)).
 */
void
otel_producer_span_link_to_span_and_push(OtelSpan *span,
										 const OtelSpan *parent)
{
	if (span == NULL)
		return;

	if (parent != NULL)
	{
		OtelSpanContext tmp;

		memcpy(tmp.trace_id, parent->trace_id, sizeof(tmp.trace_id));
		memcpy(tmp.span_id, parent->span_id, sizeof(tmp.span_id));
		memcpy(tmp.trace_flags, parent->trace_flags, sizeof(tmp.trace_flags));
		tmp.tracestate = NULL;
		otel_producer_span_link_to_ctx_and_push(span, &tmp);
	}
	else
		otel_producer_span_link_to_ctx_and_push(span, NULL);
}

/*
 * otel_producer_span_context_of --- populate `out` with a SpanContext
 * describing `span`'s current identity (trace_id, span_id,
 * trace_flags; tracestate = NULL).
 *
 * Safe for callers who need to pass a running span as the parent of
 * another span without going through span_current_context (which
 * returns the top-of-stack, not necessarily `span`).
 *
 * `span` and `out` must not be NULL.
 */
void
otel_producer_span_context_of(const OtelSpan *span, OtelSpanContext *out)
{
	Assert(span != NULL);
	Assert(out != NULL);

	memcpy(out->trace_id, span->trace_id, sizeof(out->trace_id));
	memcpy(out->span_id, span->span_id, sizeof(out->span_id));
	memcpy(out->trace_flags, span->trace_flags, sizeof(out->trace_flags));
	out->tracestate = NULL;
}
