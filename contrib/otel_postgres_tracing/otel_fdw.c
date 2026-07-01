/*-------------------------------------------------------------------------
 *
 * otel_fdw.c
 *	  pg.fdw.scan span tracing for foreign-table scans.
 *
 * Each ForeignScanState that enters execution gets a pg.fdw.scan child
 * span (kind = CLIENT, the backend acting as a client to an external data
 * source) under the enclosing pgsql.execute span.  The span carries
 * db.system = postgresql and db.collection.name = <foreign table name>.
 *
 * Two strategies, selected at compile time
 * ----------------------------------------
 * The *span machinery* (the fixed-depth stack, the begin/end logic, the
 * (sub)transaction-abort cleanup) is identical across PostgreSQL versions.
 * Only the way begin/end events are *delivered* differs:
 *
 *   PG19+  (OTEL_FDW_USE_CORE_HOOKS): the otel core patch adds
 *          ForeignScanBegin_hook / ForeignScanEnd_hook, fired from
 *          ExecInitForeignScan / ExecEndForeignScan.  This is precise
 *          (exact node boundaries) and free for FDW-less queries (the core
 *          just skips a NULL hook pointer).  Preferred where available.
 *
 *   PG18   (no such hooks in core, and the otel PG18 port deliberately did
 *          not carry that core patch): we instead drive off the existing
 *          ExecutorStart_hook / ExecutorEnd_hook, which otel_trace.c already
 *          installs, and walk queryDesc->planstate with planstate_tree_walker
 *          to find ForeignScanState nodes.  This needs no core patch -- it
 *          uses only long-standing executor hooks -- but it costs a planstate
 *          tree walk per sampled execution.  The walk is gated by the caller
 *          on there being an active recording span (otel_trace.c only calls
 *          otel_fdw_executor_start when span_active), so unsampled queries
 *          pay nothing.
 *
 * Timing note for the PG18 walk (why _start runs after, _end runs before)
 * ----------------------------------------------------------------------
 * The core hooks fire *inside* standard_ExecutorStart / standard_ExecutorEnd
 * (within ExecInitForeignScan / ExecEndForeignScan).  To reproduce those
 * boundaries from the surrounding executor hooks:
 *
 *   - otel_fdw_executor_start() must run AFTER standard_ExecutorStart, once
 *     queryDesc->planstate is built and ss_currentRelation is populated.  The
 *     enclosing pgsql.execute span is already on the producer active stack by
 *     then (otel_trace.c starts it before chaining), so our spans nest under
 *     it correctly.
 *
 *   - otel_fdw_executor_end() must run BEFORE standard_ExecutorEnd, while the
 *     ForeignScanState nodes still exist (ExecEndForeignScan frees them), and
 *     before the parent pgsql.execute span is finalized (otherwise the
 *     producer would warn about emitting a parent with children still on the
 *     stack and drop them).
 *
 * otel_trace.c honours both by calling _start at the tail of its
 * ExecutorStart hook and _end at the head of its ExecutorEnd hook.
 *
 * A single ForeignScan node is ExecInit'd once regardless of how many times
 * it is rescanned (nested-loop inner side, etc.), so one tree walk per
 * execution matches the core hooks' once-per-node semantics; rescans are not
 * separately spanned by either strategy.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/otel_postgres_tracing/otel_fdw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

#include <otel_api/otel.h>
#include "otel_postgres_tracing.h"
#include "otel_fdw.h"

/*
 * PG19+ exposes the ForeignScan core hooks; PG18 does not.  Everything
 * version-specific in this file keys off OTEL_FDW_USE_CORE_HOOKS.  The PG19+
 * path requires the otel core patch that adds these hooks (the extension is
 * only ever built against the patched core).
 */
#if PG_VERSION_NUM >= 190000
#define OTEL_FDW_USE_CORE_HOOKS 1
#include "executor/nodeForeignscan.h"
#else
#include "nodes/nodeFuncs.h"		/* planstate_tree_walker */
#endif

/*
 * Stack of in-flight pg.fdw.scan spans; fdw_scan_depth tracks how many are
 * active.  Multiple ForeignScan nodes can be open simultaneously (nested-loop
 * join over a foreign table, async FDW, etc.), and ends can be non-LIFO, so
 * entries are keyed by the ForeignScanState pointer and matched on end.
 *
 * subxact_level records the transaction nesting level at push time so the
 * SubXactCallback path can drop entries belonging to an aborting
 * subtransaction without touching entries from outer (still-live) ones.
 */
#define OTEL_FDW_SCAN_STACK_MAX 16
static struct
{
	ForeignScanState *node;		/* key: the ForeignScanState pointer */
	OtelSpan	span;			/* OTEL_UNWIND_DROP -> static storage ok */
	int			subxact_level;	/* GetCurrentTransactionNestLevel() at push */
} fdw_scan_stack[OTEL_FDW_SCAN_STACK_MAX];
static int	fdw_scan_depth = 0;


/*
 * Open a pg.fdw.scan span for one ForeignScanState and push it onto both the
 * producer active stack (so it nests under the current span) and our tracking
 * stack (so the matching end can find it).  No-op when the provider is absent
 * or the stack is full.
 */
static void
otel_fdw_scan_begin(ForeignScanState *node)
{
	const OtelTracingApi *api = otel_pg_ensure();
	OtelSpan   *span;

	if (api == NULL || fdw_scan_depth >= OTEL_FDW_SCAN_STACK_MAX)
		return;

	span = &fdw_scan_stack[fdw_scan_depth].span;
	fdw_scan_stack[fdw_scan_depth].node = node;
	fdw_scan_stack[fdw_scan_depth].subxact_level = GetCurrentTransactionNestLevel();

	api->span_init(span, otel_pg_tracer, "pg.fdw.scan", OTEL_SPAN_KIND_CLIENT);
	api->span_add_attribute_string(span, "db.system", "postgresql");

	if (node->ss.ss_currentRelation != NULL)
		api->span_add_attribute_string(span, "db.collection.name",
									   RelationGetRelationName(node->ss.ss_currentRelation));

	api->span_link_to_active_and_push(span);
	fdw_scan_depth++;
}

/*
 * Locate the tracking entry for one ForeignScanState (end may be non-LIFO
 * with async FDW), emit its span, then compact the stack.  Uses
 * otel_api_get() rather than otel_pg_ensure() because we are only emitting an
 * already-initialised span, not registering a new tracer scope.
 */
static void
otel_fdw_scan_end(ForeignScanState *node)
{
	const OtelTracingApi *api = otel_api_get();
	int			i;

	for (i = fdw_scan_depth - 1; i >= 0; i--)
	{
		if (fdw_scan_stack[i].node == node)
		{
			if (api != NULL)
			{
				fdw_scan_stack[i].span.end_time = GetCurrentTimestamp();
				api->span_emit(&fdw_scan_stack[i].span);
			}

			/* Shift remaining entries down to fill the gap. */
			for (; i < fdw_scan_depth - 1; i++)
				fdw_scan_stack[i] = fdw_scan_stack[i + 1];
			fdw_scan_depth--;
			return;
		}
	}
	/* Not found: already unwound via MemoryContext callback on error. */
}


#ifdef OTEL_FDW_USE_CORE_HOOKS

/* ---- PG19+ : core ForeignScan hooks -------------------------------- */

static ForeignScanBegin_hook_type prev_ForeignScanBegin_hook = NULL;
static ForeignScanEnd_hook_type prev_ForeignScanEnd_hook = NULL;

static void
otel_ForeignScanBegin(ForeignScanState *node, int eflags)
{
	otel_fdw_scan_begin(node);

	if (prev_ForeignScanBegin_hook)
		prev_ForeignScanBegin_hook(node, eflags);
}

static void
otel_ForeignScanEnd(ForeignScanState *node)
{
	if (prev_ForeignScanEnd_hook)
		prev_ForeignScanEnd_hook(node);

	otel_fdw_scan_end(node);
}

void
otel_fdw_install_hooks(void)
{
	prev_ForeignScanBegin_hook = ForeignScanBegin_hook;
	ForeignScanBegin_hook = otel_ForeignScanBegin;
	prev_ForeignScanEnd_hook = ForeignScanEnd_hook;
	ForeignScanEnd_hook = otel_ForeignScanEnd;
}

void
otel_fdw_executor_start(QueryDesc *queryDesc)
{
	/* Spans are opened by the core ForeignScanBegin hook. */
}

void
otel_fdw_executor_end(QueryDesc *queryDesc)
{
	/* Spans are emitted by the core ForeignScanEnd hook. */
}

#else							/* !OTEL_FDW_USE_CORE_HOOKS */

/* ---- PG18 : planstate tree walk driven from the executor hooks ----- */

static bool
otel_fdw_start_walker(PlanState *planstate, void *context)
{
	if (planstate == NULL)
		return false;

	if (IsA(planstate, ForeignScanState))
		otel_fdw_scan_begin((ForeignScanState *) planstate);

	return planstate_tree_walker(planstate, otel_fdw_start_walker, context);
}

static bool
otel_fdw_end_walker(PlanState *planstate, void *context)
{
	if (planstate == NULL)
		return false;

	if (IsA(planstate, ForeignScanState))
		otel_fdw_scan_end((ForeignScanState *) planstate);

	return planstate_tree_walker(planstate, otel_fdw_end_walker, context);
}

void
otel_fdw_install_hooks(void)
{
	/* Nothing to install: driven from otel_trace.c's executor hooks. */
}

void
otel_fdw_executor_start(QueryDesc *queryDesc)
{
	if (queryDesc == NULL || queryDesc->planstate == NULL)
		return;

	/* Process the root too, hence walk from the root node itself. */
	(void) otel_fdw_start_walker(queryDesc->planstate, NULL);
}

void
otel_fdw_executor_end(QueryDesc *queryDesc)
{
	/* Cheap gate: nothing open means no foreign scans this execution. */
	if (fdw_scan_depth == 0 || queryDesc == NULL || queryDesc->planstate == NULL)
		return;

	(void) otel_fdw_end_walker(queryDesc->planstate, NULL);
}

#endif							/* OTEL_FDW_USE_CORE_HOOKS */


/*
 * SubXactCallback path --- clean up fdw_scan_stack[] when a subtransaction
 * aborts.  Applies to BOTH strategies.
 *
 * Neither ExecEndForeignScan (PG19 hook) nor the ExecutorEnd-driven walk
 * (PG18) runs on subtransaction abort; the executor tears down resources via
 * MemoryContext reset instead.  The producer's own span_stack[] is correctly
 * maintained by on_memory_context_reset callbacks in otel_producer.c, but
 * fdw_scan_stack[] has no such mechanism.  Without this, repeated ROLLBACK TO
 * SAVEPOINT cycles over foreign scans would ratchet fdw_scan_depth toward
 * OTEL_FDW_SCAN_STACK_MAX, after which otel_fdw_scan_begin silently stops
 * creating spans; stale node pointers also risk an ABA false-match in
 * otel_fdw_scan_end.
 *
 * On SUBXACT_EVENT_ABORT_SUB we drop all entries pushed at the aborting
 * nesting level or deeper.  Entries from enclosing subtransactions / the
 * top-level transaction are preserved because their ForeignScan nodes are
 * still alive and will be closed normally.
 */
void
otel_fdw_subxact_abort(SubXactEvent event)
{
	int			current_level;
	int			new_depth;
	int			i;

	if (event != SUBXACT_EVENT_ABORT_SUB || fdw_scan_depth == 0)
		return;

	/*
	 * During SUBXACT_EVENT_ABORT_SUB, GetCurrentTransactionNestLevel()
	 * returns the level of the subtransaction that is aborting.  Any entry
	 * pushed at that level (or deeper, for savepoints nested within it)
	 * belongs to code that is being rolled back and must be discarded.  The
	 * producer has already popped those spans via on_memory_context_reset.
	 */
	current_level = GetCurrentTransactionNestLevel();
	new_depth = 0;

	for (i = 0; i < fdw_scan_depth; i++)
	{
		if (fdw_scan_stack[i].subxact_level < current_level)
		{
			/* Pushed in an enclosing subxact; still live. */
			if (new_depth != i)
				fdw_scan_stack[new_depth] = fdw_scan_stack[i];
			new_depth++;
		}
		/* else: belongs to the aborting subxact; producer already dropped it. */
	}
	fdw_scan_depth = new_depth;
}

/*
 * Reset on top-level transaction abort.  The FDW scan spans are dropped by
 * the otel_api MemoryContext callbacks during error unwind; we just clear our
 * depth counter so the next transaction starts clean.  Applies to both
 * strategies.
 */
void
otel_fdw_reset(void)
{
	fdw_scan_depth = 0;
}
