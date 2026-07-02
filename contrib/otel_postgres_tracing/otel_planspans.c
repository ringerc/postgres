/*-------------------------------------------------------------------------
 *
 * otel_planspans.c
 *	  Curated child spans for a few "interesting" plan-node types.
 *
 * A collector registered with the otel_planwalk dispatcher.  During the
 * single planstate_tree_walker pass it opens a child span (on node_begin,
 * during the start walk) and closes it (on node_end, during the end walk)
 * for a curated set of node kinds:
 *
 *   CustomScanState     -> pg.customscan   (+ pg.customscan.method)
 *   FunctionScanState   -> pg.funcscan
 *   TableFuncScanState  -> pg.tablefuncscan
 *   GatherState         -> pg.parallel.gather  (+ workers planned/launched)
 *   GatherMergeState    -> pg.parallel.gather  (+ workers planned/launched)
 *
 * The span nests under the active pgsql.execute span (span_link_to_active_
 * and_push).  This is deliberately a SMALL curated set -- spanning every plan
 * node would blow up span volume and downstream cardinality for no diagnostic
 * value (see otel-planstate-walker-proposals.md).
 *
 * The span machinery is the same as otel_fdw.c: a fixed-depth stack keyed by
 * the PlanState pointer so ends can be matched non-LIFO, plus (sub)xact-abort
 * cleanup (planspans_subxact_abort / planspans_reset).  Spans use
 * OTEL_UNWIND_DROP (span_init default) so static storage is safe.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/otel_postgres_tracing/otel_planspans.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "nodes/plannodes.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include <otel_api/otel.h>
#include <otel_api/otel_api.h>
#include "otel_postgres_tracing.h"
#include "otel_planwalk.h"
#include "otel_planspans.h"

/* GUC backing variable; defined in otel_planspans_install(). */
bool		otel_trace_plan_child_spans = false;

/*
 * Stack of in-flight curated child spans, keyed by PlanState pointer.  Ends
 * can be non-LIFO (interleaved subplans), so entries are matched on the node
 * pointer.  subxact_level records the transaction nesting level at push time
 * for the SubXactCallback cleanup path.  Same shape as fdw_scan_stack[].
 */
#define OTEL_PLANSPANS_STACK_MAX 16
static struct
{
	PlanState  *node;			/* key: the PlanState pointer */
	OtelSpan	span;			/* OTEL_UNWIND_DROP -> static storage ok */
	int			subxact_level;	/* GetCurrentTransactionNestLevel() at push */
} planspans_stack[OTEL_PLANSPANS_STACK_MAX];
static int	planspans_depth = 0;

/*
 * Return the span name for a curated node kind, or NULL if this node is not
 * one we span.  Also the cheap membership test used on both begin and end.
 */
static const char *
planspans_span_name(PlanState *ps)
{
	switch (nodeTag(ps))
	{
		case T_CustomScanState:
			return "pg.customscan";
		case T_FunctionScanState:
			return "pg.funcscan";
		case T_TableFuncScanState:
			return "pg.tablefuncscan";
		case T_GatherState:
		case T_GatherMergeState:
			return "pg.parallel.gather";
		default:
			return NULL;
	}
}

/*
 * node_begin: open a child span for a curated node and push it onto both the
 * producer active stack (so it nests under pgsql.execute) and our tracking
 * stack.  Structural attributes available at init time are added here; runtime
 * attributes (parallel worker counts) are added at node_end.
 */
static void
planspans_node_begin(PlanState *ps, OtelPlanwalkContext *ctx)
{
	const OtelTracingApi *api;
	const char *name = planspans_span_name(ps);
	OtelSpan   *span;

	(void) ctx;

	if (name == NULL)
		return;

	api = otel_pg_ensure();
	if (api == NULL || planspans_depth >= OTEL_PLANSPANS_STACK_MAX)
		return;

	span = &planspans_stack[planspans_depth].span;
	planspans_stack[planspans_depth].node = ps;
	planspans_stack[planspans_depth].subxact_level = GetCurrentTransactionNestLevel();

	api->span_init(span, otel_pg_tracer, name, OTEL_SPAN_KIND_INTERNAL);

	if (IsA(ps, CustomScanState))
	{
		/*
		 * methods->CustomName points into the CustomExecMethods registered by
		 * the extension; it lives for the backend's lifetime, so no copy.
		 */
		const char *cname = ((CustomScanState *) ps)->methods->CustomName;

		if (cname != NULL)
			api->span_add_attribute_string(span, "pg.customscan.method", cname);
	}

	api->span_link_to_active_and_push(span);
	planspans_depth++;
}

/*
 * node_end: locate the tracking entry for this node (non-LIFO), add any
 * runtime attributes, emit the span, and compact the stack.
 */
static void
planspans_node_end(PlanState *ps, OtelPlanwalkContext *ctx)
{
	const OtelTracingApi *api = otel_api_get();
	int			i;

	if (planspans_span_name(ps) == NULL || planspans_depth == 0)
		return;

	for (i = planspans_depth - 1; i >= 0; i--)
	{
		if (planspans_stack[i].node == ps)
		{
			OtelSpan   *span = &planspans_stack[i].span;

			if (api != NULL)
			{
				/*
				 * Parallel worker counts are only known after execution, so
				 * add them here (not at begin).  Emit even when launched ==
				 * planned so a healthy parallel region is still visible.
				 */
				if (IsA(ps, GatherState) || IsA(ps, GatherMergeState))
				{
					int			planned;
					int			launched;
					MemoryContext old;

					if (IsA(ps, GatherState))
					{
						launched = ((GatherState *) ps)->nworkers_launched;
						planned = ((Gather *) ps->plan)->num_workers;
					}
					else
					{
						launched = ((GatherMergeState *) ps)->nworkers_launched;
						planned = ((GatherMerge *) ps->plan)->num_workers;
					}

					old = MemoryContextSwitchTo(ctx->attr_cxt);
					api->span_add_attribute_string(span, "pg.parallel.workers_planned",
												   psprintf("%d", planned)); /* TODO(native-attr): emit as int64 once the API grows typed attribute setters */
					api->span_add_attribute_string(span, "pg.parallel.workers_launched",
												   psprintf("%d", launched)); /* TODO(native-attr): emit as int64 once the API grows typed attribute setters */
					MemoryContextSwitchTo(old);
				}

				span->end_time = GetCurrentTimestamp();
				api->span_emit(span);
			}

			/* Shift remaining entries down to fill the gap. */
			for (; i < planspans_depth - 1; i++)
				planspans_stack[i] = planspans_stack[i + 1];
			planspans_depth--;
			return;
		}
	}
	/* Not found: already unwound via MemoryContext callback on error. */
}

static bool
planspans_enabled(void)
{
	return otel_trace_plan_child_spans;
}

static const OtelPlanwalkCollector planspans_collector = {
	planspans_enabled,
	planspans_node_begin,
	planspans_node_end,
	NULL,						/* end_walk_begin */
	NULL,						/* end_walk_end */
};

void
otel_planspans_install(void)
{
	DefineCustomBoolVariable("otel.trace_plan_child_spans",
							 "Emit child spans for a curated set of plan node types.",
							 "When on, sampled queries get pg.customscan / pg.funcscan / "
							 "pg.tablefuncscan / pg.parallel.gather child spans nested under "
							 "pgsql.execute.  Structural (no per-node timing); off by default.",
							 &otel_trace_plan_child_spans,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	otel_planwalk_register_collector(&planspans_collector);
}

/*
 * SubXactCallback path: drop stack entries pushed at or below the aborting
 * subtransaction level.  Exact mirror of otel_fdw_subxact_abort -- the
 * producer has already popped those spans via on_memory_context_reset; we
 * just keep planspans_stack[] consistent so depth doesn't ratchet and stale
 * node pointers can't ABA-false-match.
 */
void
otel_planspans_subxact_abort(SubXactEvent event)
{
	int			current_level;
	int			new_depth;
	int			i;

	if (event != SUBXACT_EVENT_ABORT_SUB || planspans_depth == 0)
		return;

	current_level = GetCurrentTransactionNestLevel();
	new_depth = 0;

	for (i = 0; i < planspans_depth; i++)
	{
		if (planspans_stack[i].subxact_level < current_level)
		{
			/* Pushed in an enclosing subxact; still live. */
			if (new_depth != i)
				planspans_stack[new_depth] = planspans_stack[i];
			new_depth++;
		}
		/* else: belongs to the aborting subxact; producer already dropped it. */
	}
	planspans_depth = new_depth;
}

/*
 * Reset on top-level transaction abort.  The spans are dropped by the otel_api
 * MemoryContext callbacks during error unwind; we just clear our depth counter.
 */
void
otel_planspans_reset(void)
{
	planspans_depth = 0;
}
