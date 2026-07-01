/*-------------------------------------------------------------------------
 *
 * otel_planwalk.c
 *	  Single-pass planstate-tree-walker dispatcher for OpenTelemetry
 *	  instrumentation collectors.
 *
 * Provides a central registration point for walk-based feature collectors
 * and drives a single planstate_tree_walker per ExecutorStart / ExecutorEnd.
 * Each registered collector that reports enabled() = true has its
 * node_begin / node_end callback invoked once per PlanState node.
 *
 * See otel_planwalk.h for the public interface and usage contract.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/otel_postgres_tracing/otel_planwalk.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "utils/elog.h"
#include "utils/memutils.h"

#include <otel_api/otel.h>
#include "otel_postgres_tracing.h"
#include "otel_planwalk.h"

/*
 * GUC: otel.trace_plan_node_stats (group-A feature gate).
 * Registered in otel_postgres_tracing.c _PG_init before MarkGUCPrefixReserved.
 * Backing variable defined here; declared extern in otel_planwalk.h.
 */
bool		otel_trace_plan_node_stats = false;

/*
 * Static registry of collectors.  Fixed size — the set of collectors is
 * compile-time known (one per feature in steps 1-5).  Overflow is caught
 * at registration time with a clear error.
 */
static const OtelPlanwalkCollector *collectors[OTEL_PLANWALK_MAX_COLLECTORS];
static int	n_collectors = 0;

/*
 * Per-walk state: bitmask of which collector indices are enabled for the
 * current walk.  Built at the start of each walk to avoid repeated enabled()
 * calls inside the tree traversal.
 */
#define COLLECTOR_ENABLED_MASK(i)	(1u << (unsigned)(i))

/*
 * Internal context threaded through planstate_tree_walker.
 *
 * The public OtelPlanwalkContext carries the fields collectors need.
 * We wrap it in a private struct that also carries the per-walk enabled mask
 * and a flag distinguishing begin from end walks so one walker function
 * can serve both.
 */
typedef struct
{
	OtelPlanwalkContext pub;		/* must be first; passed to collector cbs */
	unsigned int enabled_mask;	/* bitmask of collectors active this walk */
	bool		is_end_walk;	/* true -> call node_end, false -> node_begin */
} PlanwalkInternalCtx;

/*
 * The actual planstate_tree_walker callback.  Processes the current node
 * then recurses into children.  Mirrors otel_fdw.c's walker convention:
 * bail on NULL, call all enabled collectors, then recurse.
 */
static bool
otel_planwalk_walker(PlanState *planstate, void *context)
{
	PlanwalkInternalCtx *ictx = (PlanwalkInternalCtx *) context;
	int			i;

	if (planstate == NULL)
		return false;

	/* Dispatch to enabled collectors for this node. */
	for (i = 0; i < n_collectors; i++)
	{
		if (!(ictx->enabled_mask & COLLECTOR_ENABLED_MASK(i)))
			continue;

		if (ictx->is_end_walk)
		{
			if (collectors[i]->node_end != NULL)
				collectors[i]->node_end(planstate, &ictx->pub);
		}
		else
		{
			if (collectors[i]->node_begin != NULL)
				collectors[i]->node_begin(planstate, &ictx->pub);
		}
	}

	return planstate_tree_walker(planstate, otel_planwalk_walker, context);
}

/*
 * Build the enabled mask for the current walk by calling each collector's
 * enabled() function.  Returns the mask; zero means no collector is active.
 */
static unsigned int
build_enabled_mask(void)
{
	unsigned int mask = 0;
	int			i;

	for (i = 0; i < n_collectors; i++)
	{
		/* enabled must be non-NULL per the contract in the header. */
		if (collectors[i]->enabled())
			mask |= COLLECTOR_ENABLED_MASK(i);
	}
	return mask;
}


/* -------------------------------------------------------------------------
 * Public API
 * -------------------------------------------------------------------------
 */

void
otel_planwalk_install(void)
{
	/* Reserved for future initialization.  No-op for now. */
}

void
otel_planwalk_register_collector(const OtelPlanwalkCollector *c)
{
	if (n_collectors >= OTEL_PLANWALK_MAX_COLLECTORS)
		elog(ERROR, "otel_planwalk: collector registry full (max %d)",
			 OTEL_PLANWALK_MAX_COLLECTORS);

	Assert(c != NULL);
	Assert(c->enabled != NULL);

	collectors[n_collectors++] = c;
}

void
otel_planwalk_executor_start(QueryDesc *queryDesc, OtelSpan *stmt_span,
							 MemoryContext attr_cxt)
{
	PlanwalkInternalCtx ictx;
	unsigned int mask;

	if (queryDesc == NULL || queryDesc->planstate == NULL)
		return;

	/* Build enabled mask; bail early when no collector wants this walk. */
	mask = build_enabled_mask();
	if (mask == 0)
		return;

	ictx.pub.queryDesc = queryDesc;
	ictx.pub.stmt_span = stmt_span;
	ictx.pub.attr_cxt = attr_cxt;
	ictx.enabled_mask = mask;
	ictx.is_end_walk = false;

	/* Process the root node too, then recurse; mirrors otel_fdw.c:262. */
	(void) otel_planwalk_walker(queryDesc->planstate, &ictx);
}

void
otel_planwalk_executor_end(QueryDesc *queryDesc, OtelSpan *stmt_span,
						   MemoryContext attr_cxt)
{
	PlanwalkInternalCtx ictx;
	unsigned int mask;
	int			i;

	if (queryDesc == NULL || queryDesc->planstate == NULL)
		return;

	/* Build enabled mask; bail early when no collector wants this walk. */
	mask = build_enabled_mask();
	if (mask == 0)
		return;

	ictx.pub.queryDesc = queryDesc;
	ictx.pub.stmt_span = stmt_span;
	ictx.pub.attr_cxt = attr_cxt;
	ictx.enabled_mask = mask;
	ictx.is_end_walk = true;

	/* Framing: reset accumulators before the traversal. */
	for (i = 0; i < n_collectors; i++)
	{
		if ((mask & COLLECTOR_ENABLED_MASK(i)) &&
			collectors[i]->end_walk_begin != NULL)
			collectors[i]->end_walk_begin(&ictx.pub);
	}

	(void) otel_planwalk_walker(queryDesc->planstate, &ictx);

	/* Framing: fold aggregated attributes onto stmt_span after the traversal. */
	for (i = 0; i < n_collectors; i++)
	{
		if ((mask & COLLECTOR_ENABLED_MASK(i)) &&
			collectors[i]->end_walk_end != NULL)
			collectors[i]->end_walk_end(&ictx.pub);
	}
}

bool
otel_planwalk_want_instrumentation(void)
{
	return otel_trace_plan_node_stats;
}
