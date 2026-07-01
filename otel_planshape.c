/*-------------------------------------------------------------------------
 *
 * otel_planshape.c
 *	  Plan-shape capture at plan time (proposal 4).
 *
 * Walks the Plan tree of a sampled query at ExecutorStart (before execution)
 * and folds two things onto the enclosing pgsql.execute span:
 *
 *   pg.plan.shape_hash  -- a stable digest of the plan's structure (pre-order
 *                          sequence of node tags + join types).  Cost/row
 *                          estimates are deliberately excluded so the hash
 *                          changes only when the plan *shape* changes, making
 *                          it a signal for "this statement replanned
 *                          differently" (plan regression / instability).
 *   pg.plan.node_count  -- number of Plan nodes (structural, not the
 *                          execution-time node count in otel_planpath.c).
 *   pg.plan.risks       -- comma-separated structural-risk tokens, emitted
 *                          only when at least one fires:
 *                            seqscan_large        -- a SeqScan estimated to
 *                                                    return many rows (proxy
 *                                                    for a possibly-missing
 *                                                    index / expensive scan)
 *                            nestloop_high_inner  -- a NestLoop whose inner
 *                                                    side estimates many rows
 *                                                    (risk of blowup)
 *
 * This is a separate walker from otel_planwalk.c: it traverses Plan nodes, not
 * PlanState nodes, and runs before execution, so it is not a dispatcher
 * collector.  It needs no instrument_options.  Thresholds are hardcoded for
 * now (see the *_ROWS constants); make them GUCs if demand appears.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/otel_postgres_tracing/otel_planshape.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/hashfn.h"
#include "lib/stringinfo.h"
#include "nodes/plannodes.h"
#include "nodes/pg_list.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#include <otel_api/otel.h>
#include <otel_api/otel_api.h>
#include "otel_postgres_tracing.h"
#include "otel_planshape.h"

/* GUC backing variable; defined in otel_planshape_install(). */
bool		otel_trace_plan_shape = false;

/*
 * Structural-risk thresholds (estimated rows).  Hardcoded for v1; the whole
 * point is a coarse "this plan looks risky" flag, not a tuned optimizer.
 */
#define OTEL_PLANSHAPE_SEQSCAN_LARGE_ROWS	100000.0
#define OTEL_PLANSHAPE_NESTLOOP_INNER_ROWS	10000.0

typedef struct PlanShapeCtx
{
	uint32		digest;			/* running plan-shape hash */
	int			node_count;		/* number of Plan nodes visited */
	bool		risk_seqscan_large;
	bool		risk_nestloop_high_inner;
} PlanShapeCtx;

/*
 * Per-node contribution to the shape digest: the node tag, plus the join type
 * for join nodes (so a nestloop->hashjoin swap changes the hash even if the
 * tag position is otherwise similar).  Row/cost estimates are intentionally
 * NOT hashed.
 */
static uint32
planshape_node_token(Plan *plan)
{
	uint32		t = (uint32) nodeTag(plan);

	switch (nodeTag(plan))
	{
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
			/* All three embed Join as their first member. */
			t = hash_combine(t, (uint32) ((Join *) plan)->jointype);
			break;
		default:
			break;
	}
	return t;
}

/* Recurse over a list of child Plan nodes. */
static void planshape_walk(Plan *plan, PlanShapeCtx *ctx);

static void
planshape_walk_list(List *plans, PlanShapeCtx *ctx)
{
	ListCell   *lc;

	foreach(lc, plans)
		planshape_walk((Plan *) lfirst(lc), ctx);
}

/*
 * Pre-order walk of the Plan tree.  Handles the standard lefttree/righttree
 * plus the node kinds that carry child plans in list/pointer fields.
 * Correlated subplans are not reached via lefttree/righttree; the caller walks
 * PlannedStmt->subplans separately.
 */
static void
planshape_walk(Plan *plan, PlanShapeCtx *ctx)
{
	if (plan == NULL)
		return;

	ctx->node_count++;
	ctx->digest = hash_combine(ctx->digest, planshape_node_token(plan));

	/* Structural-risk heuristics. */
	if (IsA(plan, SeqScan) &&
		plan->plan_rows > OTEL_PLANSHAPE_SEQSCAN_LARGE_ROWS)
		ctx->risk_seqscan_large = true;

	if (IsA(plan, NestLoop) && plan->righttree != NULL &&
		plan->righttree->plan_rows > OTEL_PLANSHAPE_NESTLOOP_INNER_ROWS)
		ctx->risk_nestloop_high_inner = true;

	/* Standard binary children. */
	planshape_walk(plan->lefttree, ctx);
	planshape_walk(plan->righttree, ctx);

	/* Node-specific child plans. */
	switch (nodeTag(plan))
	{
		case T_Append:
			planshape_walk_list(((Append *) plan)->appendplans, ctx);
			break;
		case T_MergeAppend:
			planshape_walk_list(((MergeAppend *) plan)->mergeplans, ctx);
			break;
		case T_BitmapAnd:
			planshape_walk_list(((BitmapAnd *) plan)->bitmapplans, ctx);
			break;
		case T_BitmapOr:
			planshape_walk_list(((BitmapOr *) plan)->bitmapplans, ctx);
			break;
		case T_SubqueryScan:
			planshape_walk(((SubqueryScan *) plan)->subplan, ctx);
			break;
		default:
			break;
	}
}

void
otel_planshape_executor_start(QueryDesc *queryDesc, OtelSpan *stmt_span,
							  MemoryContext attr_cxt)
{
	const OtelTracingApi *api;
	PlannedStmt *pstmt;
	PlanShapeCtx ctx;
	MemoryContext old;
	char	   *v;

	if (!otel_trace_plan_shape)
		return;

	if (queryDesc == NULL || queryDesc->plannedstmt == NULL || stmt_span == NULL)
		return;

	api = otel_api_get();
	if (api == NULL)
		return;

	pstmt = queryDesc->plannedstmt;

	memset(&ctx, 0, sizeof(ctx));

	/* Walk the main plan tree and any (correlated / init) subplans. */
	planshape_walk(pstmt->planTree, &ctx);
	planshape_walk_list(pstmt->subplans, &ctx);

	old = MemoryContextSwitchTo(attr_cxt);

	/* Shape hash: an identity string, not a numeric measure. */
	v = psprintf("%08x", ctx.digest);
	api->span_add_attribute_string(stmt_span, "pg.plan.shape_hash", v);

	v = psprintf("%d", ctx.node_count);
	api->span_add_attribute_string(stmt_span, "pg.plan.node_count",
								   v); /* TODO(native-attr): emit as int64 once the API grows typed attribute setters */

	if (ctx.risk_seqscan_large || ctx.risk_nestloop_high_inner)
	{
		StringInfoData risks;

		initStringInfo(&risks);
		if (ctx.risk_seqscan_large)
			appendStringInfoString(&risks, "seqscan_large");
		if (ctx.risk_nestloop_high_inner)
			appendStringInfo(&risks, "%snestloop_high_inner",
							 risks.len > 0 ? "," : "");
		/* risks.data is already in attr_cxt. */
		api->span_add_attribute_string(stmt_span, "pg.plan.risks", risks.data);
	}

	MemoryContextSwitchTo(old);
}

void
otel_planshape_install(void)
{
	DefineCustomBoolVariable("otel.trace_plan_shape",
							 "Stamp sampled queries with a plan-shape digest and structural-risk flags.",
							 "When on, sampled queries get pg.plan.shape_hash (a structure-only "
							 "digest for plan-regression detection), pg.plan.node_count, and "
							 "pg.plan.risks on the pgsql.execute span.  Plan-time only, no per-node "
							 "instrumentation; off by default.",
							 &otel_trace_plan_shape,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);
}
