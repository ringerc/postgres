/*-------------------------------------------------------------------------
 *
 * otel_planpath.c
 *	  Pathology-flags (step 3) + compact-actuals (step 4) collector for the
 *	  planstate-tree-walker dispatcher.
 *
 * One collector, one end-walk pass.  All attributes fold onto the enclosing
 * pgsql.execute span; no child spans are created here.
 *
 * Step 3 — pathology attributes (emitted only when condition holds):
 *   pg.exec.spilled            "true" when any node spilled to disk
 *   pg.exec.spill_kb           total KB spilled (Sort disk + Hash batches
 *                              space_peak/batch + HashAgg disk + tuplestore)
 *   pg.parallel.workers_planned  planned num_workers on shortfall Gather node
 *   pg.parallel.workers_launched launched workers on same node
 *   pg.exec.misestimate        "true" when worst row-estimate ratio > 10x
 *   pg.exec.misestimate_ratio  worst ratio, 1-decimal (e.g. "42.5")
 *   pg.exec.misestimate_node   node tag of the worst-offending node
 *   pg.exec.bitmap_lossy_pages lossy page count from BitmapHeapScan (> 0)
 *
 * Step 4 — compact-actuals attributes (sum across all instrumented nodes):
 *   pg.exec.buffers_read       sum shared_blks_read
 *   pg.exec.buffers_hit        sum shared_blks_hit
 *   pg.exec.buffers_dirtied    sum shared_blks_dirtied
 *   pg.exec.node_count         count of visited PlanState nodes
 *   pg.exec.slowest_nodes      "tag:ms,tag:ms,tag:ms" top-3 by actual total
 *
 * All numeric/boolean values are formatted as strings (psprintf into attr_cxt)
 * per the string-only attribute model; every such call carries a
 * TODO(native-attr) comment marking the conversion site.
 *
 * Field-access conventions:
 *   - planstate->instrument is NodeInstrumentation* (not Instrumentation*).
 *     Call InstrEndLoop(planstate->instrument) before reading counters,
 *     exactly as explain.c's ExplainNode does at line ~1839.
 *   - Timing: INSTR_TIME_GET_MILLISEC(instr->instr.total) / instr->nloops
 *     (note the nested .instr.total inside NodeInstrumentation; startup and
 *     ntuples/nloops are top-level NodeInstrumentation fields).
 *   - Sort spill: SortState.sort_Done + tuplesort_get_stats(); spaceType ==
 *     SORT_SPACE_TYPE_DISK means disk; spaceUsed is already in kB.
 *   - Hash spill: HashState.hinstrument->nbatch > 1; space_peak is bytes.
 *     We report space_peak/nbatch as an estimate (peak/batch not total kB).
 *   - HashAgg spill: AggState.hash_batches_used > 1; hash_disk_used is kB.
 *   - Material/WindowAgg spill: tuplestore_get_stats(); maxSpace is bytes;
 *     "Disk" string indicates spill.
 *   - Gather/GatherMerge shortfall: nworkers_launched < plan->num_workers.
 *   - BitmapHeapScan lossy: BitmapHeapScanState.stats.lossy_pages > 0.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/otel_postgres_tracing/otel_planpath.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "executor/instrument.h"
#include "executor/instrument_node.h"
#include "nodes/execnodes.h"
#include "nodes/nodes.h"
#include "nodes/plannodes.h"
#include "utils/memutils.h"
#include "utils/tuplesort.h"
#include "utils/tuplestore.h"

#include <otel_api/otel_api.h>
#include "otel_planpath.h"
#include "otel_planwalk.h"

/* -------------------------------------------------------------------------
 * Local constants
 * -------------------------------------------------------------------------
 */

/* Threshold for flagging a bad row estimate. */
#define MISESTIMATE_THRESHOLD	10.0

/* Number of "slowest nodes" slots to track (step 4). */
#define N_SLOWEST	3

/* Convenience: convert bytes to kibibytes (rounding up), same as explain.c. */
#define BYTES_TO_KB(b)	(((int64)(b) + 1023) / 1024)

/* -------------------------------------------------------------------------
 * Per-query accumulator (file-static; reset in end_walk_begin)
 *
 * One backend processes one query at a time, so a file-static accumulator is
 * safe, following the otel_fdw.c precedent.
 * -------------------------------------------------------------------------
 */

typedef struct SlowNode
{
	double		total_ms;		/* total actual time in ms */
	const char *tag;			/* node tag string literal */
} SlowNode;

typedef struct PlanpathAccum
{
	/* Step 3: spill */
	bool		any_spill;
	int64		spill_kb;

	/* Step 3: parallel shortfall (first Gather/GatherMerge shortfall found) */
	bool		have_shortfall;
	int			shortfall_planned;
	int			shortfall_launched;

	/* Step 3: misestimate (worst ratio seen; flag derived as > threshold) */
	double		worst_ratio;
	const char *worst_node_tag;	/* string literal from nodetag_name() */

	/* Step 3: lossy bitmap pages */
	uint64		bitmap_lossy_pages;

	/* Step 4: aggregate buffer counts */
	int64		buffers_read;
	int64		buffers_hit;
	int64		buffers_dirtied;

	/* Step 4: node count */
	int			node_count;

	/* Step 4: top-3 slowest nodes (insertion-sorted by total_ms desc) */
	SlowNode	slowest[N_SLOWEST];
} PlanpathAccum;

static PlanpathAccum accum;

/* -------------------------------------------------------------------------
 * Helpers
 * -------------------------------------------------------------------------
 */

/*
 * Return a stable string-literal name for a PlanState node tag.
 * Only the node types we inspect need precise names; all others get the
 * generic tag number rendered as a literal (handled by the default below via
 * a small on-stack buffer -- but for the nodes we actually visit we return
 * a string literal directly so no allocation is needed on the hot path).
 *
 * Because this is called only from node_end (single-threaded backend), the
 * fallback static buffer is safe.
 */
static const char *
nodetag_name(NodeTag tag)
{
	switch (tag)
	{
		case T_SeqScanState:		return "SeqScan";
		case T_IndexScanState:		return "IndexScan";
		case T_IndexOnlyScanState:	return "IndexOnlyScan";
		case T_BitmapIndexScanState:	return "BitmapIndexScan";
		case T_BitmapHeapScanState:	return "BitmapHeapScan";
		case T_TidScanState:		return "TidScan";
		case T_TidRangeScanState:	return "TidRangeScan";
		case T_SubqueryScanState:	return "SubqueryScan";
		case T_FunctionScanState:	return "FunctionScan";
		case T_TableFuncScanState:	return "TableFuncScan";
		case T_ValuesScanState:		return "ValuesScan";
		case T_CteScanState:		return "CteScan";
		case T_NamedTuplestoreScanState:	return "NamedTuplestoreScan";
		case T_WorkTableScanState:	return "WorkTableScan";
		case T_ForeignScanState:	return "ForeignScan";
		case T_CustomScanState:		return "CustomScan";
		case T_NestLoopState:		return "NestLoop";
		case T_MergeJoinState:		return "MergeJoin";
		case T_HashJoinState:		return "HashJoin";
		case T_MaterialState:		return "Material";
		case T_MemoizeState:		return "Memoize";
		case T_SortState:			return "Sort";
		case T_IncrementalSortState:	return "IncrementalSort";
		case T_GroupState:			return "Group";
		case T_AggState:			return "Agg";
		case T_WindowAggState:		return "WindowAgg";
		case T_UniqueState:			return "Unique";
		case T_GatherState:			return "Gather";
		case T_GatherMergeState:	return "GatherMerge";
		case T_HashState:			return "Hash";
		case T_SetOpState:			return "SetOp";
		case T_LockRowsState:		return "LockRows";
		case T_LimitState:			return "Limit";
		case T_AppendState:			return "Append";
		case T_MergeAppendState:	return "MergeAppend";
		case T_RecursiveUnionState:	return "RecursiveUnion";
		case T_BitmapAndState:		return "BitmapAnd";
		case T_BitmapOrState:		return "BitmapOr";
		case T_ResultState:			return "Result";
		case T_ProjectSetState:		return "ProjectSet";
		case T_ModifyTableState:	return "ModifyTable";
		default:
			{
				static char buf[32];

				snprintf(buf, sizeof(buf), "Node%u", (unsigned) tag);
				return buf;
			}
	}
}

/*
 * Insert a candidate (tag, total_ms) into the top-N slowest[] array,
 * maintaining descending order by total_ms.
 */
static void
maybe_insert_slowest(const char *tag, double total_ms)
{
	int			i;
	int			insert_at = -1;

	/* Find insertion point (first slot where we beat the stored total_ms). */
	for (i = 0; i < N_SLOWEST; i++)
	{
		if (total_ms > accum.slowest[i].total_ms)
		{
			insert_at = i;
			break;
		}
	}

	if (insert_at < 0)
		return;					/* not in top-N */

	/* Shift down to make room. */
	for (i = N_SLOWEST - 1; i > insert_at; i--)
		accum.slowest[i] = accum.slowest[i - 1];

	accum.slowest[insert_at].total_ms = total_ms;
	accum.slowest[insert_at].tag = tag;
}

/* -------------------------------------------------------------------------
 * Collector callbacks
 * -------------------------------------------------------------------------
 */

static bool
planpath_enabled(void)
{
	/*
	 * The collector's walk runs if EITHER the compact rollup
	 * (otel.trace_plan_node_stats) OR the rich per-node events
	 * (otel.trace_plan_node_events) are on.  The two emit paths are
	 * independent: node_end reads each node's instrument once and (a) feeds
	 * the compact accumulator when stats is on, and (b) emits a pg.plan.node
	 * event when events is on.
	 */
	return otel_trace_plan_node_stats || otel_trace_plan_node_events;
}

/*
 * end_walk_begin: reset the accumulator before the end-walk traversal.
 */
static void
planpath_end_walk_begin(OtelPlanwalkContext *ctx)
{
	int			i;

	accum.any_spill = false;
	accum.spill_kb = 0;
	accum.have_shortfall = false;
	accum.shortfall_planned = 0;
	accum.shortfall_launched = 0;
	accum.worst_ratio = 0.0;
	accum.worst_node_tag = NULL;
	accum.bitmap_lossy_pages = 0;
	accum.buffers_read = 0;
	accum.buffers_hit = 0;
	accum.buffers_dirtied = 0;
	accum.node_count = 0;
	for (i = 0; i < N_SLOWEST; i++)
	{
		accum.slowest[i].total_ms = 0.0;
		accum.slowest[i].tag = NULL;
	}
}

/*
 * node_end: accumulate per-node data into accum.
 *
 * Called once per PlanState node during the end-walk.  We call InstrEndLoop
 * on each node's instrument before reading counters, exactly as ExplainNode
 * does (explain.c ~line 1839).
 */
static void
planpath_node_end(PlanState *ps, OtelPlanwalkContext *ctx)
{
	NodeTag		tag;
	const char *tag_name;
	NodeInstrumentation *ni;

	if (ps == NULL)
		return;

	tag = nodeTag(ps);
	tag_name = nodetag_name(tag);

	accum.node_count++;

	/* ----------------------------------------------------------------
	 * Step 3a: Spill detection (node-type specific)
	 * ---------------------------------------------------------------- */

	switch (tag)
	{
		case T_SortState:
			{
				SortState  *ss = (SortState *) ps;

				if (ss->sort_Done && ss->tuplesortstate != NULL)
				{
					TuplesortInstrumentation stats;

					tuplesort_get_stats((Tuplesortstate *) ss->tuplesortstate,
										&stats);
					if (stats.spaceType == SORT_SPACE_TYPE_DISK)
					{
						/* spaceUsed is already in kB (tuplesort.h contract) */
						accum.any_spill = true;
						accum.spill_kb += stats.spaceUsed;
					}
				}
				/* Also check parallel workers' sort stats. */
				if (ss->shared_info != NULL)
				{
					int			n;

					for (n = 0; n < ss->shared_info->num_workers; n++)
					{
						TuplesortInstrumentation *si =
							&ss->shared_info->sinstrument[n];

						if (si->sortMethod == SORT_TYPE_STILL_IN_PROGRESS)
							continue;
						if (si->spaceType == SORT_SPACE_TYPE_DISK)
						{
							accum.any_spill = true;
							accum.spill_kb += si->spaceUsed;
						}
					}
				}
				break;
			}

		case T_HashState:
			{
				HashState  *hs = (HashState *) ps;
				HashInstrumentation hinstr = {0};

				if (hs->hinstrument)
					memcpy(&hinstr, hs->hinstrument, sizeof(HashInstrumentation));

				/* Merge worker stats (max across participants, like explain.c). */
				if (hs->shared_info != NULL)
				{
					int			i;

					for (i = 0; i < hs->shared_info->num_workers; i++)
					{
						HashInstrumentation *whi =
							&hs->shared_info->hinstrument[i];

						if (whi->nbatch > hinstr.nbatch)
							hinstr.nbatch = whi->nbatch;
						if (whi->space_peak > hinstr.space_peak)
							hinstr.space_peak = whi->space_peak;
					}
				}

				/*
				 * nbatch > 1 means the hash table spilled to disk.
				 * space_peak is bytes of hash table peak memory; there is no
				 * separate "disk bytes" counter on HashState — we record the
				 * peak memory as a proxy (it sets the scale of the spill).
				 * TODO: report actual on-disk bytes once the instrumentation
				 * exposes them.
				 */
				if (hinstr.nbatch > 1)
				{
					accum.any_spill = true;
					accum.spill_kb += BYTES_TO_KB(hinstr.space_peak);
				}
				break;
			}

		case T_AggState:
			{
				AggState   *as = (AggState *) ps;
				Agg		   *agg = (Agg *) ps->plan;

				if (agg->aggstrategy == AGG_HASHED ||
					agg->aggstrategy == AGG_MIXED)
				{
					/* hash_batches_used > 1 means disk spill occurred. */
					if (as->hash_batches_used > 1)
					{
						/* hash_disk_used is in kB (see AggState in execnodes.h). */
						accum.any_spill = true;
						accum.spill_kb += (int64) as->hash_disk_used;
					}

					/* Also sum parallel workers' disk usage. */
					if (as->shared_info != NULL)
					{
						int			n;

						for (n = 0; n < as->shared_info->num_workers; n++)
						{
							AggregateInstrumentation *si =
								&as->shared_info->sinstrument[n];

							if (si->hash_mem_peak == 0)
								continue;
							if (si->hash_batches_used > 1)
							{
								accum.any_spill = true;
								accum.spill_kb += (int64) si->hash_disk_used;
							}
						}
					}
				}
				break;
			}

		case T_MaterialState:
			{
				MaterialState *ms = (MaterialState *) ps;

				if (ms->tuplestorestate != NULL)
				{
					char	   *storage_type;
					int64		max_space;

					tuplestore_get_stats(ms->tuplestorestate,
										 &storage_type, &max_space);
					if (strcmp(storage_type, "Disk") == 0)
					{
						accum.any_spill = true;
						accum.spill_kb += BYTES_TO_KB(max_space);
					}
				}
				break;
			}

		case T_WindowAggState:
			{
				WindowAggState *ws = (WindowAggState *) ps;

				if (ws->buffer != NULL)
				{
					char	   *storage_type;
					int64		max_space;

					tuplestore_get_stats(ws->buffer, &storage_type, &max_space);
					if (strcmp(storage_type, "Disk") == 0)
					{
						accum.any_spill = true;
						accum.spill_kb += BYTES_TO_KB(max_space);
					}
				}
				break;
			}

		case T_GatherState:
			{
				GatherState *gs = (GatherState *) ps;
				Gather	   *g = (Gather *) ps->plan;

				if (!accum.have_shortfall &&
					gs->nworkers_launched < g->num_workers)
				{
					accum.have_shortfall = true;
					accum.shortfall_planned = g->num_workers;
					accum.shortfall_launched = gs->nworkers_launched;
				}
				break;
			}

		case T_GatherMergeState:
			{
				GatherMergeState *gms = (GatherMergeState *) ps;
				GatherMerge *gm = (GatherMerge *) ps->plan;

				if (!accum.have_shortfall &&
					gms->nworkers_launched < gm->num_workers)
				{
					accum.have_shortfall = true;
					accum.shortfall_planned = gm->num_workers;
					accum.shortfall_launched = gms->nworkers_launched;
				}
				break;
			}

		case T_BitmapHeapScanState:
			{
				BitmapHeapScanState *bhs = (BitmapHeapScanState *) ps;

				accum.bitmap_lossy_pages += bhs->stats.lossy_pages;
				break;
			}

		default:
			break;
	}

	/* ----------------------------------------------------------------
	 * Steps 3b + 4: per-node instrumentation (requires instrument != NULL)
	 *
	 * Call InstrEndLoop before reading counters, as ExplainNode does.
	 * ---------------------------------------------------------------- */

	ni = ps->instrument;
	if (ni == NULL)
		return;

	InstrEndLoop(ni);

	if (ni->nloops <= 0)
		return;					/* node never executed */

	/* Step 4: buffer usage aggregation. */
	accum.buffers_read += ni->instr.bufusage.shared_blks_read;
	accum.buffers_hit += ni->instr.bufusage.shared_blks_hit;
	accum.buffers_dirtied += ni->instr.bufusage.shared_blks_dirtied;

	/* Step 4: top-N slowest nodes by actual total time. */
	{
		double		total_ms =
			INSTR_TIME_GET_MILLISEC(ni->instr.total) / ni->nloops;

		maybe_insert_slowest(tag_name, total_ms);
	}

	/* Step 3b: bad row estimate detection. */
	{
		double		plan_rows = ps->plan->plan_rows;
		double		actual_rows = ni->ntuples / ni->nloops;
		double		ratio;

		/*
		 * ratio = max(plan, actual) / max(1, min(plan, actual))
		 * Guards against both directions of misestimate and division by zero.
		 */
		if (plan_rows < 1.0)
			plan_rows = 1.0;
		if (actual_rows < 1.0)
			actual_rows = 1.0;

		if (plan_rows > actual_rows)
			ratio = plan_rows / actual_rows;
		else
			ratio = actual_rows / plan_rows;

		if (ratio > accum.worst_ratio)
		{
			accum.worst_ratio = ratio;
			accum.worst_node_tag = tag_name;
		}
	}

	/* ----------------------------------------------------------------
	 * Proposal 1 "rich": one span event per plan node.
	 *
	 * Additive and independent of the compact accumulation above -- gated on
	 * its own GUC (otel.trace_plan_node_events).  We reuse the very same
	 * finalized NodeInstrumentation fields the compact path reads (ni->startup,
	 * ni->instr.total, ni->ntuples, ni->nloops, ni->instr.bufusage.*), so the
	 * field access mirrors that code exactly.  No child spans are minted.
	 *
	 * All per-node events share the ExecutorEnd timestamp: we pass ts=0 ("now")
	 * -- there is no per-node wall-clock capture point, only the aggregate
	 * end-walk moment.  Attribute values are built transiently in ctx->attr_cxt
	 * with psprintf; the generic event API COPIES name + attrs into the span's
	 * context, so they may go out of scope immediately after the call.
	 * ---------------------------------------------------------------- */
	if (otel_trace_plan_node_events)
	{
		const OtelTracingApi *api = otel_api_get();

		if (api != NULL && ctx->stmt_span != NULL && api->span_add_event != NULL)
		{
			OtelKeyValue attrs[8];
			int			n = 0;
			MemoryContext old = MemoryContextSwitchTo(ctx->attr_cxt);
			double		startup_ms = INSTR_TIME_GET_MILLISEC(ni->startup) / ni->nloops;
			double		total_ms = INSTR_TIME_GET_MILLISEC(ni->instr.total) / ni->nloops;
			double		rows = ni->ntuples / ni->nloops;

			/* Node type: string literal from nodetag_name(), no psprintf. */
			attrs[n].key = "pg.plan.node.type";
			attrs[n].value = tag_name;
			n++;

			attrs[n].key = "pg.plan.node.actual_startup_ms";
			attrs[n].value = psprintf("%.3f", startup_ms); /* TODO(native-attr): emit as int64/double once the API grows typed attribute setters */
			n++;

			attrs[n].key = "pg.plan.node.actual_total_ms";
			attrs[n].value = psprintf("%.3f", total_ms); /* TODO(native-attr): emit as int64/double once the API grows typed attribute setters */
			n++;

			attrs[n].key = "pg.plan.node.rows";
			attrs[n].value = psprintf("%.0f", rows); /* TODO(native-attr): emit as int64/double once the API grows typed attribute setters */
			n++;

			attrs[n].key = "pg.plan.node.loops";
			attrs[n].value = psprintf("%.0f", ni->nloops); /* TODO(native-attr): emit as int64/double once the API grows typed attribute setters */
			n++;

			attrs[n].key = "pg.plan.node.buffers_read";
			attrs[n].value = psprintf(INT64_FORMAT, (int64) ni->instr.bufusage.shared_blks_read); /* TODO(native-attr): emit as int64/double once the API grows typed attribute setters */
			n++;

			attrs[n].key = "pg.plan.node.buffers_hit";
			attrs[n].value = psprintf(INT64_FORMAT, (int64) ni->instr.bufusage.shared_blks_hit); /* TODO(native-attr): emit as int64/double once the API grows typed attribute setters */
			n++;

			attrs[n].key = "pg.plan.node.buffers_dirtied";
			attrs[n].value = psprintf(INT64_FORMAT, (int64) ni->instr.bufusage.shared_blks_dirtied); /* TODO(native-attr): emit as int64/double once the API grows typed attribute setters */
			n++;

			api->span_add_event(ctx->stmt_span, "pg.plan.node", 0, attrs, n);

			MemoryContextSwitchTo(old);
		}
	}
}

/*
 * Check whether the worst misestimate ratio exceeds the threshold.
 * Called from planpath_end_walk_end after all nodes have been visited.
 */
static bool
planpath_check_misestimate(void)
{
	return accum.worst_ratio > MISESTIMATE_THRESHOLD;
}

/*
 * end_walk_end: fold accumulated data onto ctx->stmt_span.
 *
 * Called once after the end-walk traversal completes, while stmt_span is
 * still open and attr_cxt is live.  Allocate attribute values in attr_cxt
 * so they outlive standard_ExecutorEnd (attribute-lifetime rule).
 */
static void
planpath_end_walk_end(OtelPlanwalkContext *ctx)
{
	const OtelTracingApi *api;
	MemoryContext old;

	api = otel_api_get();
	if (api == NULL || ctx->stmt_span == NULL)
		return;

	old = MemoryContextSwitchTo(ctx->attr_cxt);

	/* ----------------------------------------------------------------
	 * Step 3: pathology attributes
	 * ---------------------------------------------------------------- */

	/* Spill to disk */
	if (accum.any_spill)
	{
		char	   *v;

		api->span_add_attribute_string(ctx->stmt_span,
									   "pg.exec.spilled",
									   "true"); /* TODO(native-attr): emit as bool once the API grows typed attribute setters */

		v = psprintf(INT64_FORMAT, accum.spill_kb);
		api->span_add_attribute_string(ctx->stmt_span,
									   "pg.exec.spill_kb",
									   v); /* TODO(native-attr): emit as int64 once the API grows typed attribute setters */
	}

	/* Parallel worker shortfall */
	if (accum.have_shortfall)
	{
		char	   *vp;
		char	   *vl;

		vp = psprintf("%d", accum.shortfall_planned);
		vl = psprintf("%d", accum.shortfall_launched);
		api->span_add_attribute_string(ctx->stmt_span,
									   "pg.parallel.workers_planned",
									   vp); /* TODO(native-attr): emit as int64 once the API grows typed attribute setters */
		api->span_add_attribute_string(ctx->stmt_span,
									   "pg.parallel.workers_launched",
									   vl); /* TODO(native-attr): emit as int64 once the API grows typed attribute setters */
	}

	/* Bad row estimate */
	if (planpath_check_misestimate())
	{
		char	   *vr;

		api->span_add_attribute_string(ctx->stmt_span,
									   "pg.exec.misestimate",
									   "true"); /* TODO(native-attr): emit as bool once the API grows typed attribute setters */

		vr = psprintf("%.1f", accum.worst_ratio);
		api->span_add_attribute_string(ctx->stmt_span,
									   "pg.exec.misestimate_ratio",
									   vr); /* TODO(native-attr): emit as double once the API grows typed attribute setters */

		/* worst_node_tag is a string literal — no psprintf needed */
		api->span_add_attribute_string(ctx->stmt_span,
									   "pg.exec.misestimate_node",
									   accum.worst_node_tag);
	}

	/* Lossy bitmap pages */
	if (accum.bitmap_lossy_pages > 0)
	{
		char	   *v;

		v = psprintf(UINT64_FORMAT, accum.bitmap_lossy_pages);
		api->span_add_attribute_string(ctx->stmt_span,
									   "pg.exec.bitmap_lossy_pages",
									   v); /* TODO(native-attr): emit as int64 once the API grows typed attribute setters */
	}

	/* ----------------------------------------------------------------
	 * Step 4: compact-actuals attributes
	 * ---------------------------------------------------------------- */

	/* Buffer usage */
	if (accum.buffers_read > 0)
	{
		char	   *v = psprintf(INT64_FORMAT, accum.buffers_read);

		api->span_add_attribute_string(ctx->stmt_span,
									   "pg.exec.buffers_read",
									   v); /* TODO(native-attr): emit as int64 once the API grows typed attribute setters */
	}

	if (accum.buffers_hit > 0)
	{
		char	   *v = psprintf(INT64_FORMAT, accum.buffers_hit);

		api->span_add_attribute_string(ctx->stmt_span,
									   "pg.exec.buffers_hit",
									   v); /* TODO(native-attr): emit as int64 once the API grows typed attribute setters */
	}

	if (accum.buffers_dirtied > 0)
	{
		char	   *v = psprintf(INT64_FORMAT, accum.buffers_dirtied);

		api->span_add_attribute_string(ctx->stmt_span,
									   "pg.exec.buffers_dirtied",
									   v); /* TODO(native-attr): emit as int64 once the API grows typed attribute setters */
	}

	/* Node count */
	{
		char	   *v = psprintf("%d", accum.node_count);

		api->span_add_attribute_string(ctx->stmt_span,
									   "pg.exec.node_count",
									   v); /* TODO(native-attr): emit as int64 once the API grows typed attribute setters */
	}

	/* Top-3 slowest nodes — pure string, no native-attr TODO needed */
	{
		int			i;
		bool		have_any = false;
		char	   *buf = NULL;

		for (i = 0; i < N_SLOWEST; i++)
		{
			if (accum.slowest[i].tag == NULL)
				break;
			have_any = true;
		}

		if (have_any)
		{
			StringInfoData si;

			initStringInfo(&si);
			for (i = 0; i < N_SLOWEST; i++)
			{
				if (accum.slowest[i].tag == NULL)
					break;
				if (i > 0)
					appendStringInfoChar(&si, ',');
				appendStringInfo(&si, "%s:%.0f",
								 accum.slowest[i].tag,
								 accum.slowest[i].total_ms);
			}
			buf = si.data;		/* allocated in attr_cxt already (via initStringInfo) */
			api->span_add_attribute_string(ctx->stmt_span,
										   "pg.exec.slowest_nodes",
										   buf);
		}
	}

	MemoryContextSwitchTo(old);
}

/* -------------------------------------------------------------------------
 * Collector registration
 * -------------------------------------------------------------------------
 */

static const OtelPlanwalkCollector planpath_collector = {
	.enabled = planpath_enabled,
	.node_begin = NULL,			/* root-fold collector: no begin-walk work */
	.node_end = planpath_node_end,
	.end_walk_begin = planpath_end_walk_begin,
	.end_walk_end = planpath_end_walk_end,
};

void
otel_planpath_install(void)
{
	otel_planwalk_register_collector(&planpath_collector);
}
