/*-------------------------------------------------------------------------
 *
 * otel_planwalk.h
 *	  Single-pass planstate-tree-walker dispatcher for OpenTelemetry
 *	  instrumentation collectors.
 *
 * Every walk-based feature (FDW spans on PG18, per-node stats, pathology
 * flags, curated child spans) registers a collector here.  A single
 * planstate_tree_walker pass is performed per ExecutorStart / ExecutorEnd,
 * dispatching to each enabled collector's node_begin / node_end callback.
 *
 * Usage:
 *   1. At _PG_init time, call otel_planwalk_install().
 *   2. In otel_ExecutorStart, after the chain, call
 *      otel_planwalk_executor_start() when span_active.
 *   3. In otel_ExecutorEnd, before the chain, call
 *      otel_planwalk_executor_end().
 *   4. Collectors call otel_planwalk_register_collector() from their own
 *      install/init functions; the registered array is static and
 *      fixed-size (OTEL_PLANWALK_MAX_COLLECTORS entries).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/otel_postgres_tracing/otel_planwalk.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONTRIB_OTEL_POSTGRES_TRACING_PLANWALK_H
#define CONTRIB_OTEL_POSTGRES_TRACING_PLANWALK_H

#include "executor/execdesc.h"
#include "nodes/execnodes.h"
#include "utils/memutils.h"

#include <otel_api/otel.h>

/*
 * Context passed to every collector callback during a walk.  The dispatcher
 * builds one of these per executor call and passes a pointer down through the
 * planstate_tree_walker.
 */
typedef struct OtelPlanwalkContext
{
	QueryDesc  *queryDesc;		/* the active QueryDesc */
	OtelSpan   *stmt_span;		/* the enclosing pgsql.execute span */
	MemoryContext attr_cxt;		/* span_cxt: allocate attribute values here */
} OtelPlanwalkContext;

/*
 * A collector registration entry.  Any callback may be NULL (the dispatcher
 * skips NULL callbacks cheaply).
 *
 *   enabled()        -- returns true iff this collector wants callbacks for
 *                       the current query.  Called once per walk before the
 *                       tree traversal begins.  Must be non-NULL.
 *   node_begin(ps, ctx) -- called on each PlanState node during the start
 *                       walk (after standard_ExecutorStart).
 *   node_end(ps, ctx)   -- called on each PlanState node during the end
 *                       walk (before standard_ExecutorEnd).
 *   end_walk_begin(ctx) -- called ONCE before the end-walk traversal begins.
 *                       Use to reset per-query accumulators.  Aggregation
 *                       collectors (per-node stats, pathology flags) need this.
 *   end_walk_end(ctx)   -- called ONCE after the end-walk traversal completes,
 *                       while stmt_span is still open and attr_cxt is live.
 *                       Fold aggregated attributes onto ctx->stmt_span here.
 *
 * Child-span collectors (FDW, curated node spans) use node_begin/node_end and
 * leave the end_walk_* framing callbacks NULL.  Root-fold collectors use
 * end_walk_begin (reset) + node_end (accumulate) + end_walk_end (emit).
 */
typedef struct OtelPlanwalkCollector
{
	bool		(*enabled) (void);
	void		(*node_begin) (PlanState *ps, OtelPlanwalkContext *ctx);
	void		(*node_end) (PlanState *ps, OtelPlanwalkContext *ctx);
	void		(*end_walk_begin) (OtelPlanwalkContext *ctx);
	void		(*end_walk_end) (OtelPlanwalkContext *ctx);
} OtelPlanwalkCollector;

/* Maximum number of simultaneously registered collectors. */
#define OTEL_PLANWALK_MAX_COLLECTORS 8

/*
 * Register a collector.  Must be called before the first executor hook fires
 * (i.e., from _PG_init or from install functions called by _PG_init).
 * Panics (elog ERROR) if the fixed-size array is full.
 */
extern void otel_planwalk_register_collector(const OtelPlanwalkCollector *c);

/*
 * Called from otel_trace_install_hooks() once at _PG_init.  Reserved for
 * future initialization; currently a no-op.
 */
extern void otel_planwalk_install(void);

/*
 * Executor hook drivers called from otel_trace.c.
 *
 *   _executor_start: called after standard_ExecutorStart when span_active.
 *                    Performs the begin-walk (calls node_begin on each node).
 *   _executor_end:   called before standard_ExecutorEnd (unconditionally,
 *                    or at minimum whenever span was active at start).
 *                    Performs the end-walk (calls node_end on each node).
 *                    Early-outs cheaply when no collector is enabled.
 */
extern void otel_planwalk_executor_start(QueryDesc *queryDesc,
										 OtelSpan *stmt_span,
										 MemoryContext attr_cxt);
extern void otel_planwalk_executor_end(QueryDesc *queryDesc,
									   OtelSpan *stmt_span,
									   MemoryContext attr_cxt);

/*
 * Returns true iff any group-A feature (requiring per-node Instrumentation)
 * is currently enabled.  Used by otel_ExecutorStart to decide whether to OR
 * INSTRUMENT_* flags into queryDesc->instrument_options before the chain.
 */
extern bool otel_planwalk_want_instrumentation(void);

/* Backing variable for otel.trace_plan_node_stats GUC. */
extern bool otel_trace_plan_node_stats;

#endif							/* CONTRIB_OTEL_POSTGRES_TRACING_PLANWALK_H */
