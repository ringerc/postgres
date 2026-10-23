/*-------------------------------------------------------------------------
 *
 * otel_planspans.h
 *	  Curated child spans for a few "interesting" plan-node types.
 *
 *	  A planstate-walker collector (driven by otel_planwalk.c) that emits a
 *	  child span under the enclosing pgsql.execute span for a small, curated
 *	  set of node kinds where "this operator did remote/expensive work" is
 *	  itself the story: CustomScan, FunctionScan, TableFuncScan, and the
 *	  parallel Gather / GatherMerge region.  NEVER a span per node.
 *
 *	  Reuses the same discipline as otel_fdw.c: a fixed-depth span stack keyed
 *	  by the PlanState pointer (non-LIFO close order safe), plus (sub)xact-abort
 *	  cleanup, because a walk-driven child-span collector needs the same abort
 *	  handling (neither ExecEndNode nor the ExecutorEnd walk runs on abort).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/otel_postgres_tracing/otel_planspans.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONTRIB_OTEL_POSTGRES_TRACING_PLANSPANS_H
#define CONTRIB_OTEL_POSTGRES_TRACING_PLANSPANS_H

#include "access/xact.h"

/*
 * Register the curated-child-span collector and define the
 * otel.trace_plan_child_spans GUC.  Called once from _PG_init BEFORE
 * MarkGUCPrefixReserved("otel").
 */
extern void otel_planspans_install(void);

/*
 * (Sub)transaction-abort cleanup, mirroring otel_fdw_subxact_abort /
 * otel_fdw_reset.  The dispatcher end-walk does not run on abort, so the
 * in-flight child-span stack must be drained out of band.  Wired into
 * otel_trace.c's SubXactCallback / xact-abort paths alongside the FDW ones.
 */
extern void otel_planspans_subxact_abort(SubXactEvent event);
extern void otel_planspans_reset(void);

/* Backing variable for the otel.trace_plan_child_spans GUC. */
extern bool otel_trace_plan_child_spans;

#endif							/* CONTRIB_OTEL_POSTGRES_TRACING_PLANSPANS_H */
