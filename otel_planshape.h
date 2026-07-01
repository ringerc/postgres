/*-------------------------------------------------------------------------
 *
 * otel_planshape.h
 *	  Plan-shape capture at plan time (proposal 4).
 *
 *	  A sibling of the planstate-walker collectors that walks the Plan tree
 *	  (not the PlanState tree) at ExecutorStart, before execution.  It stamps
 *	  the pgsql.execute span with a plan-shape digest (for "did this
 *	  statement's plan change?" regression detection) and a few structural
 *	  risk flags (large seq scan, nested loop with a high inner estimate).
 *
 *	  Unlike the execution-time collectors it needs no per-node
 *	  Instrumentation and runs even for queries that error during execution
 *	  (ExecutorStart precedes ExecutorRun).  It is its own walker over Plan
 *	  nodes, so it is NOT registered with the otel_planwalk dispatcher.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/otel_postgres_tracing/otel_planshape.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONTRIB_OTEL_POSTGRES_TRACING_PLANSHAPE_H
#define CONTRIB_OTEL_POSTGRES_TRACING_PLANSHAPE_H

#include "executor/execdesc.h"
#include "utils/memutils.h"

#include <otel_api/otel.h>

/*
 * Define the otel.trace_plan_shape GUC.  Called once from _PG_init BEFORE
 * MarkGUCPrefixReserved("otel").
 */
extern void otel_planshape_install(void);

/*
 * Walk queryDesc->plannedstmt at ExecutorStart and stamp plan-shape
 * attributes onto stmt_span.  Self-gates on the otel.trace_plan_shape GUC, so
 * the caller only needs to ensure a span is active.  Attribute values are
 * allocated in attr_cxt (span_cxt) so they outlive standard_ExecutorEnd.
 */
extern void otel_planshape_executor_start(QueryDesc *queryDesc,
										   OtelSpan *stmt_span,
										   MemoryContext attr_cxt);

/* Backing variable for the otel.trace_plan_shape GUC. */
extern bool otel_trace_plan_shape;

#endif							/* CONTRIB_OTEL_POSTGRES_TRACING_PLANSHAPE_H */
