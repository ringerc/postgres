/*-------------------------------------------------------------------------
 *
 * otel_planpath.h
 *	  Pathology-flags + compact-actuals collector for the planstate walker.
 *
 * Implements steps 3 and 4 of the planstate-walker implementation plan:
 *
 *   Step 3 — Pathology flags: spill-to-disk, parallel worker shortfall,
 *             bad row estimates, and lossy bitmap heap scans.
 *
 *   Step 4 — Compact actuals: aggregate buffer usage, node count, and the
 *             top-3 slowest nodes — all folded onto pgsql.execute as span
 *             attributes in a single end-walk pass.
 *
 * Both sets of attributes share the otel.trace_plan_node_stats GUC (group-A
 * feature gate defined in otel_planwalk.h / registered in
 * otel_postgres_tracing.c) and are accumulated in one file-static struct
 * reset by end_walk_begin and emitted by end_walk_end.
 *
 * Usage: call otel_planpath_install() from _PG_init (before
 * MarkGUCPrefixReserved("otel")).  The integrator does this; this file
 * contains no GUC definitions of its own.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/otel_postgres_tracing/otel_planpath.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONTRIB_OTEL_POSTGRES_TRACING_PLANPATH_H
#define CONTRIB_OTEL_POSTGRES_TRACING_PLANPATH_H

/*
 * Register the pathology-flags + compact-actuals collector with the
 * planwalk dispatcher.  Must be called from _PG_init, before
 * MarkGUCPrefixReserved("otel") and before the first executor hook fires.
 */
extern void otel_planpath_install(void);

#endif							/* CONTRIB_OTEL_POSTGRES_TRACING_PLANPATH_H */
