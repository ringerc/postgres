/*-------------------------------------------------------------------------
 *
 * otel_fdw.h
 *	  pg.fdw.scan span tracing for foreign-table scans.
 *
 *	  Internal interface consumed by otel_trace.c.  The implementation
 *	  (otel_fdw.c) has two execution-time strategies selected at compile
 *	  time by PG_VERSION_NUM; see that file's header comment.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/otel_postgres_tracing/otel_fdw.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONTRIB_OTEL_POSTGRES_TRACING_FDW_H
#define CONTRIB_OTEL_POSTGRES_TRACING_FDW_H

#include "access/xact.h"
#include "executor/execdesc.h"

/*
 * Install FDW-scan tracing.  Called once from _PG_init.
 *
 *   PG19+ : registers the ForeignScanBegin/ForeignScanEnd core hooks.
 *   PG18  : no-op; spans are driven from the executor hooks below.
 */
extern void otel_fdw_install_hooks(void);

/*
 * Executor-hook drivers, called from otel_trace.c's ExecutorStart /
 * ExecutorEnd hooks.
 *
 *   PG19+ : both no-ops (the core hooks do the work at the exact node
 *           init/teardown points).
 *   PG18  : _executor_start walks the planstate tree opening a pg.fdw.scan
 *           span per ForeignScanState; _executor_end walks it again
 *           emitting them.  See otel_fdw.c for the timing contract.
 *
 * The caller gates _executor_start on having an active recording span so
 * unsampled queries skip the tree walk entirely.  _executor_end is cheap
 * to call unconditionally (it early-returns when no spans are open).
 */
extern void otel_fdw_executor_start(QueryDesc *queryDesc);
extern void otel_fdw_executor_end(QueryDesc *queryDesc);

/*
 * Transaction-abort cleanup.  Both apply to both strategies: neither the
 * core ForeignScanEnd hook nor the ExecutorEnd-driven walk runs when a
 * (sub)transaction aborts, so the in-flight span stack must be drained
 * out of band.  See otel_fdw.c.
 */
extern void otel_fdw_subxact_abort(SubXactEvent event);
extern void otel_fdw_reset(void);

#endif							/* CONTRIB_OTEL_POSTGRES_TRACING_FDW_H */
