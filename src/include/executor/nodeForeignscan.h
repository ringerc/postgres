/*-------------------------------------------------------------------------
 *
 * nodeForeignscan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeForeignscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEFOREIGNSCAN_H
#define NODEFOREIGNSCAN_H

#include "access/parallel.h"
#include "nodes/execnodes.h"

extern ForeignScanState *ExecInitForeignScan(ForeignScan *node, EState *estate, int eflags);
extern void ExecEndForeignScan(ForeignScanState *node);
extern void ExecReScanForeignScan(ForeignScanState *node);

extern void ExecForeignScanEstimate(ForeignScanState *node,
									ParallelContext *pcxt);
extern void ExecForeignScanInitializeDSM(ForeignScanState *node,
										 ParallelContext *pcxt);
extern void ExecForeignScanReInitializeDSM(ForeignScanState *node,
										   ParallelContext *pcxt);
extern void ExecForeignScanInitializeWorker(ForeignScanState *node,
											ParallelWorkerContext *pwcxt);
extern void ExecShutdownForeignScan(ForeignScanState *node);

extern void ExecAsyncForeignScanRequest(AsyncRequest *areq);
extern void ExecAsyncForeignScanConfigureWait(AsyncRequest *areq);
extern void ExecAsyncForeignScanNotify(AsyncRequest *areq);

/* Hooks for FDW scan begin/end observation (e.g., OTel tracing). */
typedef void (*ForeignScanBegin_hook_type) (ForeignScanState *node, int eflags);
typedef void (*ForeignScanEnd_hook_type) (ForeignScanState *node);

PGDLLIMPORT extern ForeignScanBegin_hook_type ForeignScanBegin_hook;
PGDLLIMPORT extern ForeignScanEnd_hook_type ForeignScanEnd_hook;

#endif							/* NODEFOREIGNSCAN_H */
