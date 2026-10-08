/*-------------------------------------------------------------------------
 *
 * otel_postgres_tracing.c
 *	  Module entry point for contrib/otel_postgres_tracing.
 *
 * This module is the query-instrumentation consumer of contrib/otel's
 * OtelTracingApi.  It installs ExecutorStart / ExecutorEnd /
 * ProcessUtility_hook callbacks that build statement-level spans
 * and an emit_log_hook callback that captures ereport events as
 * span events on the active span.  All cross-module work goes
 * through the cached OtelTracingApi function pointers; no direct
 * extern symbol references into contrib/otel.
 *
 * The split between this module and contrib/otel was introduced
 * in Phase 4 of the contrib/otel restructure (see contrib-otel-
 * split.md in the parent workspace).  Before the split, the query-
 * tracing hooks lived inside contrib/otel itself and reached into
 * its internal storage directly.
 *
 * Load order: otel_postgres_tracing may appear anywhere in
 * shared_preload_libraries (or session_preload_libraries, or be
 * LOADed at session start).  The provider rendezvous is resolved
 * lazily at first hot-path use via otel_api_get() so preload order
 * is irrelevant.  If the provider is absent, tracing degrades to a
 * silent zero-cost no-op.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/otel_postgres_tracing/otel_postgres_tracing.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "utils/elog.h"
#include "utils/guc.h"

#include <otel_api/otel.h>

#include "otel_postgres_tracing.h"
#include "otel_planwalk.h"
#include "otel_planpath.h"
#include "otel_planspans.h"
#include "otel_planshape.h"


PG_MODULE_MAGIC;


/*
 * This module's tracer (OTel InstrumentationScope).  otel_api fills in
 * ->scope the first time it is passed to otel_span_start(); no explicit
 * registration call is needed with the new producer API.
 * Declared in otel_postgres_tracing.h.
 */
OtelTracer	otel_pg_tracer = {.name = "otel_postgres_tracing", .version = PG_VERSION};

/*
 * GUC controlling whether spans are emitted for queries that
 * carry no propagated trace context.  Off by default --- the
 * common case is "trace only what the client asked for".
 */
bool otel_trace_all_queries = false;


void		_PG_init(void);


void
_PG_init(void)
{
	/*
	 * No load-time guard: this module may be loaded via
	 * shared_preload_libraries, session_preload_libraries,
	 * local_preload_libraries, or a direct LOAD command.  All
	 * preload mechanisms run _PG_init before any query; a LOAD at
	 * session time gives best-effort, current-backend-only tracing
	 * from registration onward.  No ERROR for any of these paths.
	 *
	 * The provider (otel_api) is resolved lazily at first hot-path
	 * use via otel_pg_ensure(), so no rendezvous lookup here.
	 */

	/*
	 * trace_all_queries is owned by this module post-split: it's a
	 * query-tracing-policy GUC, not an API/infrastructure GUC.
	 * GUC name kept as "otel.trace_all_queries" for continuity with
	 * existing user configurations.
	 */
	DefineCustomBoolVariable("otel.trace_all_queries",
							 "Emit spans for all queries, even ones with no client-propagated trace context.",
							 "When off (default), spans are only produced when the client has supplied an otel.traceparent header or sqlcommenter context.",
							 &otel_trace_all_queries,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	/*
	 * Install executor and log hooks unconditionally.  The hooks
	 * call otel_pg_ensure() on every invocation and no-op when the
	 * provider is absent.  The tracer handle (otel_pg_tracer) is
	 * registered on the first successful otel_pg_ensure() call.
	 *
	 * otel_sdt_install() defines additional otel.* GUCs
	 * (otel.trace_sdt_probes, otel.trace_sdt_smgr, otel.trace_syncrep,
	 * otel.trace_replica).  These must all be registered BEFORE we call
	 * MarkGUCPrefixReserved("otel") below; reserving the prefix first
	 * would cause postgresql.conf values for those GUCs to be treated as
	 * unrecognised placeholders under a reserved prefix and silently
	 * dropped at load time.
	 */
	/*
	 * otel.trace_plan_node_stats: group-A gate for per-node Instrumentation.
	 * When on (and a span is active), INSTRUMENT_TIMER | INSTRUMENT_ROWS |
	 * INSTRUMENT_BUFFERS | INSTRUMENT_WAL are ORed into
	 * queryDesc->instrument_options before standard_ExecutorStart so the
	 * executor allocates per-node Instrumentation for sampled queries.
	 * Must be registered BEFORE MarkGUCPrefixReserved("otel") below.
	 */
	DefineCustomBoolVariable("otel.trace_plan_node_stats",
							 "Collect per-node execution statistics for sampled queries.",
							 "When on, enables per-node Instrumentation (timing, rows, buffers) "
							 "for queries that are being traced. Has no effect when no span is "
							 "active for a given query. Off by default.",
							 &otel_trace_plan_node_stats,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	/*
	 * otel.trace_plan_node_events: "rich" per-node span events.  Distinct from
	 * otel.trace_plan_node_stats DELIBERATELY -- the stats GUC folds a bounded
	 * compact rollup onto the single pgsql.execute span, whereas this GUC emits
	 * one span event PER PlanState node (far higher volume).  Operators need to
	 * enable the cheap rollup without opting into the per-node event firehose,
	 * so we do not reuse trace_plan_node_stats.  Either GUC ORs the same
	 * INSTRUMENT_* flags into instrument_options for sampled queries.
	 * Must be registered BEFORE MarkGUCPrefixReserved("otel") below.
	 */
	DefineCustomBoolVariable("otel.trace_plan_node_events",
							 "Emit one span event per plan node for sampled queries.",
							 "When on, attaches a pg.plan.node span event (type, actual "
							 "timing, rows, loops, buffers) to the pgsql.execute span for "
							 "every executed plan node. Much higher volume than "
							 "otel.trace_plan_node_stats. Has no effect when no span is "
							 "active for a given query. Off by default.",
							 &otel_trace_plan_node_events,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	otel_trace_install_hooks();
	otel_log_install_hooks();
	otel_sdt_install();

	/*
	 * Register the planstate-walker collectors.  otel_planpath_install only
	 * registers a collector (its otel.trace_plan_node_stats GUC is defined
	 * above); otel_planspans_install also defines otel.trace_plan_child_spans,
	 * so it MUST run before MarkGUCPrefixReserved("otel") below.
	 */
	otel_planpath_install();
	otel_planspans_install();
	otel_planshape_install();

	/*
	 * Reserve the "otel." and "otel_postgres_tracing." GUC prefixes now
	 * that every otel.* GUC owned by this module has been registered.
	 * The reservation must come AFTER all DefineCustom*Variable calls for
	 * these prefixes (including those inside otel_sdt_install()) so that
	 * values set in postgresql.conf are not discarded as unrecognised
	 * placeholders under a reserved prefix.
	 *
	 * "otel." covers: otel.trace_all_queries (above), otel.trace_sdt_probes,
	 * otel.trace_sdt_smgr, otel.trace_syncrep, otel.trace_replica
	 * (all registered by otel_sdt_install()).
	 *
	 * Also reserve "otel_postgres_tracing.*" for any future module-
	 * specific GUCs (none today).
	 */
	MarkGUCPrefixReserved("otel");
	MarkGUCPrefixReserved("otel_postgres_tracing");
}
