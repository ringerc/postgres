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
 * Load order: 'otel,otel_postgres_tracing' in
 * shared_preload_libraries.  contrib/otel publishes the rendezvous
 * api at its _PG_init; this module looks it up at its own _PG_init
 * (which runs second).  An ereport(ERROR) at startup tells the
 * operator to fix the preload order if it's wrong.
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
#include "miscadmin.h"
#include "utils/elog.h"
#include "utils/guc.h"

#include <otel_api/otel.h>

#include "otel_postgres_tracing.h"


PG_MODULE_MAGIC;


/*
 * The OtelTracingApi pointer cached at _PG_init time.  Used by
 * otel_trace.c and otel_log.c to reach into contrib/otel without
 * direct extern-symbol linkage.  Declared in
 * otel_postgres_tracing.h.
 */
const OtelTracingApi *otel_api = NULL;

/*
 * InstrumentationScope handle for this module's spans.  Registered
 * at _PG_init via otel_api->tracer_register; cached forever in
 * TopMemoryContext.  Used by otel_trace.c to tag every span it
 * produces.  Declared in otel_postgres_tracing.h.
 */
const OtelInstrumentationScope *otel_pg_tracer = NULL;

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
	void	  **slot;

	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(ERROR,
				(errmsg("otel_postgres_tracing must be loaded via shared_preload_libraries"),
				 errhint("Add 'otel,otel_postgres_tracing' to shared_preload_libraries.")));
	}

	slot = find_rendezvous_variable(OTEL_TRACING_API_RENDEZVOUS_NAME);
	otel_api = (const OtelTracingApi *) *slot;
	if (otel_api == NULL)
		ereport(ERROR,
				(errmsg("otel_postgres_tracing requires contrib/otel to be loaded first"),
				 errhint("In shared_preload_libraries, 'otel' must come before 'otel_postgres_tracing'.")));
	if (OTEL_API_MAJOR(otel_api->version) != OTEL_TRACING_API_MAJOR ||
		OTEL_API_MINOR(otel_api->version) < OTEL_TRACING_API_MINOR)
		ereport(ERROR,
				(errmsg("OtelTracingApi version mismatch"),
				 errdetail("Loaded contrib/otel exposes api version %u.%u; otel_postgres_tracing was built against version %u.%u.",
						   OTEL_API_MAJOR(otel_api->version),
						   OTEL_API_MINOR(otel_api->version),
						   OTEL_TRACING_API_MAJOR,
						   OTEL_TRACING_API_MINOR)));

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
	 * "otel.trace_all_queries" is the lone surviving GUC in the
	 * legacy "otel." namespace --- it predates the otel -> otel_api
	 * extension rename and is kept here for continuity with existing
	 * user configurations (a "SET otel.trace_all_queries = on" line
	 * in postgresql.conf shouldn't break on upgrade).  Other otel_api
	 * GUCs all moved to "otel_api.*" when extension naming was
	 * aligned to the package directory, so this module is now the
	 * sole owner of the "otel." prefix and the reservation is
	 * conflict-free.
	 *
	 * Also reserve "otel_postgres_tracing.*" for any future module-
	 * specific GUCs (none today).
	 */
	MarkGUCPrefixReserved("otel");
	MarkGUCPrefixReserved("otel_postgres_tracing");

	otel_pg_tracer = otel_api->tracer_register("contrib/otel_postgres_tracing",
											   PG_VERSION,
											   NULL);

	otel_trace_install_hooks();
	otel_log_install_hooks();
}
