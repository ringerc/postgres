/*-------------------------------------------------------------------------
 *
 * otel_trace.c
 *	  Span production hooks for contrib/otel_postgres_tracing.
 *
 * Owns the executor / utility hooks that produce one span per statement.
 * otel_api owns all span storage now (an OtelSpanRef is an 8-byte
 * handle); this file keeps only a small bookkeeping stack that maps a
 * statement's identity (its QueryDesc, or a per-call token for a
 * utility statement) to the OtelSpanRef so the matching ExecutorEnd /
 * ProcessUtility return can end it.  Nested statements (e.g. CREATE
 * TABLE AS running its SELECT through the executor) now get their own
 * span apiece, nested under the outer one via the API's active stack
 * (OTEL_PARENT_ACTIVE, the default parent).
 *
 * Hot path:
 *	 ExecutorStart_hook -> early-bail or start_stmt_span()
 *	 ExecutorEnd_hook   -> end the span matching this QueryDesc
 *
 * Error path: if ExecutorEnd is never reached (an error unwinds past
 * it), the span's resource owner is released on abort and otel_api
 * exports it with ERROR status.  This file does not need its own
 * abort-time span cleanup; it only needs to keep its own bookkeeping
 * stack in sync, which the xact/subxact callbacks below do.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/otel_postgres_tracing/otel_trace.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "access/parallel.h"
#include "access/xact.h"
#include "executor/executor.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "tcop/cmdtag.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "utils/backend_status.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/guc.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include "executor/instrument.h"

#include <otel_api/otel.h>
#include "otel_postgres_tracing.h"
#include "otel_fdw.h"
#include "otel_planwalk.h"
#include "otel_planspans.h"
#include "otel_planshape.h"

/*
 * Bookkeeping stack: maps a statement's identity to the OtelSpanRef
 * started for it.  Not span storage -- just enough to find the right
 * handle again at the matching ExecutorEnd / ProcessUtility return.
 *
 * key is queryDesc for executor spans, or the address of a stack-local
 * in the owning otel_ProcessUtility() frame for utility spans (unique
 * and stable for the duration of that call, including recursive
 * invocations for nested utility statements).
 *
 * Entries are removed at the matching end call.  On (sub)transaction
 * abort, entries at or above the aborting nesting level are dropped
 * here too: their spans are ended (and exported with ERROR status) by
 * otel_api's own resource-owner release, so this is bookkeeping
 * cleanup only, not span cleanup.
 *
 * detached marks a cursor's executor span (see
 * otel_declaring_cursor_name below): it is .detached, so it does not
 * sit on otel_api's active stack across the statements between
 * DECLARE and CLOSE/portal-drop.  otel_ExecutorRun() looks entries up
 * by key (without removing them) to activate/deactivate it around the
 * one ExecutorRun call that belongs to its FETCH.
 */
#define OTEL_STMT_STACK_MAX 32
typedef struct StmtSpanEntry
{
	const void *key;
	OtelSpanRef ref;
	int			subxact_level;
	bool		detached;		/* a cursor's executor span */
} StmtSpanEntry;

static StmtSpanEntry stmt_stack[OTEL_STMT_STACK_MAX];
static int	stmt_stack_depth = 0;

/*
 * The portal name from a DeclareCursorStmt currently being processed
 * by otel_ProcessUtility(), or NULL.  PerformCursorOpen() plans the
 * cursor's query (which can run arbitrary nested statements, e.g. a
 * volatile-in-practice IMMUTABLE function folded by the planner, each
 * with its own ExecutorStart) *before* it creates the portal and
 * calls PortalStart() -- so a bare "are we inside DECLARE CURSOR's
 * ProcessUtility call" flag would wrongly mark every such nested
 * statement's span .detached too, not just the cursor's own.
 *
 * PortalStart() (tcop/pquery.c) sets the global ActivePortal to the
 * new portal *before* calling ExecutorStart() for it, and nothing
 * else does between here and there (planning happens first, with
 * ActivePortal still pointing at whatever the enclosing statement's
 * portal is). So otel_ExecutorStart() checks ActivePortal->name
 * against this name: true only for the cursor's own ExecutorStart,
 * never for a nested statement run during planning.
 */
static const char *otel_declaring_cursor_name = NULL;

/* True iff ActivePortal is the cursor otel_declaring_cursor_name names --
 * i.e. this ExecutorStart is the cursor's own, not some nested statement
 * run while planning it. */
static inline bool
otel_active_portal_is_declaring_cursor(void)
{
	return otel_declaring_cursor_name != NULL &&
		ActivePortal != NULL &&
		ActivePortal->name != NULL &&
		strcmp(ActivePortal->name, otel_declaring_cursor_name) == 0;
}

/* Per-backend scratch context for building attribute values (e.g. plan
 * shape digests) that are copied into the span the moment they're set;
 * it need not outlive the setter call, so it is just reset per use. */
static MemoryContext stmt_attr_cxt = NULL;

/* Hook chains */
static ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
static ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;
static ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;

static void otel_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void otel_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
							  uint64 count);
static void otel_ExecutorEnd(QueryDesc *queryDesc);
static void otel_ProcessUtility(PlannedStmt *pstmt,
								const char *queryString,
								bool readOnlyTree,
								ProcessUtilityContext context,
								ParamListInfo params,
								QueryEnvironment *queryEnv,
								DestReceiver *dest,
								QueryCompletion *qc);
static void otel_pgtracing_xact_callback(XactEvent event, void *arg);
static void otel_pgtracing_subxact_callback(SubXactEvent event,
											SubTransactionId mySubid,
											SubTransactionId parentSubid,
											void *arg);
static void otel_proc_exit_cb(int code, Datum arg);
static void push_stmt_span(const void *key, OtelSpanRef ref, bool detached);
static OtelSpanRef pop_stmt_span(const void *key);
static bool peek_cursor_span(const void *key, OtelSpanRef *ref);
static void maybe_apply_sqlcommenter(const char *sql);
static void maybe_reset_comment_context(void);
static void publish_leader_context(OtelSpanRef s);
static void restore_leader_context(void);
static OtelSpanRef start_stmt_span(const char *name, const char *query_text,
								   uint64 query_id, bool detached);
static void finalize_stmt_span(OtelSpanRef s);


/*
 * Install our executor hooks and register the xact + proc exit
 * callbacks.  Called once from _PG_init.
 */
void
otel_trace_install_hooks(void)
{
	prev_ExecutorStart_hook = ExecutorStart_hook;
	ExecutorStart_hook = otel_ExecutorStart;
	prev_ExecutorRun_hook = ExecutorRun_hook;
	ExecutorRun_hook = otel_ExecutorRun;
	prev_ExecutorEnd_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = otel_ExecutorEnd;

	/* Utility-statement spans. */
	prev_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = otel_ProcessUtility;

	/* FDW scan spans (pg.fdw.scan); strategy is version-gated in otel_fdw.c. */
	otel_fdw_install_hooks();

	/* Planstate-walker dispatcher; also called by collectors at their install. */
	otel_planwalk_install();

	RegisterXactCallback(otel_pgtracing_xact_callback, NULL);
	RegisterSubXactCallback(otel_pgtracing_subxact_callback, NULL);
	on_proc_exit(otel_proc_exit_cb, (Datum) 0);
}

/*
 * Push a bookkeeping entry.  If the (fixed-size) stack is full, end the
 * span immediately rather than leak the handle -- this is only meant
 * to bound pathological recursion depth, not a normal path.
 */
static void
push_stmt_span(const void *key, OtelSpanRef ref, bool detached)
{
	if (ref.v == 0)
		return;

	if (stmt_stack_depth >= OTEL_STMT_STACK_MAX)
	{
		otel_span_end(ref);
		return;
	}

	stmt_stack[stmt_stack_depth].key = key;
	stmt_stack[stmt_stack_depth].ref = ref;
	stmt_stack[stmt_stack_depth].subxact_level = GetCurrentTransactionNestLevel();
	stmt_stack[stmt_stack_depth].detached = detached;
	stmt_stack_depth++;
}

/*
 * Find and remove the bookkeeping entry for key (searching from the
 * top, like otel_fdw.c's stack, since ends are expected LIFO but the
 * search is robust to the rare exception).  Returns OTEL_SPAN_NONE if
 * not found (e.g. the entry was already dropped by an abort).
 */
static OtelSpanRef
pop_stmt_span(const void *key)
{
	int			i;

	for (i = stmt_stack_depth - 1; i >= 0; i--)
	{
		if (stmt_stack[i].key == key)
		{
			OtelSpanRef ref = stmt_stack[i].ref;

			for (; i < stmt_stack_depth - 1; i++)
				stmt_stack[i] = stmt_stack[i + 1];
			stmt_stack_depth--;
			return ref;
		}
	}
	return OTEL_SPAN_NONE;
}

/*
 * Non-destructive lookup for otel_ExecutorRun(): true and *ref set if
 * key names a .detached (cursor) entry still on the bookkeeping stack;
 * false (an ordinary statement, or no entry at all -- e.g. a FETCH's
 * ExecutorRun when nothing is recording) otherwise.  Does not touch
 * the entry; it is removed later, at the matching ExecutorEnd.
 */
static bool
peek_cursor_span(const void *key, OtelSpanRef *ref)
{
	for (int i = stmt_stack_depth - 1; i >= 0; i--)
	{
		if (stmt_stack[i].key == key)
		{
			if (!stmt_stack[i].detached)
				return false;
			*ref = stmt_stack[i].ref;
			return true;
		}
	}
	return false;
}

/*
 * sqlcommenter: try to pick up a traceparent embedded in the SQL text
 * as a comment, but only when no root context is already set (a client-
 * supplied 'M' header or otel.traceparent GUC wins).  No-op when
 * otel_api is absent.
 */
static void
maybe_apply_sqlcommenter(const char *sql)
{
	const OtelInternalApi *api = otel_internal_api();
	OtelRootContext rc;

	if (api == NULL || sql == NULL)
		return;

	api->get_root_context(&rc);
	if (!rc.is_set)
		(void) api->try_apply_sqlcommenter_context(sql);
}

/*
 * A sqlcommenter-derived root context applies to one statement only;
 * reset it once that statement's span has ended so it doesn't bleed
 * into the next.  ('M' / GUC-supplied contexts are unaffected: reset
 * is a no-op for those.)
 */
static void
maybe_reset_comment_context(void)
{
	const OtelInternalApi *api = otel_internal_api();
	OtelRootContext rc;

	if (api == NULL)
		return;

	api->get_root_context(&rc);
	if (rc.from_comment)
		api->reset_root_context();
}

/*
 * Publish this span's context for any parallel workers this backend
 * spawns while it is the innermost active span.  Cleared at finalize.
 */
static void
publish_leader_context(OtelSpanRef s)
{
	const OtelInternalApi *api = otel_internal_api();
	OtelSpanContext ctx;

	if (api == NULL || s.v == 0)
		return;

	if (otel_span_context_of(s, &ctx))
		api->parallel_publish_leader_context(&ctx);
}

/*
 * After a statement span ends, publish the enclosing statement span (if
 * any) again, so parallel workers launched by an outer statement after a
 * nested one has finished still find their parent.
 */
static void
restore_leader_context(void)
{
	const OtelInternalApi *api = otel_internal_api();

	if (api == NULL)
		return;
	if (stmt_stack_depth > 0)
		publish_leader_context(stmt_stack[stmt_stack_depth - 1].ref);
	else
		api->parallel_clear_leader_context();
}

/*
 * start_stmt_span --- start a statement-level span, or return
 * OTEL_SPAN_NONE if nothing should be recorded.
 *
 * otel_span_start() does its own sampling (at start) and its own
 * parent resolution (active stack, else parallel-leader context in a
 * worker, else the backend's root context).  The only gate this file
 * still applies is trace_all_queries semantics: don't even ask to
 * start a span when there is no trace context to join and
 * otel.trace_all_queries is off, since otherwise otel_span_start()
 * would begin a brand-new (unwanted) root trace.
 */
static OtelSpanRef
start_stmt_span(const char *name, const char *query_text, uint64 query_id,
				bool detached)
{
	OtelSpanContext ctx;
	bool		have_ctx;
	OtelSpanRef s;

	have_ctx = otel_span_context_of(OTEL_SPAN_NONE, &ctx);
	if (!have_ctx && !otel_trace_all_queries)
		return OTEL_SPAN_NONE;

	/*
	 * The outermost statement span is the server span for the client's
	 * request: its parent is the client's context (or none), not
	 * whatever SDT bridge span is open.  Nested statements, and parallel
	 * workers, take the active parent.
	 */
	if (stmt_stack_depth == 0 && !IsParallelWorker())
	{
		const OtelInternalApi *iapi = otel_internal_api();
		OtelRootContext rc = {0};

		if (iapi)
			iapi->get_root_context(&rc);
		s = otel_span_start(.tracer = &otel_pg_tracer,
							.name = name,
							.kind = OTEL_SPAN_KIND_SERVER,
							.parent = rc.is_set ? OTEL_PARENT_CONTEXT : OTEL_PARENT_ROOT,
							.parent_ctx = rc.is_set ? &rc.ctx : NULL,
							.detached = detached,
							.force_sample = otel_trace_all_queries);
	}
	else
		s = otel_span_start(.tracer = &otel_pg_tracer,
							.name = name,
							.kind = OTEL_SPAN_KIND_SERVER,
							.detached = detached,
							.force_sample = otel_trace_all_queries);
	if (s.v == 0)
		return s;

	otel_span_set_str(s, OTEL_SC_DB_SYSTEM_NAME, OTEL_SC_DB_SYSTEM_POSTGRESQL);

	/*
	 * Never get_database_name(): it reads the catalog and allocates, and
	 * this span may be starting outside a valid transaction state (e.g.
	 * the ROLLBACK/ROLLBACK TO SAVEPOINT that recovers from an error --
	 * get_database_name() asserts IsTransactionState() and would crash a
	 * cassert backend there). MyProcPort->database_name is the name the
	 * client connected with, already verified against pg_database by
	 * InitPostgres, and costs nothing to read. A backend with no Port
	 * (e.g. a background worker) has no catalog-free source for the
	 * name; the attribute is just omitted for it. See the proposed core
	 * MyDatabaseName global in docs/plans/future-core-improvements.md.
	 */
	if (MyProcPort && MyProcPort->database_name)
		otel_span_set_str(s, OTEL_SC_DB_NAMESPACE, MyProcPort->database_name);

	if (OidIsValid(MyDatabaseId))
		otel_span_set_int(s, OTEL_PG_DATABASE_OID, (int64) MyDatabaseId);

	if (query_text)
		OTEL_SPAN_SET_STR_IF_RECORDING(s, OTEL_SC_DB_QUERY_TEXT, query_text);

	if (MyProcPort && MyProcPort->user_name)
		otel_span_set_str(s, OTEL_PG_SESSION_USER, MyProcPort->user_name);

	if (MyProcPort && MyProcPort->remote_host)
		otel_span_set_str(s, OTEL_SC_CLIENT_ADDRESS, MyProcPort->remote_host);

	if (application_name && application_name[0])
		otel_span_set_str(s, OTEL_PG_APPLICATION_NAME, application_name);

	if (query_id != UINT64CONST(0))
		otel_span_set_int(s, OTEL_PG_QUERY_ID, (int64) query_id);

	publish_leader_context(s);

#ifdef PG_HAVE_SDT_PROBE_HOOK
	/*
	 * Bidirectionally link this statement span to the enclosing pg.txn
	 * span (emitted by the SDT bridge in its own trace).  Unlike the
	 * bridge's pg.query linking, this fires even with no propagated
	 * traceparent, so statements run by clients that don't inject
	 * trace context are still associated with their transaction.
	 */
	{
		OtelSpanContext txn_ctx;

		if (otel_sdt_get_txn_context(&txn_ctx))
		{
			OtelSpanContext my_ctx;

			otel_span_add_link(s, &txn_ctx);
			if (otel_span_context_of(s, &my_ctx))
				otel_sdt_link_stmt_to_txn(&my_ctx);
		}
	}
#endif							/* PG_HAVE_SDT_PROBE_HOOK */

	return s;
}

static void
finalize_stmt_span(OtelSpanRef s)
{
	if (s.v == 0)
		return;

	otel_span_end(s);
	restore_leader_context();
	maybe_reset_comment_context();
}

/*
 * ExecutorStart hook.
 */
static void
otel_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	OtelSpanRef s;
	bool		is_cursor = otel_active_portal_is_declaring_cursor();

	if (queryDesc != NULL)
		maybe_apply_sqlcommenter(queryDesc->sourceText);

	/*
	 * The command-tag-derived name is superseded below by a fixed
	 * "pgsql.execute" (see the historical TODO on span naming in
	 * otel_postgres_tracing's design notes: a clear HOOK-based vs.
	 * INTERCEPTED-tracepoint span-source discriminator is still owed;
	 * OTEL_PG_SPAN_SOURCE is available for that once adopted here).
	 *
	 * A cursor's span (ActivePortal matching otel_declaring_cursor_name,
	 * set by otel_ProcessUtility for DECLARE CURSOR -- not just "are we
	 * somewhere inside its ProcessUtility call", which would also wrongly
	 * match a nested statement run while planning the cursor's query) is
	 * .detached: it must
	 * outlive this call, staying open across every FETCH up to CLOSE,
	 * so it cannot sit on otel_api's active stack the way an ordinary
	 * statement span does.  otel_ExecutorRun() activates it only for
	 * the duration of each FETCH's run.
	 */
	s = start_stmt_span("pgsql.execute",
						queryDesc ? queryDesc->sourceText : NULL,
						queryDesc && queryDesc->plannedstmt
						? (uint64) queryDesc->plannedstmt->queryId : 0,
						is_cursor);

	if (queryDesc != NULL)
		push_stmt_span(queryDesc, s, is_cursor);

	/*
	 * If any group-A feature is enabled, turn on per-node Instrumentation
	 * for this query so collectors can read timing/row counts at
	 * ExecutorEnd.  Must happen BEFORE standard_ExecutorStart so the
	 * executor allocates Instrumentation nodes.
	 */
	if (otel_span_recording(s) && otel_planwalk_want_instrumentation())
		queryDesc->instrument_options |= INSTRUMENT_TIMER | INSTRUMENT_ROWS |
			INSTRUMENT_BUFFERS | INSTRUMENT_WAL;

	/* Chain. */
	if (prev_ExecutorStart_hook)
		prev_ExecutorStart_hook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	/*
	 * Drive the planstate-walker dispatcher now that the planstate tree
	 * exists.  Gated on the span recording so unsampled queries skip the
	 * walk entirely.
	 */
	if (otel_span_recording(s))
	{
		if (stmt_attr_cxt == NULL)
			stmt_attr_cxt = AllocSetContextCreate(TopMemoryContext,
												  "otel_stmt_attr_cxt",
												  ALLOCSET_SMALL_SIZES);
		else
			MemoryContextReset(stmt_attr_cxt);

		otel_planwalk_executor_start(queryDesc, s, stmt_attr_cxt);
		otel_planshape_executor_start(queryDesc, s, stmt_attr_cxt);
	}
}

/*
 * ExecutorRun hook.  For an ordinary statement, its own executor span
 * is already current (pushed, non-.detached, by otel_ExecutorStart), so
 * there is nothing to do here.  For a cursor's portal -- its span is
 * .detached, so it is NOT on the active stack between DECLARE and
 * CLOSE -- activate it for just this one FETCH's run, so work done
 * while materialising rows (e.g. a volatile function called from the
 * target list) parents to the cursor span instead of whatever
 * unrelated span happens to be current (e.g. the "FETCH" utility
 * span, or nothing).  Deactivated in PG_FINALLY so a FETCH that
 * errors still leaves the active stack clean.
 */
static void
otel_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count)
{
	OtelSpanRef cursor_span = OTEL_SPAN_NONE;
	OtelActivation act = OTEL_ACTIVATION_NONE;
	bool		is_cursor;

	is_cursor = queryDesc != NULL && peek_cursor_span(queryDesc, &cursor_span);
	if (is_cursor)
		act = otel_span_activate(cursor_span);

	PG_TRY();
	{
		if (prev_ExecutorRun_hook)
			prev_ExecutorRun_hook(queryDesc, direction, count);
		else
			standard_ExecutorRun(queryDesc, direction, count);
	}
	PG_FINALLY();
	{
		if (is_cursor)
			otel_span_deactivate(act);
	}
	PG_END_TRY();
}

static void
otel_ExecutorEnd(QueryDesc *queryDesc)
{
	OtelSpanRef s = pop_stmt_span(queryDesc);

	/*
	 * Drive the planstate-walker end-walk before standard_ExecutorEnd
	 * frees the planstate nodes, and before the span is ended (child
	 * spans must be emitted first).
	 */
	otel_planwalk_executor_end(queryDesc, s, stmt_attr_cxt);

	if (prev_ExecutorEnd_hook)
		prev_ExecutorEnd_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);

	finalize_stmt_span(s);
}

/*
 * ProcessUtility hook --- spans for utility commands (BEGIN, COMMIT,
 * COPY, DDL, EXPLAIN, etc.) that don't go through the executor.
 *
 * Only starts a span for PROCESS_UTILITY_TOPLEVEL invocations; nested
 * ProcessUtility calls (from another utility statement) get their own
 * span too now (they're no longer suppressed), nested under the outer
 * one via the API's active stack.
 */
static void
otel_ProcessUtility(PlannedStmt *pstmt,
					const char *queryString,
					bool readOnlyTree,
					ProcessUtilityContext context,
					ParamListInfo params,
					QueryEnvironment *queryEnv,
					DestReceiver *dest,
					QueryCompletion *qc)
{
	/* Unique per call (including recursive/nested invocations): the
	 * address of this frame-local variable. */
	char		call_token;
	OtelSpanRef s = OTEL_SPAN_NONE;
	DeclareCursorStmt *cstmt = (pstmt->utilityStmt &&
								 IsA(pstmt->utilityStmt, DeclareCursorStmt))
		? (DeclareCursorStmt *) pstmt->utilityStmt : NULL;
	const char *save_declaring_cursor_name = otel_declaring_cursor_name;

	if (context == PROCESS_UTILITY_TOPLEVEL)
	{
		const char *name;

		maybe_apply_sqlcommenter(queryString);

		name = pstmt->utilityStmt
			? GetCommandTagName(CreateCommandTag(pstmt->utilityStmt))
			: "pgsql.utility";

		s = start_stmt_span(name, queryString, (uint64) pstmt->queryId, false);
		push_stmt_span(&call_token, s, false);
	}

	/*
	 * otel_ExecutorStart(), called synchronously below (from
	 * PerformCursorOpen()'s PortalStart() for a real DECLARE CURSOR,
	 * but also, earlier in this same call, from planning the cursor's
	 * query -- constant-folding an IMMUTABLE function that itself runs
	 * SQL, say -- for any nested statement that runs along the way),
	 * checks otel_active_portal_is_declaring_cursor() to start the
	 * cursor's own executor span .detached. Save/restore rather than
	 * assume it was NULL: a DECLARE CURSOR whose query itself somehow
	 * re-enters ProcessUtility (not expected, but this keeps a stray
	 * error path from leaving it stuck) must not clobber an enclosing
	 * one.
	 */
	if (cstmt != NULL)
		otel_declaring_cursor_name = cstmt->portalname;

	PG_TRY();
	{
		if (prev_ProcessUtility_hook)
			prev_ProcessUtility_hook(pstmt, queryString, readOnlyTree,
									 context, params, queryEnv,
									 dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString, readOnlyTree,
									context, params, queryEnv,
									dest, qc);
	}
	PG_CATCH();
	{
		otel_declaring_cursor_name = save_declaring_cursor_name;
		/* Re-throw; the span (if any) is ended, and exported with
		 * ERROR status, by resource-owner release.  Just keep our
		 * own bookkeeping consistent. */
		if (context == PROCESS_UTILITY_TOPLEVEL)
			(void) pop_stmt_span(&call_token);
		PG_RE_THROW();
	}
	PG_END_TRY();
	otel_declaring_cursor_name = save_declaring_cursor_name;

	if (context == PROCESS_UTILITY_TOPLEVEL)
	{
		OtelSpanRef popped = pop_stmt_span(&call_token);

		finalize_stmt_span(popped);
	}
}

/*
 * XactCallback: keep the bookkeeping stack in sync on abort.  The
 * spans themselves are ended, and exported with ERROR status, by
 * otel_api's own resource-owner release, so this only needs to drop
 * stale entries, not emit anything.
 */
static void
otel_pgtracing_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			stmt_stack_depth = 0;
			otel_fdw_reset();
			otel_planspans_reset();
			break;
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PREPARE:
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
			break;
	}
}

/*
 * SubXactCallback --- drop bookkeeping entries pushed at or below the
 * aborting subtransaction level; their spans are unwound by otel_api's
 * resource-owner release.  Also hands off to otel_fdw.c / otel_planspans.c
 * for their own tracking stacks.
 */
static void
otel_pgtracing_subxact_callback(SubXactEvent event,
								SubTransactionId mySubid,
								SubTransactionId parentSubid,
								void *arg)
{
	if (event == SUBXACT_EVENT_ABORT_SUB)
	{
		int			current_level = GetCurrentTransactionNestLevel();
		int			new_depth = 0;
		int			i;

		for (i = 0; i < stmt_stack_depth; i++)
		{
			if (stmt_stack[i].subxact_level < current_level)
			{
				if (new_depth != i)
					stmt_stack[new_depth] = stmt_stack[i];
				new_depth++;
			}
		}
		stmt_stack_depth = new_depth;
	}

	otel_fdw_subxact_abort(event);
	otel_planspans_subxact_abort(event);
}

/*
 * Defensive cleanup on backend exit: just drop our bookkeeping.  Any
 * still-open spans are otel_api's problem (session-span budget /
 * backend-exit release), not this module's.
 */
static void
otel_proc_exit_cb(int code, Datum arg)
{
	stmt_stack_depth = 0;
}
