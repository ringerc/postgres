/*-------------------------------------------------------------------------
 *
 * otel_trace.c
 *	  Span production hooks for contrib/otel.
 *
 * Owns the span lifecycle (start / finalize) and the executor hooks
 * that produce one OtelSpan per top-level statement.  When a span
 * is emitted, hands it to whichever exporter registered via
 * otel_span_emit_hook.
 *
 * Hot path:
 *	 ExecutorStart_hook -> early-bail or start_span(queryDesc)
 *	 ExecutorEnd_hook   -> finalize_span(OTEL_STATUS_UNSET)
 *
 * Error path (ExecutorEnd_hook is not called):
 *	 XACT_EVENT_ABORT   -> finalize_span(OTEL_STATUS_ERROR) if active
 *
 * Worst case:
 *	 on_proc_exit       -> defensively finalize any orphan span
 *
 * State lives in module-statics (span_storage, span_cxt, span_active).
 * Per-query allocation is limited to whatever overflow_attrs/events
 * we need, all in span_cxt which is reset between spans.
 *
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

#include "access/xact.h"
#include "commands/dbcommands.h"
#include "executor/executor.h"
#include "executor/nodeForeignscan.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "tcop/cmdtag.h"
#include "tcop/utility.h"
#include "utils/backend_status.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/guc.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

#include <otel_api/otel.h>
#include "otel_postgres_tracing.h"

/*
 * Span lifecycle state --- per backend.
 *
 * span_storage is a static slab reused for every span (no per-span
 * palloc).  span_active flags whether it currently holds a live
 * span.  span_cxt is a per-backend MemoryContext used for any
 * variable-length data we need to copy in (e.g. on the abort path
 * where the portal's memory may already be gone).  It is reset
 * between spans, never deleted.
 *
 * span_originator identifies which hook owns the active span, so the
 * matching hook is the one that finalizes it.  This matters for the
 * nested case (utility command that runs an executor underneath, e.g.
 * CTAS): the outer hook keeps ownership; the inner hook's End is a
 * no-op for the span.  Set when start_span() is called, cleared on
 * finalize_span().
 */
typedef enum SpanOriginator
{
	SPAN_ORIGIN_NONE = 0,
	SPAN_ORIGIN_EXECUTOR = 1,
	SPAN_ORIGIN_UTILITY = 2,
} SpanOriginator;

static OtelSpan span_storage;
static bool		span_active = false;
static SpanOriginator span_originator = SPAN_ORIGIN_NONE;
static MemoryContext span_cxt = NULL;

/* Phase 3: the saved_current_span_id_guc + restore machinery is
 * gone.  Parallel-worker propagation now uses the per-backend
 * shared-memory slot in otel_parallel.c; the leader publishes at
 * start_span and clears at finalize_span. */

/* Hook chains */
static ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;
static ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;
static ForeignScanBegin_hook_type prev_ForeignScanBegin_hook = NULL;
static ForeignScanEnd_hook_type prev_ForeignScanEnd_hook = NULL;

/*
 * Stack of in-flight pg.fdw.scan spans; fdw_scan_depth tracks how many
 * are active.  Multiple ForeignScan nodes can be open simultaneously
 * (nested-loop join over a foreign table, async FDW, etc.).
 */
#define OTEL_FDW_SCAN_STACK_MAX 16
static struct
{
	ForeignScanState *node;		/* key: the ForeignScanState pointer */
	OtelSpan		  span;		/* OTEL_UNWIND_DROP → static storage ok */
} fdw_scan_stack[OTEL_FDW_SCAN_STACK_MAX];
static int	fdw_scan_depth = 0;

static void otel_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void otel_ExecutorEnd(QueryDesc *queryDesc);
static void otel_ForeignScanBegin(ForeignScanState *node, int eflags);
static void otel_ForeignScanEnd(ForeignScanState *node);
static void otel_ProcessUtility(PlannedStmt *pstmt,
								const char *queryString,
								bool readOnlyTree,
								ProcessUtilityContext context,
								ParamListInfo params,
								QueryEnvironment *queryEnv,
								DestReceiver *dest,
								QueryCompletion *qc);
static void otel_pgtracing_xact_callback(XactEvent event, void *arg);
static void otel_proc_exit_cb(int code, Datum arg);
static OtelSamplerDecision decide_whether_to_record(const char *name_hint);
static void start_span(QueryDesc *queryDesc);
static void start_utility_span(PlannedStmt *pstmt, const char *queryString);
static void finalize_span(OtelSpanStatus status);
static void generate_span_id(char out[OTEL_SPAN_ID_LEN + 1]);
static void bytes_to_lower_hex(const unsigned char *src, size_t n, char *dst);
static void span_add_attr(const char *key, const char *value);
static OtelSpanEvent *acquire_event_slot(void);
static void capture_event_core(OtelEventCore *core, ErrorData *edata);
static void capture_event_extended(OtelSpanEvent *event, ErrorData *edata);
/* otel_emit_span_as_log_line: the JSON log-line fallback emitter
 * lives in contrib/otel; this module reaches it via the producer
 * dispatch when otel.emit_spans_to_log is on. */


/*
 * Install our executor hooks and register the xact + proc exit
 * callbacks.  Called once from _PG_init.
 */
void
otel_trace_install_hooks(void)
{
	prev_ExecutorStart_hook = ExecutorStart_hook;
	ExecutorStart_hook = otel_ExecutorStart;
	prev_ExecutorEnd_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = otel_ExecutorEnd;

	/* Utility-statement spans (step 4). */
	prev_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = otel_ProcessUtility;

	/* FDW scan spans. */
	prev_ForeignScanBegin_hook = ForeignScanBegin_hook;
	ForeignScanBegin_hook = otel_ForeignScanBegin;
	prev_ForeignScanEnd_hook = ForeignScanEnd_hook;
	ForeignScanEnd_hook = otel_ForeignScanEnd;

	RegisterXactCallback(otel_pgtracing_xact_callback, NULL);
	on_proc_exit(otel_proc_exit_cb, (Datum) 0);
}


/*
 * Generate a fresh 8-byte span id and write it lowercase-hex into out.
 * pg_strong_random is overkill for span IDs but it is the available
 * cryptographic-quality randomness primitive and the per-span cost
 * is irrelevant.
 */
static void
generate_span_id(char out[OTEL_SPAN_ID_LEN + 1])
{
	unsigned char buf[OTEL_SPAN_ID_LEN / 2];	/* 8 bytes = 16 hex chars */

	if (!pg_strong_random(buf, sizeof(buf)))
	{
		/* Falling back to less-random is acceptable; span IDs only
		 * need to be unique within a trace, not unguessable.  Use
		 * the time + pid as a fallback. */
		uint64		fallback = (uint64) MyProcPid ^ (uint64) GetCurrentTimestamp();

		memcpy(buf, &fallback, sizeof(buf));
	}
	bytes_to_lower_hex(buf, sizeof(buf), out);
}

static void
bytes_to_lower_hex(const unsigned char *src, size_t n, char *dst)
{
	static const char hex[] = "0123456789abcdef";
	size_t		i;

	for (i = 0; i < n; i++)
	{
		dst[i * 2]     = hex[(src[i] >> 4) & 0xf];
		dst[i * 2 + 1] = hex[src[i] & 0xf];
	}
	dst[n * 2] = '\0';
}

/*
 * Add an attribute to the current span.  Uses inline storage when
 * available; overflows into span_cxt when not.  Best-effort: on
 * allocation failure for overflow, the attribute is silently
 * dropped --- the span still emits with what got captured.
 *
 * key and value MUST remain valid for the lifetime of the span.
 * For borrowed pointers into long-lived backend state (db.system
 * literals, queryDesc->sourceText while the portal is alive, GUC
 * values, etc.) this is fine; for transient strings the caller
 * must arrange a copy itself.
 */
static void
span_add_attr(const char *key, const char *value)
{
	if (!span_active || value == NULL)
		return;

	if (span_storage.n_attrs < OTEL_INLINE_ATTRS)
	{
		span_storage.attrs[span_storage.n_attrs].key = key;
		span_storage.attrs[span_storage.n_attrs].value = value;
		span_storage.n_attrs++;
		return;
	}

	/* Overflow path - allocate inside span_cxt.  Best-effort: on
	 * allocation failure, silently drop. */
	PG_TRY();
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(span_cxt);
		int			newcnt = span_storage.n_overflow_attrs + 1;
		OtelKeyValue *newarr;

		if (span_storage.overflow_attrs == NULL)
			newarr = palloc(sizeof(OtelKeyValue) * newcnt);
		else
			newarr = repalloc(span_storage.overflow_attrs,
							  sizeof(OtelKeyValue) * newcnt);
		newarr[newcnt - 1].key = key;
		newarr[newcnt - 1].value = value;
		span_storage.overflow_attrs = newarr;
		span_storage.n_overflow_attrs = newcnt;
		MemoryContextSwitchTo(oldcxt);
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
}

/* Phase 3: update_current_span_id_guc and restore_current_span_id_guc
 * are gone.  Their replacement is the per-backend shared-memory slot
 * in otel_parallel.c: start_span calls
 * otel_api->parallel_publish_leader_context, finalize_span calls
 * otel_api->parallel_clear_leader_context. */

/*
 * Initialize span_storage for a new span and populate attributes.
 *
 * Caller should have already confirmed that a span SHOULD be
 * started (early-bail gates passed).  Sets span_active = true on
 * success; on any internal failure, leaves it unset and span_storage
 * in a clean state.
 */
static void
start_span(QueryDesc *queryDesc)
{
	const char *parent;

	Assert(!span_active);

	if (span_cxt == NULL)
	{
		/* Lazy: create the per-backend context on first use. */
		span_cxt = AllocSetContextCreate(TopMemoryContext,
										 "otel_span_cxt",
										 ALLOCSET_SMALL_SIZES);
	}
	else
	{
		MemoryContextReset(span_cxt);
	}

	memset(&span_storage, 0, sizeof(span_storage));
	span_storage.scope = otel_pg_tracer;

	/* Snapshot the root context (client-supplied via 'M' header or
	 * SET otel.traceparent or sqlcommenter parse). */
	{
		OtelRootContextSnapshot rc;
		otel_api->get_root_context_snapshot(&rc);

		/* Identity from propagated trace context if available; otherwise
		 * synthesize parentless (only happens when trace_all_queries is on).
		 *
		 * Parent-span selection:
		 *	 1. If we're a parallel worker AND our leader has a published
		 *	    SpanContext, use the leader's span_id as parent.  This
		 *	    overrides any client-propagated parent because the leader's
		 *	    current span is closer to us in the trace hierarchy.
		 *	 2. Otherwise, fall back to the client-propagated parent in
		 *	    the root context.
		 */
		if (rc.is_set)
		{
			OtelParallelContext leader_ctx;

			memcpy(span_storage.trace_id, rc.trace_id, sizeof(span_storage.trace_id));
			memcpy(span_storage.trace_flags, rc.trace_flags, sizeof(span_storage.trace_flags));
			if (otel_api->parallel_get_leader_context(&leader_ctx))
				parent = leader_ctx.parent_span_id;
			else
				parent = rc.span_id;
			strlcpy(span_storage.parent_span_id, parent,
					sizeof(span_storage.parent_span_id));
		}
		else
		{
			/* trace_all_queries path: synthesize a trace id too. */
			unsigned char buf[16];

			if (!pg_strong_random(buf, sizeof(buf)))
				memset(buf, 0xa5, sizeof(buf));
			bytes_to_lower_hex(buf, sizeof(buf), span_storage.trace_id);
			strcpy(span_storage.trace_flags, "00");
			span_storage.parent_span_id[0] = '\0';
		}

		generate_span_id(span_storage.span_id);

		span_storage.tracestate = rc.tracestate;
	}

	span_storage.name = GetCommandTagName(queryDesc->operation == CMD_UNKNOWN
										  ? CMDTAG_UNKNOWN
										  : (CommandTag) queryDesc->operation);
	/* Note: queryDesc->operation is a CmdType, not a CommandTag enum;
	 * use a small mapping switch instead.  Fixing below via portal-state
	 * inspection would be cleaner; for the POC just use a generic name. */
	span_storage.name = "pgsql.execute";
	span_storage.kind = OTEL_SPAN_KIND_SERVER;
	span_storage.status = OTEL_STATUS_UNSET;
	span_storage.start_time = GetCurrentTimestamp();

	/*
	 * TODO: provide a clear, queryable way to distinguish HOOK-based spans
	 * (this file: ExecutorStart/End -> "pgsql.execute", ProcessUtility ->
	 * command-tag/"pgsql.utility") from INTERCEPTED-tracepoint spans
	 * (otel_sdt_bridge.c: pg.query/parse/rewrite/plan/execute/sort/smgr/txn).
	 * Today the only discriminator is the span-name convention, which is
	 * fragile and inconsistent:
	 *   - "pgsql.*" is meant to mean hook-based, but ProcessUtility spans are
	 *     named by raw command tag ("SET", "BEGIN", "CREATE TABLE AS", ...),
	 *     so they carry no "pgsql." prefix at all;
	 *   - the InstrumentationScope is supposed to separate them (otel_pg_tracer
	 *     here vs sdt_scope in the bridge) but the Rust exporter collapses every
	 *     span's ScopeName to the crate name ("postgres_otel_tracing_demo"), so
	 *     scope is useless for filtering downstream (verified in ClickHouse).
	 * Proposed fix: set an explicit span attribute on every span at creation,
	 * e.g. pg.otel.span_source = "executor_hook" | "process_utility_hook" |
	 * "sdt_probe", in BOTH producers; and/or fix the exporter so distinct
	 * InstrumentationScope names survive export, then document the
	 * scope->producer mapping. See the companion TODO in otel_sdt_bridge.c.
	 */

	/* Flip the active flag BEFORE populating attributes --- the
	 * attribute helpers check span_active and would silently no-op
	 * otherwise. */
	span_active = true;
	span_originator = SPAN_ORIGIN_EXECUTOR;

	/* Attributes --- borrowed pointers into long-lived state. */
	span_add_attr("db.system", "postgresql");

	if (MyDatabaseId != InvalidOid)
	{
		const char *dbname = get_database_name(MyDatabaseId);

		if (dbname)
			span_add_attr("db.name", dbname);
	}

	if (queryDesc->sourceText)
		span_add_attr("db.statement", queryDesc->sourceText);

	if (MyProcPort && MyProcPort->user_name)
		span_add_attr("db.user", MyProcPort->user_name);

	if (MyProcPort && MyProcPort->remote_host)
		span_add_attr("net.peer.addr", MyProcPort->remote_host);

	if (application_name && application_name[0])
		span_add_attr("application_name", application_name);

	/* Update the GUC for parallel-worker propagation. */
	/* Phase 3: publish our span's identity to the per-backend
	 * shared-memory slot so any parallel workers we spawn during
	 * this span will pick us up as parent.  Supersedes the
	 * otel.current_span_id GUC. */
	otel_api->parallel_publish_leader_context(span_storage.trace_id,
										 span_storage.span_id,
										 span_storage.trace_flags);

	/* Phase 2 migration: opt this span into emit-as-ERROR on
	 * ereport unwind, then push it onto the producer-side active
	 * stack.  Aborted statements now appear in traces with
	 * status = ERROR + descriptive reason rather than being
	 * silently dropped, and external producer-API consumers can
	 * read this span as their parent via api->span_current_context. */
	span_storage.unwind_policy = OTEL_UNWIND_ERROR;
	otel_api->span_push(&span_storage);

#ifdef PG_HAVE_SDT_PROBE_HOOK
	/*
	 * Bidirectionally link this statement span to the enclosing pg.txn span
	 * (emitted by the SDT bridge in its own trace).  Unlike the bridge's
	 * pg.query linking, this fires even with no propagated traceparent, so
	 * statements run by clients that don't inject trace context (e.g.
	 * cnp_metrics_exporter) are still associated with their transaction.
	 */
	{
		OtelSpanContext txn_ctx;

		if (otel_sdt_get_txn_context(&txn_ctx))
		{
			otel_span_add_link(&span_storage, txn_ctx.trace_id,
							   txn_ctx.span_id, txn_ctx.trace_flags);
			otel_sdt_link_stmt_to_txn(span_storage.trace_id,
									  span_storage.span_id,
									  span_storage.trace_flags);
		}
	}
#endif							/* PG_HAVE_SDT_PROBE_HOOK */
}

static void
finalize_span(OtelSpanStatus status)
{
	if (!span_active)
		return;

	span_storage.end_time = GetCurrentTimestamp();

	/* If status was already set to ERROR by an event capture, keep
	 * it; otherwise apply the caller-supplied status. */
	if (span_storage.status == OTEL_STATUS_UNSET)
		span_storage.status = status;

	/* Phase 2 migration: emit through the producer-side dispatch
	 * which pops the stack and calls the registered exporter hook
	 * + the JSON-log fallback.  Equivalent to the inline dispatch
	 * this code used to do, but goes through the same path
	 * external consumers use. */
	otel_api->span_emit(&span_storage);

	span_active = false;
	span_originator = SPAN_ORIGIN_NONE;
	/* Phase 3: clear our published context so any workers spawned
	 * AFTER this span ends (in a future query) don't read a stale
	 * value. */
	otel_api->parallel_clear_leader_context();

	/*
	 * Statement-scoped scrub for comment-derived context: a
	 * sqlcommenter traceparent applies to ONE statement and must
	 * not bleed into the next.  Reset root context now.  ('M' /
	 * GUC paths are not affected; reset is a no-op for those.)
	 */
	{
		OtelRootContextSnapshot rc;
		otel_api->get_root_context_snapshot(&rc);
		if (rc.from_comment)
			otel_api->reset_root_context();
	}
}

/*
 * decide_whether_to_record --- the consolidated sampling decision.
 *
 * Returns OTEL_SAMPLE_DROP to skip span creation entirely (the
 * caller MUST NOT allocate after seeing DROP).  Returns
 * RECORD_AND_SAMPLE for the upstream-positively-sampled and
 * trace_all_queries paths.  Returns RECORD_ONLY or
 * RECORD_AND_SAMPLE per the sampler hook for paths where the
 * registered policy delegates to it.
 *
 * Decision order, expressed as gates:
 *
 *	  1. No provider (otel_api absent) -> DROP (silent no-op).
 *	  2. No consumer (no exporter hook AND log emission disabled)
 *		 -> DROP.  Backends not actively tracing pay only the two
 *		 pointer/bool reads at this gate.
 *	  3. otel.trace_all_queries -> RECORD_AND_SAMPLE.  Always-on
 *		 mode bypasses propagated state.
 *	  4. No propagated context (otel_ctx.is_set == false) -> DROP.
 *	  5-7. Policy-dependent.  See OtelSamplerHookPolicy in otel.h
 *		 for the four regimes; the dispatch below applies them.
 *
 * Per W3C TraceContext Level 1 §3.2.2.1 the unset sampled bit is
 * advisory and not a binding directive against recording; OTel SDK
 * convention is stricter.  Our default policy adopts the OTel
 * convention; exporters can override via api->set_sampler_policy.
 */
static OtelSamplerDecision
decide_whether_to_record(const char *name_hint)
{
	const OtelTracingApi *api;
	OtelRootContextSnapshot rc;

	/* Gate 1: no provider */
	api = otel_pg_ensure();
	if (api == NULL)
		return OTEL_SAMPLE_DROP;

	/* Gate 2: no consumer */
	if (!api->any_emit_consumer_present())
		return OTEL_SAMPLE_DROP;

	/* Gate 3: force-on overrides propagation entirely */
	if (otel_trace_all_queries)
		return OTEL_SAMPLE_RECORD_AND_SAMPLE;

	/* Gate 4: no propagated context */
	api->get_root_context_snapshot(&rc);
	if (!rc.is_set)
		return OTEL_SAMPLE_DROP;

	/* Gates 5-7: policy-driven dispatch handled inside the api. */
	{
		OtelSamplerInput in;

		in.trace_id = rc.trace_id;
		in.parent_span_id = rc.span_id;
		in.trace_flags = rc.trace_flags;
		in.tracestate = rc.tracestate;
		in.name = name_hint;
		in.kind = OTEL_SPAN_KIND_SERVER;

		return api->compute_sampler_decision(&in, rc.sampled_flag_set);
	}
}

/*
 * ExecutorStart hook.  Gated by the early-bail checks; only starts
 * a span when there's actually a consumer AND something to record.
 */
static void
otel_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	OtelSamplerDecision decision;
	const OtelTracingApi *api;

	/*
	 * If no in-memory context yet (no 'M' header, no SET) AND
	 * sqlcommenter parsing is enabled (checked inside the api),
	 * try the SQL text.  No-op when sqlcommenter is disabled or
	 * the provider is absent.
	 */
	api = otel_pg_ensure();
	if (api != NULL)
	{
		OtelRootContextSnapshot rc_pre;
		api->get_root_context_snapshot(&rc_pre);
		if (!rc_pre.is_set && queryDesc != NULL)
			(void) api->try_apply_sqlcommenter_context(queryDesc->sourceText);
	}

	decision = decide_whether_to_record("pgsql.execute");

	/* Defensive: never overlap. */
	if (decision != OTEL_SAMPLE_DROP && !span_active)
	{
		PG_TRY();
		{
			start_span(queryDesc);
			span_storage.sampler_decision = decision;
		}
		PG_CATCH();
		{
			FlushErrorState();
			span_active = false;
		}
		PG_END_TRY();
	}

	/* Chain. */
	if (prev_ExecutorStart_hook)
		prev_ExecutorStart_hook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}

static void
otel_ExecutorEnd(QueryDesc *queryDesc)
{
	/*
	 * Run cleanup first so that FDW-scan and other child spans (pushed
	 * during ExecInitForeignScan / ExecEndForeignScan) are finalized
	 * before we emit the enclosing pgsql.execute span.  Emitting the
	 * parent while children are still on the producer stack would
	 * trigger an out-of-order-emit warning and silently drop the children.
	 */
	if (prev_ExecutorEnd_hook)
		prev_ExecutorEnd_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);

	/* Only finalize if this hook started the active span.  If a
	 * utility command started the span (CTAS, etc.) and the
	 * executor is running underneath it, the utility's hook owns
	 * the span lifecycle and the executor's End should be a no-op
	 * for the span. */
	if (span_originator == SPAN_ORIGIN_EXECUTOR)
		finalize_span(OTEL_STATUS_UNSET);
}

/*
 * ForeignScanBegin hook --- open a pg.fdw.scan span for each ForeignScan
 * node that enters execution.  Multiple nodes can be active at the same
 * time (nested-loop join probing a foreign table, async FDW, etc.), so
 * we maintain a fixed-depth stack keyed by the ForeignScanState pointer.
 *
 * The span is a child of whatever span is currently on the producer
 * active stack (typically the enclosing pgsql.execute or pg.execute SDT
 * span).  We use OTEL_SPAN_KIND_CLIENT because the backend is acting as
 * a client to an external data source.
 */
static void
otel_ForeignScanBegin(ForeignScanState *node, int eflags)
{
	const OtelTracingApi *api = otel_pg_ensure();
	OtelSpan   *span;

	if (api == NULL || fdw_scan_depth >= OTEL_FDW_SCAN_STACK_MAX)
	{
		if (prev_ForeignScanBegin_hook)
			prev_ForeignScanBegin_hook(node, eflags);
		return;
	}

	span = &fdw_scan_stack[fdw_scan_depth].span;
	fdw_scan_stack[fdw_scan_depth].node = node;

	api->span_init(span, otel_pg_tracer, "pg.fdw.scan", OTEL_SPAN_KIND_CLIENT);
	api->span_add_attribute_string(span, "db.system", "postgresql");

	if (node->ss.ss_currentRelation != NULL)
		api->span_add_attribute_string(span, "db.collection.name",
									   RelationGetRelationName(node->ss.ss_currentRelation));

	api->span_link_to_active_and_push(span);
	fdw_scan_depth++;

	if (prev_ForeignScanBegin_hook)
		prev_ForeignScanBegin_hook(node, eflags);
}

/*
 * ForeignScanEnd hook --- locate the matching entry on the stack (end
 * may be non-LIFO with async FDW), emit its span, then compact the stack.
 *
 * Uses otel_api_get() rather than otel_pg_ensure() because we are only
 * emitting an already-initialised span, not registering a new tracer scope.
 */
static void
otel_ForeignScanEnd(ForeignScanState *node)
{
	const OtelTracingApi *api = otel_api_get();
	int			i;

	if (prev_ForeignScanEnd_hook)
		prev_ForeignScanEnd_hook(node);

	for (i = fdw_scan_depth - 1; i >= 0; i--)
	{
		if (fdw_scan_stack[i].node == node)
		{
			if (api != NULL)
			{
				fdw_scan_stack[i].span.end_time = GetCurrentTimestamp();
				api->span_emit(&fdw_scan_stack[i].span);
			}

			/* Shift remaining entries down to fill the gap. */
			for (; i < fdw_scan_depth - 1; i++)
				fdw_scan_stack[i] = fdw_scan_stack[i + 1];
			fdw_scan_depth--;
			return;
		}
	}
	/* Not found: already unwound via MemoryContext callback on error. */
}

/*
 * start_utility_span --- populate span_storage for a utility command.
 *
 * Identity / parent-link mechanics are the same as start_span(), but
 * the name is the command tag of the utility statement and we record
 * span_originator = SPAN_ORIGIN_UTILITY so otel_ExecutorEnd knows not
 * to finalize this span when it later runs (e.g. for CTAS's
 * underlying SELECT).
 */
static void
start_utility_span(PlannedStmt *pstmt, const char *queryString)
{
	const char *parent;

	Assert(!span_active);

	if (span_cxt == NULL)
		span_cxt = AllocSetContextCreate(TopMemoryContext,
										 "otel_span_cxt",
										 ALLOCSET_SMALL_SIZES);
	else
		MemoryContextReset(span_cxt);

	memset(&span_storage, 0, sizeof(span_storage));
	span_storage.scope = otel_pg_tracer;

	{
		OtelRootContextSnapshot rc;
		otel_api->get_root_context_snapshot(&rc);

		if (rc.is_set)
		{
			OtelParallelContext leader_ctx;

			memcpy(span_storage.trace_id, rc.trace_id, sizeof(span_storage.trace_id));
			memcpy(span_storage.trace_flags, rc.trace_flags, sizeof(span_storage.trace_flags));
			if (otel_api->parallel_get_leader_context(&leader_ctx))
				parent = leader_ctx.parent_span_id;
			else
				parent = rc.span_id;
			strlcpy(span_storage.parent_span_id, parent,
					sizeof(span_storage.parent_span_id));
		}
		else
		{
			unsigned char buf[16];

			if (!pg_strong_random(buf, sizeof(buf)))
				memset(buf, 0xa5, sizeof(buf));
			bytes_to_lower_hex(buf, sizeof(buf), span_storage.trace_id);
			strcpy(span_storage.trace_flags, "00");
			span_storage.parent_span_id[0] = '\0';
		}

		generate_span_id(span_storage.span_id);

		span_storage.tracestate = rc.tracestate;
	}

	/*
	 * TODO: fix span nesting for utility statements that internally run a
	 * query (e.g. CREATE TABLE AS, EXPLAIN ANALYZE, DECLARE CURSOR). The SDT
	 * QUERY_* probes for the inner query fire and grab the propagated root as
	 * their parent BEFORE this ProcessUtility span is pushed onto the active
	 * span stack, so the probe span (pg.execute) and this hook span end up as
	 * overlapping SIBLINGS under the root instead of probe-nested-under-hook.
	 * Observed: trace 30c5b7fd... -- "CREATE TABLE AS" and "pg.execute" both
	 * parent to the injected root and span the same ~264ms range. Contrast a
	 * plain SELECT, which nests correctly (ExecutorStart_hook pushes
	 * pgsql.execute before the executor runs). Fix: push this utility span
	 * onto the producer active-span stack before calling prev_ProcessUtility
	 * (which runs the inner query / fires the probes). See the span-stack
	 * contract in otel_api/otel_producer.c.
	 */
	/* Use the utility statement's command tag as the span name.
	 * GetCommandTagName returns a pointer into rodata --- safe to
	 * borrow without copying. */
	span_storage.name = pstmt->utilityStmt
		? GetCommandTagName(CreateCommandTag(pstmt->utilityStmt))
		: "pgsql.utility";
	span_storage.kind = OTEL_SPAN_KIND_SERVER;
	span_storage.status = OTEL_STATUS_UNSET;
	span_storage.start_time = GetCurrentTimestamp();

	span_active = true;
	span_originator = SPAN_ORIGIN_UTILITY;

	span_add_attr("db.system", "postgresql");
	if (MyDatabaseId != InvalidOid)
	{
		const char *dbname = get_database_name(MyDatabaseId);

		if (dbname)
			span_add_attr("db.name", dbname);
	}
	if (queryString)
		span_add_attr("db.statement", queryString);
	if (MyProcPort && MyProcPort->user_name)
		span_add_attr("db.user", MyProcPort->user_name);
	if (MyProcPort && MyProcPort->remote_host)
		span_add_attr("net.peer.addr", MyProcPort->remote_host);
	if (application_name && application_name[0])
		span_add_attr("application_name", application_name);

	/* Phase 3: publish to per-backend slot for parallel workers
	 * (see start_span() for the equivalent call). */
	otel_api->parallel_publish_leader_context(span_storage.trace_id,
										 span_storage.span_id,
										 span_storage.trace_flags);

	/* Phase 2 migration: see start_span() above. */
	span_storage.unwind_policy = OTEL_UNWIND_ERROR;
	otel_api->span_push(&span_storage);

#ifdef PG_HAVE_SDT_PROBE_HOOK
	/* Link this utility span to the enclosing pg.txn span; see start_span().
	 * A utility span is linked when a transaction span is active as its span
	 * starts -- which includes BEGIN, since the SDT TRANSACTION_START probe
	 * fires before this ProcessUtility span starts.  A standalone SET/SHOW
	 * outside any transaction has no active pg.txn and is therefore not
	 * linked. */
	{
		OtelSpanContext txn_ctx;

		if (otel_sdt_get_txn_context(&txn_ctx))
		{
			otel_span_add_link(&span_storage, txn_ctx.trace_id,
							   txn_ctx.span_id, txn_ctx.trace_flags);
			otel_sdt_link_stmt_to_txn(span_storage.trace_id,
									  span_storage.span_id,
									  span_storage.trace_flags);
		}
	}
#endif							/* PG_HAVE_SDT_PROBE_HOOK */
}

/*
 * ProcessUtility hook --- spans for utility commands (BEGIN, COMMIT,
 * COPY, DDL, EXPLAIN, etc.) that don't go through the executor.
 *
 * Only fires for PROCESS_UTILITY_TOPLEVEL to avoid creating spans
 * for recursive ProcessUtility invocations from inside another
 * utility statement.
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
	OtelSamplerDecision decision;
	bool		started_here = false;

	/* Only consult the sampler for top-level commands; nested utility
	 * invocations (e.g. from EXPLAIN, CTAS) share the outer span. */
	if (context == PROCESS_UTILITY_TOPLEVEL && !span_active)
	{
		/* sqlcommenter fallback --- see equivalent block in
		 * otel_ExecutorStart for rationale.  No-op when provider absent. */
		{
			const OtelTracingApi *api2 = otel_pg_ensure();

			if (api2 != NULL)
			{
				OtelRootContextSnapshot rc_pre;
				api2->get_root_context_snapshot(&rc_pre);
				if (!rc_pre.is_set)
					(void) api2->try_apply_sqlcommenter_context(queryString);
			}
		}

		decision = decide_whether_to_record("pgsql.utility");
		if (decision != OTEL_SAMPLE_DROP)
		{
			PG_TRY();
			{
				start_utility_span(pstmt, queryString);
				span_storage.sampler_decision = decision;
				started_here = true;
			}
			PG_CATCH();
			{
				FlushErrorState();
				span_active = false;
				span_originator = SPAN_ORIGIN_NONE;
			}
			PG_END_TRY();
		}
	}

	/* Chain to the previous hook (or standard) and finalize on
	 * normal return.  On error, XactCallback ABORT handles
	 * finalization. */
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
		/* Re-throw; abort callback will finalize the span. */
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Success path: only finalize spans we own here. */
	if (started_here && span_originator == SPAN_ORIGIN_UTILITY)
		finalize_span(OTEL_STATUS_UNSET);
}

/*
 * XactCallback for the error path: if a span survived past where
 * ExecutorEnd would have fired (because the transaction aborted),
 * emit it now with ERROR status.
 */
static void
otel_pgtracing_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			finalize_span(OTEL_STATUS_ERROR);
			/* FDW scan spans are dropped by the otel_api MemoryContext
			 * callbacks on error unwind; reset our depth counter here. */
			fdw_scan_depth = 0;
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
 * Defensive flush on backend exit.  Span lives in static storage,
 * so this is mainly to invoke the exporter one last time if a span
 * was somehow left active.
 */
static void
otel_proc_exit_cb(int code, Datum arg)
{
	if (span_active)
		finalize_span(OTEL_STATUS_ERROR);
}


/* ====================================================================
 * Event capture --- called from the emit_log_hook in otel_log.c.
 * ==================================================================== */

/*
 * Find or allocate an event slot on the current span.
 *
 * Returns the inline_event slot on first use (no allocation needed).
 * On subsequent calls, tries to grow the overflow_events array.
 * Returns NULL if the inline slot is already used and the overflow
 * allocation failed --- caller still updates span status.
 */
static OtelSpanEvent *
acquire_event_slot(void)
{
	OtelSpanEvent *slot = NULL;

	if (!span_storage.inline_event_used)
	{
		span_storage.inline_event_used = true;
		memset(&span_storage.inline_event, 0, sizeof(span_storage.inline_event));
		return &span_storage.inline_event;
	}

	PG_TRY();
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(span_cxt);
		int			newcnt = span_storage.n_overflow_events + 1;
		OtelSpanEvent *newarr;

		if (span_storage.overflow_events == NULL)
			newarr = palloc(sizeof(OtelSpanEvent) * newcnt);
		else
			newarr = repalloc(span_storage.overflow_events,
							  sizeof(OtelSpanEvent) * newcnt);
		/* Zero the new slot; older slots are already populated. */
		memset(&newarr[newcnt - 1], 0, sizeof(OtelSpanEvent));
		span_storage.overflow_events = newarr;
		span_storage.n_overflow_events = newcnt;
		slot = &span_storage.overflow_events[newcnt - 1];
		MemoryContextSwitchTo(oldcxt);
	}
	PG_CATCH();
	{
		FlushErrorState();
		slot = NULL;
	}
	PG_END_TRY();

	return slot;
}

/*
 * Populate the core of an event from ErrorData.  Always succeeds; no
 * allocation, no failure modes.
 */
static void
capture_event_core(OtelEventCore *core, ErrorData *edata)
{
	const char *sqlstate = unpack_sql_state(edata->sqlerrcode);

	core->time = GetCurrentTimestamp();
	core->elevel = edata->elevel;
	/* sqlstate is 5 chars + NUL; unpack_sql_state returns a pointer
	 * to a static buffer.  Copy by value. */
	memcpy(core->sqlstate, sqlstate, 6);
	core->filename = edata->filename;	/* points at __FILE__ literal */
	core->lineno = edata->lineno;
	core->funcname = edata->funcname;	/* points at __func__ literal */
}

/*
 * Populate the optional extended fields of an event.  Best-effort:
 * any individual field may end up NULL if its allocation fails.
 * Caller wraps in PG_TRY for the harder failure modes.
 */
static void
capture_event_extended(OtelSpanEvent *event, ErrorData *edata)
{
	MemoryContext oldcxt = MemoryContextSwitchTo(span_cxt);

	if (edata->message)
		event->message = pstrdup(edata->message);
	if (edata->detail)
		event->detail = pstrdup(edata->detail);
	if (edata->hint)
		event->hint = pstrdup(edata->hint);

	/*
	 * TODO: full log event capture — add the remaining ErrorData fields that
	 * the OTel log semantic conventions expect but we currently drop:
	 *   edata->context       (PL/pgSQL and other call-context chain)
	 *   edata->internalquery (internal query text, e.g. SPI calls)
	 *   edata->cursorpos / edata->internalpos  (byte offsets in those queries)
	 *   edata->schema_name, edata->table_name, edata->column_name,
	 *   edata->datatype_name, edata->constraint_name  (catalog-object detail)
	 * These should be stored in event->attrs (OtelKeyValue) using the standard
	 * OTel log attribute names (exception.stacktrace for context, db.sql.table,
	 * etc.) and gated on a GUC (otel.log_event_detail_level or similar) so
	 * operators can cap memory use for high-volume error workloads.
	 * Also consider: capturing events below WARNING when a trace is active and
	 * the user has opted in (e.g. otel.log_event_min_level = notice).
	 */

	MemoryContextSwitchTo(oldcxt);
}

/*
 * Capture an ereport as an event on the currently-active span (if any).
 * No-op when no span is active, or when elevel is below WARNING.
 *
 * Core fields (sqlstate, filename, lineno) are always captured
 * without allocation.  Extended fields (message, detail, hint) are
 * captured best-effort and skipped under OOM / FATAL+ to avoid
 * re-entering the allocator from the error-handling path.  Span
 * status is set to ERROR on ereport elevel >= ERROR.
 *
 * Exposed via otel_internal.h so otel_log.c can call it without
 * needing visibility into span_storage / span_active.
 */
void
otel_span_record_log_event(ErrorData *edata)
{
	OtelSpanEvent *event;

	if (!span_active || edata->elevel < WARNING)
		return;

	event = acquire_event_slot();

	if (event != NULL)
	{
		/* Core is always populated --- no allocation needed. */
		capture_event_core(&event->core, edata);

		/* Extended capture: skip entirely under OOM/FATAL+ to
		 * avoid re-entering the allocator from the error path. */
		if (edata->sqlerrcode != ERRCODE_OUT_OF_MEMORY &&
			edata->elevel < FATAL)
		{
			PG_TRY();
			{
				capture_event_extended(event, edata);
			}
			PG_CATCH();
			{
				/* Extended fields stay NULL; core is intact. */
				FlushErrorState();
			}
			PG_END_TRY();
		}
	}

	/* Span status reflects the error regardless of whether the
	 * event itself was captured. */
	if (edata->elevel >= ERROR)
		span_storage.status = OTEL_STATUS_ERROR;
}
