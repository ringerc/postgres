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
 *	  contrib/otel/otel_trace.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "access/xact.h"
#include "executor/executor.h"
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
#include "utils/timestamp.h"

#include "otel.h"
#include "otel_internal.h"

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

/*
 * To restore otel.current_span_id at ExecutorEnd we save what it was
 * before our span started.  Only one slot --- nested spans are not
 * tracked separately in this POC; they keep the outermost
 * current_span_id GUC and effectively all share the same parent.
 */
static char	saved_current_span_id_guc[OTEL_SPAN_ID_LEN + 1];
static bool	saved_current_span_id_set = false;

/* Hook chains */
static ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;
static ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;

static void otel_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void otel_ExecutorEnd(QueryDesc *queryDesc);
static void otel_ProcessUtility(PlannedStmt *pstmt,
								const char *queryString,
								bool readOnlyTree,
								ProcessUtilityContext context,
								ParamListInfo params,
								QueryEnvironment *queryEnv,
								DestReceiver *dest,
								QueryCompletion *qc);
static void otel_xact_callback(XactEvent event, void *arg);
static void otel_proc_exit_cb(int code, Datum arg);
static OtelSamplerDecision decide_whether_to_record(const char *name_hint);
static void start_span(QueryDesc *queryDesc);
static void start_utility_span(PlannedStmt *pstmt, const char *queryString);
static void finalize_span(OtelSpanStatus status);
static void generate_span_id(char out[OTEL_SPAN_ID_LEN + 1]);
static void bytes_to_lower_hex(const unsigned char *src, size_t n, char *dst);
static void span_add_attr(const char *key, const char *value);
static void update_current_span_id_guc(const char *new_value);
static void restore_current_span_id_guc(void);
static OtelSpanEvent *acquire_event_slot(void);
static void capture_event_core(OtelEventCore *core, ErrorData *edata);
static void capture_event_extended(OtelSpanEvent *event, ErrorData *edata);
static void emit_span_as_log_line(const OtelSpan *span);


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

	RegisterXactCallback(otel_xact_callback, NULL);
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
			newarr = palloc(sizeof(OtelKeyValue) * 4);
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

/*
 * Save the prior otel.current_span_id GUC, then set it to the
 * leader's new span_id so any parallel workers spawned during this
 * operation will use the leader's span as their parent.
 *
 * Wrapped in PG_TRY: GUC writes can allocate, and per the
 * best-effort principle we never propagate a tracing-side error
 * upward.
 */
static void
update_current_span_id_guc(const char *new_value)
{
	PG_TRY();
	{
		/* save what was there for restoration at span end */
		if (otel_current_span_id_guc && otel_current_span_id_guc[0])
			strlcpy(saved_current_span_id_guc, otel_current_span_id_guc,
					sizeof(saved_current_span_id_guc));
		else
			saved_current_span_id_guc[0] = '\0';
		saved_current_span_id_set = true;

		(void) set_config_option("otel.current_span_id",
								 (new_value && new_value[0]) ? new_value : NULL,
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SET, true, LOG, false);
	}
	PG_CATCH();
	{
		FlushErrorState();
		saved_current_span_id_set = false;
	}
	PG_END_TRY();
}

static void
restore_current_span_id_guc(void)
{
	if (!saved_current_span_id_set)
		return;
	PG_TRY();
	{
		(void) set_config_option("otel.current_span_id",
								 saved_current_span_id_guc[0]
								 ? saved_current_span_id_guc : NULL,
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SET, true, LOG, false);
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	saved_current_span_id_set = false;
}

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

	/* Identity from propagated trace context if available; otherwise
	 * synthesize parentless (only happens when trace_all_queries is on). */
	if (otel_ctx.is_set)
	{
		memcpy(span_storage.trace_id, otel_ctx.trace_id, sizeof(span_storage.trace_id));
		memcpy(span_storage.trace_flags, otel_ctx.trace_flags, sizeof(span_storage.trace_flags));
		/* Parent: prefer the GUC's current_span_id (set by an outer
		 * leader in a parallel-worker scenario); fall back to the
		 * client-propagated parent. */
		parent = (otel_current_span_id_guc && otel_current_span_id_guc[0])
			? otel_current_span_id_guc
			: otel_ctx.span_id;
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

	span_storage.tracestate = (otel_tracestate_guc && otel_tracestate_guc[0])
		? otel_tracestate_guc : NULL;

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
	update_current_span_id_guc(span_storage.span_id);
}

static void
finalize_span(OtelSpanStatus status)
{
	otel_span_emit_hook_type emit_hook;

	if (!span_active)
		return;

	span_storage.end_time = GetCurrentTimestamp();

	/* If status was already set to ERROR by an event capture, keep
	 * it; otherwise apply the caller-supplied status. */
	if (span_storage.status == OTEL_STATUS_UNSET)
		span_storage.status = status;

	emit_hook = otel_get_span_emit_hook();

	/* Hand off to exporter (best-effort, swallow errors). */
	PG_TRY();
	{
		if (emit_hook)
			emit_hook(&span_storage);

		if (otel_emit_spans_to_log)
			emit_span_as_log_line(&span_storage);
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();

	span_active = false;
	span_originator = SPAN_ORIGIN_NONE;
	restore_current_span_id_guc();

	/*
	 * Statement-scoped scrub for comment-derived context: a
	 * sqlcommenter traceparent applies to ONE statement and must
	 * not bleed into the next.  Reset otel_ctx now.  ('M' / GUC
	 * paths are not affected; they sit on otel_ctx until the
	 * client clears them or the transaction ends.)
	 */
	if (otel_ctx_from_comment)
	{
		otel_ctx_reset();
		otel_ctx_from_comment = false;
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
 *	  1. No consumer (no exporter hook AND log emission disabled)
 *		 -> DROP.  Backends not actively tracing pay only the two
 *		 pointer/bool reads at this gate.
 *	  2. otel.trace_all_queries -> RECORD_AND_SAMPLE.  Always-on
 *		 mode bypasses propagated state.
 *	  3. No propagated context (otel_ctx.is_set == false) -> DROP.
 *	  4-6. Policy-dependent.  See OtelSamplerHookPolicy in otel.h
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
	otel_span_emit_hook_type emit_hook = otel_get_span_emit_hook();
	otel_sampler_hook_type sampler_hook = otel_get_sampler_hook();
	OtelSamplerHookPolicy policy = otel_get_sampler_hook_policy();

	/* Gate 1: no consumer */
	if (emit_hook == NULL && !otel_emit_spans_to_log)
		return OTEL_SAMPLE_DROP;

	/* Gate 2: force-on overrides propagation entirely */
	if (otel_trace_all_queries)
		return OTEL_SAMPLE_RECORD_AND_SAMPLE;

	/* Gate 3: no propagated context */
	if (!otel_ctx.is_set)
		return OTEL_SAMPLE_DROP;

	/*
	 * Gates 4-6: policy-driven dispatch.
	 *
	 * The four policies map onto four straight-line decision paths
	 * with no further branching:
	 */
	switch (policy)
	{
		case OTEL_SAMPLER_HOOK_NEVER_ALWAYS_SAMPLE:
			/* Record everything that has a propagated context, no
			 * sampler call, no W3C bit check. */
			return OTEL_SAMPLE_RECORD_AND_SAMPLE;

		case OTEL_SAMPLER_HOOK_NEVER_RESPECT_BIT:
			/* Pure W3C ParentBased; sampler hook is never consulted. */
			return otel_ctx.sampled_flag_set
				? OTEL_SAMPLE_RECORD_AND_SAMPLE
				: OTEL_SAMPLE_DROP;

		case OTEL_SAMPLER_HOOK_ALWAYS:
			/* Defer EVERY decision to the hook, regardless of the
			 * wire bit.  If no hook is registered, fall back to
			 * always-record (a no-hook + ALWAYS combination is a
			 * configuration error, but recording is the safer
			 * default than silently dropping). */
			if (sampler_hook == NULL)
				return OTEL_SAMPLE_RECORD_AND_SAMPLE;
			break;				/* fall through to the hook call below */

		case OTEL_SAMPLER_HOOK_ON_UNSAMPLED_BIT:
		default:
			/* Default: honour wire bit; only call hook on unset. */
			if (otel_ctx.sampled_flag_set)
				return OTEL_SAMPLE_RECORD_AND_SAMPLE;
			if (sampler_hook == NULL)
				return OTEL_SAMPLE_DROP;
			break;				/* fall through to the hook call below */
	}

	/* Hook-call path (reached only by ALWAYS or ON_UNSAMPLED_BIT
	 * after their wire-bit checks). */
	{
		OtelSamplerInput in;

		in.trace_id = otel_ctx.trace_id;
		in.parent_span_id = otel_ctx.span_id;
		in.trace_flags = otel_ctx.trace_flags;
		in.tracestate = (otel_tracestate_guc && otel_tracestate_guc[0])
			? otel_tracestate_guc : NULL;
		in.name = name_hint;
		in.kind = OTEL_SPAN_KIND_SERVER;

		return sampler_hook(&in);
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

	/*
	 * If no in-memory context yet (no 'M' header, no SET) AND
	 * sqlcommenter parsing is enabled, try the SQL text.  No-op
	 * when the GUC is off (the cheap path is one boolean read).
	 */
	if (!otel_ctx.is_set && otel_parse_sqlcommenter && queryDesc != NULL)
		(void) try_apply_sqlcommenter_context(queryDesc->sourceText);

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
	/* Only finalize if this hook started the active span.  If a
	 * utility command started the span (CTAS, etc.) and the
	 * executor is running underneath it, the utility's hook owns
	 * the span lifecycle and the executor's End should be a no-op
	 * for the span. */
	if (span_originator == SPAN_ORIGIN_EXECUTOR)
		finalize_span(OTEL_STATUS_UNSET);

	if (prev_ExecutorEnd_hook)
		prev_ExecutorEnd_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
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

	if (otel_ctx.is_set)
	{
		memcpy(span_storage.trace_id, otel_ctx.trace_id, sizeof(span_storage.trace_id));
		memcpy(span_storage.trace_flags, otel_ctx.trace_flags, sizeof(span_storage.trace_flags));
		parent = (otel_current_span_id_guc && otel_current_span_id_guc[0])
			? otel_current_span_id_guc : otel_ctx.span_id;
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

	span_storage.tracestate = (otel_tracestate_guc && otel_tracestate_guc[0])
		? otel_tracestate_guc : NULL;

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

	update_current_span_id_guc(span_storage.span_id);
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
		 * otel_ExecutorStart for rationale. */
		if (!otel_ctx.is_set && otel_parse_sqlcommenter)
			(void) try_apply_sqlcommenter_context(queryString);

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
otel_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			finalize_span(OTEL_STATUS_ERROR);
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
			newarr = palloc0(sizeof(OtelSpanEvent) * 4);
		else
			newarr = repalloc(span_storage.overflow_events,
							  sizeof(OtelSpanEvent) * newcnt);
		/* Zero only the new slot (older slots are already populated) */
		if (newcnt > 1)
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


/* ====================================================================
 * emit_span_as_log_line --- zero-config JSON-log fallback emitter.
 * ====================================================================
 *
 * Gated by the otel.emit_spans_to_log GUC.  Writes the span as a
 * single structured LOG line; operators can ship those lines
 * downstream via fluentd / vector / the OTel Collector's filelog
 * receiver.
 *
 * Uses StringInfo (and palloc) so it is best-effort under memory
 * pressure: an allocation failure causes the log line for this one
 * span to be dropped, with no effect on the user's transaction.
 * Caller (finalize_span) wraps in PG_TRY.
 *
 * The JSON shape is documented as stable for THIS PoC across minor
 * revisions of contrib/otel; do not consider it OTLP and do not
 * embed in production tooling expecting OTLP compatibility.
 */
static void
emit_span_as_log_line(const OtelSpan *span)
{
	StringInfoData buf;
	int			i;
	bool		first;

	initStringInfo(&buf);

	appendStringInfoChar(&buf, '{');

	appendStringInfoString(&buf, "\"trace_id\":");
	escape_json(&buf, span->trace_id);
	appendStringInfoString(&buf, ",\"span_id\":");
	escape_json(&buf, span->span_id);
	appendStringInfoString(&buf, ",\"parent_span_id\":");
	escape_json(&buf, span->parent_span_id);
	appendStringInfoString(&buf, ",\"trace_flags\":");
	escape_json(&buf, span->trace_flags);
	if (span->tracestate)
	{
		appendStringInfoString(&buf, ",\"tracestate\":");
		escape_json(&buf, span->tracestate);
	}
	appendStringInfoString(&buf, ",\"name\":");
	escape_json(&buf, span->name ? span->name : "");
	appendStringInfo(&buf, ",\"kind\":%d", (int) span->kind);
	appendStringInfo(&buf, ",\"status\":%d", (int) span->status);
	appendStringInfo(&buf, ",\"start_time\":%" PRId64,
					 (int64) span->start_time);
	appendStringInfo(&buf, ",\"end_time\":%" PRId64,
					 (int64) span->end_time);

	/* Attributes object */
	appendStringInfoString(&buf, ",\"attributes\":{");
	first = true;
	for (i = 0; i < span->n_attrs; i++)
	{
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;
		escape_json(&buf, span->attrs[i].key ? span->attrs[i].key : "");
		appendStringInfoChar(&buf, ':');
		escape_json(&buf, span->attrs[i].value ? span->attrs[i].value : "");
	}
	for (i = 0; i < span->n_overflow_attrs; i++)
	{
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;
		escape_json(&buf, span->overflow_attrs[i].key ? span->overflow_attrs[i].key : "");
		appendStringInfoChar(&buf, ':');
		escape_json(&buf, span->overflow_attrs[i].value ? span->overflow_attrs[i].value : "");
	}
	appendStringInfoChar(&buf, '}');

	/* Events array */
	appendStringInfoString(&buf, ",\"events\":[");
	first = true;
	if (span->inline_event_used)
	{
		const OtelSpanEvent *e = &span->inline_event;

		appendStringInfoChar(&buf, '{');
		appendStringInfo(&buf, "\"time\":%" PRId64,
						 (int64) e->core.time);
		appendStringInfo(&buf, ",\"elevel\":%d", e->core.elevel);
		appendStringInfoString(&buf, ",\"sqlstate\":");
		escape_json(&buf, e->core.sqlstate);
		if (e->core.filename)
		{
			appendStringInfoString(&buf, ",\"filename\":");
			escape_json(&buf, e->core.filename);
		}
		appendStringInfo(&buf, ",\"lineno\":%d", e->core.lineno);
		if (e->message)
		{
			appendStringInfoString(&buf, ",\"message\":");
			escape_json(&buf, e->message);
		}
		if (e->detail)
		{
			appendStringInfoString(&buf, ",\"detail\":");
			escape_json(&buf, e->detail);
		}
		if (e->hint)
		{
			appendStringInfoString(&buf, ",\"hint\":");
			escape_json(&buf, e->hint);
		}
		appendStringInfoChar(&buf, '}');
		first = false;
	}
	for (i = 0; i < span->n_overflow_events; i++)
	{
		const OtelSpanEvent *e = &span->overflow_events[i];

		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;
		appendStringInfoChar(&buf, '{');
		appendStringInfo(&buf, "\"time\":%" PRId64,
						 (int64) e->core.time);
		appendStringInfo(&buf, ",\"elevel\":%d", e->core.elevel);
		appendStringInfoString(&buf, ",\"sqlstate\":");
		escape_json(&buf, e->core.sqlstate);
		if (e->core.filename)
		{
			appendStringInfoString(&buf, ",\"filename\":");
			escape_json(&buf, e->core.filename);
		}
		appendStringInfo(&buf, ",\"lineno\":%d", e->core.lineno);
		if (e->message)
		{
			appendStringInfoString(&buf, ",\"message\":");
			escape_json(&buf, e->message);
		}
		appendStringInfoChar(&buf, '}');
	}
	appendStringInfoChar(&buf, ']');

	appendStringInfoChar(&buf, '}');

	/* Emit as a single LOG line with a distinctive prefix so log
	 * collectors can filter for span records.  errmsg_internal
	 * suppresses translation since the JSON is not localized. */
	ereport(LOG,
			(errmsg_internal("otel-span: %s", buf.data)));

	pfree(buf.data);
}
