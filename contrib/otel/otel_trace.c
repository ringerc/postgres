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
#include "utils/backend_status.h"
#include "utils/elog.h"
#include "utils/guc.h"
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
 */
static OtelSpan span_storage;
static bool		span_active = false;
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

static void otel_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void otel_ExecutorEnd(QueryDesc *queryDesc);
static void otel_xact_callback(XactEvent event, void *arg);
static void otel_proc_exit_cb(int code, Datum arg);
static void start_span(QueryDesc *queryDesc);
static void finalize_span(OtelSpanStatus status);
static void generate_span_id(char out[OTEL_SPAN_ID_LEN + 1]);
static void bytes_to_lower_hex(const unsigned char *src, size_t n, char *dst);
static void span_add_attr(const char *key, const char *value);
static void update_current_span_id_guc(const char *new_value);
static void restore_current_span_id_guc(void);


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
	if (!span_active)
		return;

	span_storage.end_time = GetCurrentTimestamp();

	/* If status was already set to ERROR by an event capture, keep
	 * it; otherwise apply the caller-supplied status. */
	if (span_storage.status == OTEL_STATUS_UNSET)
		span_storage.status = status;

	/* Hand off to exporter (best-effort, swallow errors). */
	PG_TRY();
	{
		if (otel_span_emit_hook)
			otel_span_emit_hook(&span_storage);

		/* TODO step 5: built-in JSON log emitter when otel_emit_spans_to_log
		 * is true.  Skipped here. */
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();

	span_active = false;
	restore_current_span_id_guc();
}

/*
 * ExecutorStart hook.  Gated by the early-bail checks; only starts
 * a span when there's actually a consumer AND something to record.
 */
static void
otel_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	bool		wants_span;

	/* Early bail #1: any consumer at all? */
	wants_span = (otel_span_emit_hook != NULL) || otel_emit_spans_to_log;

	/* Early bail #2: any trace context active, OR opt-in to trace
	 * untraced queries? */
	if (wants_span)
		wants_span = otel_ctx.is_set || otel_trace_all_queries;

	/* Defensive: never overlap. */
	if (wants_span && !span_active)
	{
		PG_TRY();
		{
			start_span(queryDesc);
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
	/* Finalize span (success path; status stays UNSET unless
	 * error-event capture flipped it to ERROR). */
	finalize_span(OTEL_STATUS_UNSET);

	if (prev_ExecutorEnd_hook)
		prev_ExecutorEnd_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
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
