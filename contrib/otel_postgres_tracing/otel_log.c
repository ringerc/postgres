/*-------------------------------------------------------------------------
 *
 * otel_log.c
 *	  emit_log_hook integration for contrib/otel: surfaces the
 *	  propagated trace context in the server's log output so an
 *	  operator can correlate a log line back to the trace it
 *	  belongs to.
 *
 *	  When core postgres has the native trace-context fields on
 *	  ErrorData (the OTEL_HAVE_ERRTRACE feature), we fill
 *	  edata->trace_id / span_id / trace_flags directly.  The built-
 *	  in log writers then surface them via JSON keys, CSV columns,
 *	  and the %T / %S log_line_prefix escapes --- structured,
 *	  machine-parseable.
 *
 *	  When core postgres lacks those fields (an unpatched server),
 *	  we fall back to appending a "trace_id=... span_id=... " line
 *	  to edata->context so the trace context still appears in the
 *	  textual log output, just less structured.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/otel_postgres_tracing/otel_log.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"

#include "otel_postgres_tracing.h"

static emit_log_hook_type prev_emit_log_hook = NULL;

static void otel_emit_log_hook(ErrorData *edata);
static const char *severity_label(int elevel);


/*
 * Install our emit_log_hook, saving any previously-installed hook so
 * we can chain to it.  Called once from _PG_init.
 */
void
otel_log_install_hooks(void)
{
	prev_emit_log_hook = emit_log_hook;
	emit_log_hook = otel_emit_log_hook;
}


/*
 * emit_log_hook entry point.
 *
 *	1. Surface trace context in the log line.  When core postgres
 *	   has the native ErrorData.trace_id/span_id/trace_flags
 *	   fields, fill those.  Otherwise append a trace_id=... line
 *	   to edata->context as a less-structured fallback.
 *	2. Hand the ereport to the span event-capture path in
 *	   otel_trace.c; it handles the active-span check, the elevel
 *	   gate, and the ERROR-status update internally.
 *	3. Chain.
 */
static void
otel_emit_log_hook(ErrorData *edata)
{
	OtelRootContextSnapshot rc;

	otel_api->get_root_context_snapshot(&rc);

	if (rc.is_set)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(edata->assoc_context);

#ifdef OTEL_HAVE_ERRTRACE
		if (edata->trace_id == NULL)
			edata->trace_id = pstrdup(rc.trace_id);
		if (edata->span_id == NULL)
			edata->span_id = pstrdup(rc.span_id);
		if (edata->trace_flags == NULL)
			edata->trace_flags = pstrdup(rc.trace_flags);
#else
		/*
		 * Fallback for unpatched servers without ErrorData trace
		 * fields: append a "trace_id=... span_id=... trace_flags=..."
		 * line to edata->context.  Less structured than the native
		 * path (no JSON keys / CSV columns / %T %S prefix), but it
		 * preserves the correlation in the textual log output.
		 *
		 * Skipped when the context already mentions our trace_id ---
		 * the chained prev_emit_log_hook may have appended an
		 * identical line, and a chain of identical hooks shouldn't
		 * stack duplicates.
		 */
		if (edata->context == NULL ||
			strstr(edata->context, rc.trace_id) == NULL)
		{
			StringInfoData ctx;

			initStringInfo(&ctx);
			if (edata->context)
				appendStringInfo(&ctx, "%s\n", edata->context);
			appendStringInfo(&ctx,
							 "trace_id=%s span_id=%s trace_flags=%s",
							 rc.trace_id, rc.span_id, rc.trace_flags);
			edata->context = ctx.data;
		}
#endif

		MemoryContextSwitchTo(oldcxt);
	}

	/*
	 * Count this ereport against the per-severity Counter.
	 * severity_label returns NULL for elevels we don't track
	 * (DEBUG levels, INFO, COMMERROR, WARNING_CLIENT_ONLY); those
	 * are skipped to keep cardinality bounded.  Note: emit_log_hook
	 * only fires for events that pass log_min_messages, so the
	 * NOTICE counter is only meaningful when log_min_messages is
	 * relaxed below the WARNING default.
	 */
	{
		const char *sev = severity_label(edata->elevel);

		if (sev != NULL)
			otel_api->metric_counter_add(otel_pg_log_events_counter, 1, sev);
	}

	otel_span_record_log_event(edata);

	if (prev_emit_log_hook)
		prev_emit_log_hook(edata);
}

/*
 * Map an elog.h elevel constant to the severity label used as the
 * Counter's attribute value.  Returns NULL for elevels we don't
 * count (DEBUG*, INFO, COMMERROR, WARNING_CLIENT_ONLY).
 */
static const char *
severity_label(int elevel)
{
	switch (elevel)
	{
		case LOG:
			return "LOG";
		case NOTICE:
			return "NOTICE";
		case WARNING:
			return "WARNING";
		case ERROR:
			return "ERROR";
		case FATAL:
			return "FATAL";
		case PANIC:
			return "PANIC";
		default:
			return NULL;
	}
}
