/*-------------------------------------------------------------------------
 *
 * otel_log.c
 *	  emit_log_hook integration for otel_postgres_tracing: surfaces the
 *	  propagated trace context in the server's log output so an
 *	  operator can correlate a log line back to the trace it
 *	  belongs to.
 *
 *	  When core postgres has the structured-annotation API on
 *	  ErrorData (the OTEL_HAVE_ERRANNOT feature), we attach
 *	  trace_id / span_id / trace_flags as annotations. The built-
 *	  in log writers then surface them via JSON keys, the
 *	  annotations object in CSV, and the %A / %{key}A
 *	  log_line_prefix escapes --- structured, machine-parseable.
 *
 *	  When core postgres lacks the annotation API (an unpatched
 *	  server), we fall back to appending a "trace_id=... span_id=... "
 *	  line to edata->context so the trace context still appears in
 *	  the textual log output, just less structured.
 *
 *	  otel_api now captures WARNING+ ereports into the innermost
 *	  recording span automatically (its own emit_log_hook), so this
 *	  file no longer forwards events into a span itself; it only
 *	  reads the current trace context to annotate the log line.
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
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"

#include "otel_postgres_tracing.h"

static emit_log_hook_type prev_emit_log_hook = NULL;

static void otel_emit_log_hook(ErrorData *edata);


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
 *	   supports structured annotations on ErrorData, attach
 *	   trace_id / span_id / trace_flags as annotations under their
 *	   well-known keys.  Otherwise append a trace_id=... line to
 *	   edata->context as a less-structured fallback.
 *	2. Chain.
 *
 *	When there is no current trace context, the annotation step is
 *	skipped; the hook chain is still exercised.
 */
static void
otel_emit_log_hook(ErrorData *edata)
{
	OtelSpanContext ctx;

	/* otel_api refuses every call inside a critical section. */
	if (CritSectionCount == 0 && otel_span_context_of(OTEL_SPAN_NONE, &ctx))
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(edata->assoc_context);
		char		trace_id_hex[OTEL_TRACE_ID_HEX_LEN + 1];
		char		span_id_hex[OTEL_SPAN_ID_HEX_LEN + 1];
		char		flags_hex[3];

		otel_trace_id_to_hex(&ctx.trace_id, trace_id_hex);
		otel_span_id_to_hex(&ctx.span_id, span_id_hex);
		otel_bytes_to_hex(&ctx.trace_flags, 1, flags_hex);

#ifdef OTEL_HAVE_ERRANNOT
		/*
		 * errannot() operates on errordata[errordata_stack_depth]
		 * which is what emit_log_hook receives as edata, so
		 * attaching here updates the same ErrorData the log writers
		 * are about to read.  set_annotation() in core uses
		 * find-or-append semantics under each key, so a chained hook
		 * that previously set the same key replaces rather than
		 * stacking.  We deliberately do NOT pre-check whether a
		 * previous hook set the annotation: the most-recent
		 * hook-installed trace context is the most authoritative for
		 * this log line.
		 */
		errannot(OTEL_ERRANNOT_KEY_TRACE_ID, trace_id_hex);
		errannot(OTEL_ERRANNOT_KEY_SPAN_ID, span_id_hex);
		errannot(OTEL_ERRANNOT_KEY_TRACE_FLAGS, flags_hex);
#else
		/*
		 * Fallback for unpatched servers without ErrorData
		 * annotations: append a "trace_id=... span_id=...
		 * trace_flags=..." line to edata->context.  Less
		 * structured than the native path (no JSON keys / CSV
		 * column / %A %{key}A prefix), but it preserves the
		 * correlation in the textual log output.
		 *
		 * Skipped when the context already mentions our trace_id ---
		 * the chained prev_emit_log_hook may have appended an
		 * identical line, and a chain of identical hooks shouldn't
		 * stack duplicates.
		 */
		if (edata->context == NULL ||
			strstr(edata->context, trace_id_hex) == NULL)
		{
			StringInfoData ctxbuf;

			initStringInfo(&ctxbuf);
			if (edata->context)
				appendStringInfo(&ctxbuf, "%s\n", edata->context);
			appendStringInfo(&ctxbuf,
							 "trace_id=%s span_id=%s trace_flags=%s",
							 trace_id_hex, span_id_hex, flags_hex);
			edata->context = ctxbuf.data;
		}
#endif

		MemoryContextSwitchTo(oldcxt);
	}

	if (prev_emit_log_hook)
		prev_emit_log_hook(edata);
}
