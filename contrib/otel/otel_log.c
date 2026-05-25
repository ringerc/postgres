/*-------------------------------------------------------------------------
 *
 * otel_log.c
 *	  emit_log_hook integration for contrib/otel: fills ErrorData
 *	  trace_id / span_id / trace_flags from the propagated trace
 *	  context when the originating ereport site did not set them.
 *	  Makes trace context appear in the JSON / CSV log writers and
 *	  in the %T / %S log_line_prefix escapes without each ereport
 *	  caller having to know anything about tracing.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/otel/otel_log.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"

#include "otel_internal.h"

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
 *	1. Inject trace context into ErrorData so the built-in log
 *	   writers and %T / %S log_line_prefix escapes pick it up
 *	   automatically.
 *	2. Hand the ereport to the span event-capture path in
 *	   otel_trace.c; it handles the active-span check, the elevel
 *	   gate, and the ERROR-status update internally.
 *	3. Chain.
 */
static void
otel_emit_log_hook(ErrorData *edata)
{
	if (otel_ctx.is_set)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(edata->assoc_context);

		if (edata->trace_id == NULL)
			edata->trace_id = pstrdup(otel_ctx.trace_id);
		if (edata->span_id == NULL)
			edata->span_id = pstrdup(otel_ctx.span_id);
		if (edata->trace_flags == NULL)
			edata->trace_flags = pstrdup(otel_ctx.trace_flags);

		MemoryContextSwitchTo(oldcxt);
	}

	otel_span_record_log_event(edata);

	if (prev_emit_log_hook)
		prev_emit_log_hook(edata);
}
