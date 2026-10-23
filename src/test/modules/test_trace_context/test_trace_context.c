/*-------------------------------------------------------------------------
 *
 * test_trace_context.c
 *	  Test module for the trace-context protocol message ('M').
 *
 * Registers a trace-context handler via RegisterTraceContextHandler.
 * Records received traceparent/tracestate and exposes them via SQL
 * functions for TAP test assertions.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/test/modules/test_trace_context/test_trace_context.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "libpq/trace_context.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

/*
 * Last received trace context.  Stored in TopMemoryContext so values
 * survive transaction-context resets.
 */
static char *last_traceparent = NULL;
static char *last_tracestate = NULL;
static bool context_is_active = false;

static void
tc_apply_cb(const char *traceparent, const char *tracestate, void *ctx)
{
	MemoryContext oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	if (last_traceparent != NULL)
		pfree(last_traceparent);
	if (last_tracestate != NULL)
		pfree(last_tracestate);

	last_traceparent = pstrdup(traceparent);
	last_tracestate = pstrdup(tracestate ? tracestate : "");
	context_is_active = true;

	MemoryContextSwitchTo(oldcxt);

	ereport(LOG,
			(errmsg("test_trace_context: apply traceparent=%s tracestate=%s",
					traceparent, tracestate ? tracestate : "")));
}

static void
tc_clear_cb(void *ctx)
{
	if (!context_is_active)
		return;

	context_is_active = false;

	ereport(LOG,
			(errmsg("test_trace_context: clear")));
}

/* SQL functions */
PG_FUNCTION_INFO_V1(test_tc_traceparent);
PG_FUNCTION_INFO_V1(test_tc_tracestate);
PG_FUNCTION_INFO_V1(test_tc_is_active);

Datum
test_tc_traceparent(PG_FUNCTION_ARGS)
{
	if (last_traceparent == NULL)
		PG_RETURN_NULL();
	PG_RETURN_TEXT_P(cstring_to_text(last_traceparent));
}

Datum
test_tc_tracestate(PG_FUNCTION_ARGS)
{
	if (last_tracestate == NULL)
		PG_RETURN_NULL();
	PG_RETURN_TEXT_P(cstring_to_text(last_tracestate));
}

Datum
test_tc_is_active(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(context_is_active);
}

void		_PG_init(void);

void
_PG_init(void)
{
	RegisterTraceContextHandler(tc_apply_cb, tc_clear_cb, NULL);
}
