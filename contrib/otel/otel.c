/*-------------------------------------------------------------------------
 *
 * otel.c
 *	  OpenTelemetry trace-context support for PostgreSQL --- module
 *	  setup, coordination, and trace-context input layer.
 *
 * Loadable module that consumes the per-message RequestHeaders ('M')
 * mechanism to receive a W3C Trace Context from the client and attach
 * the trace-id, span-id and trace-flags to every log message emitted
 * for the remainder of the current transaction.
 *
 * State storage:
 *
 * The active trace context is stored in two custom GUCs, mirroring the
 * two HTTP headers the W3C Trace Context spec defines:
 *
 *	 otel.traceparent  Required, fixed format:
 *					   "{version}-{trace-id}-{parent-id}-{flags}".
 *					   Every component has spec-defined semantics;
 *					   every conformant tracing participant MUST
 *					   understand it.  Our assign-hook parses and
 *					   decomposes the value into the in-memory
 *					   OtelContext struct used by the emit_log_hook.
 *					   This is the load-bearing piece for log
 *					   correlation.
 *
 *	 otel.tracestate   Optional, vendor-extensible: a comma-separated
 *					   list of "vendor=value" pairs, where the
 *					   semantics of each value are defined by that
 *					   vendor's tracing system (Datadog, Honeycomb,
 *					   etc.), not by the W3C spec.
 *
 *					   We accept and store this opaquely.  We do NOT
 *					   interpret it, log it, or attach it to ErrorData
 *					   today.  It is kept for three reasons:
 *
 *					   1. The W3C spec requires that participants
 *						  propagate unknown tracestate entries
 *						  unchanged; clients that follow the spec
 *						  send both headers and would have to drop
 *						  vendor state at the Postgres boundary if we
 *						  refused tracestate.
 *					   2. Future use: if this module ever emits OTLP
 *						  child spans of its own, propagating
 *						  tracestate becomes mandatory.  Having the
 *						  storage in place now avoids a wire/API
 *						  change later.
 *					   3. Diagnostic visibility via SHOW
 *						  otel.tracestate when operators are
 *						  debugging trace propagation through proxy
 *						  chains.
 *
 *					   `tracestate` is NOT baggage.  W3C Baggage is a
 *					   separate spec (https://www.w3.org/TR/baggage/)
 *					   with its own HTTP header, key namespace, size
 *					   budget, and audience (application code, not
 *					   vendor tracing tools).  If an application
 *					   needs to propagate baggage --- e.g. tenant_id
 *					   for RLS, user_id for audit --- through
 *					   PostgreSQL, that belongs in a separate
 *					   `baggage.*` prefix handler (likely a sibling
 *					   `contrib/baggage` module), not in
 *					   `otel.tracestate`.
 *
 * Using GUCs as the canonical storage has a deliberate side effect:
 * GUC values automatically propagate to parallel workers via
 * RestoreGUCState during ParallelWorkerMain startup.  The workers'
 * assign-hooks then populate their own copies of the in-memory
 * OtelContext, so worker-side log emission picks up the trace context
 * exactly as the leader's does.  No bespoke parallel-state plumbing
 * is required.
 *
 * Behaviour:
 *
 *	 * The header handler is registered at
 *	   PROTOCOL_HEADER_SCOPE_TRANSACTION; one 'M' before BEGIN (or
 *	   before a one-shot Parse/Bind/Execute) installs context for
 *	   every statement until COMMIT/ROLLBACK or the end of the
 *	   implicit transaction.  Last-write-wins.
 *	 * An emit_log_hook (in otel_log.c) fills ErrorData.trace_id /
 *	   span_id / trace_flags when not already set, so the built-in
 *	   JSON-log, CSV-log, and log_line_prefix %T / %S escapes pick
 *	   the values up automatically.
 *	 * Malformed traceparent values are rejected by the GUC check-hook
 *	   with a clear error message (instead of being silently dropped
 *	   like in the previous direct-write design); any user-facing
 *	   error is surfaced through normal GUC error reporting.
 *	 * An empty value is the documented "clear this key" convention
 *	   from the headers mechanism, and translates to a GUC reset
 *	   (SetConfigOption with a NULL value).
 *
 * Module must be loaded via shared_preload_libraries so the header
 * handler is registered before any backend processes its first 'M'
 * message, and so the custom GUCs are defined before any worker
 * tries to restore them.  CREATE EXTENSION otel installs the
 * introspection SQL function but is not required for receiving
 * trace context from clients.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/otel/otel.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "fmgr.h"
#include "libpq/protocol_headers.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#include "otel.h"
#include "otel_internal.h"

PG_MODULE_MAGIC;

/*
 * Exporter hook --- exporters in separate loadable modules register
 * a callback against this to receive completed spans.  See otel.h
 * for the contract.
 */
otel_span_emit_hook_type otel_span_emit_hook = NULL;

/* In-memory derived state populated by the otel.traceparent assign-hook
 * (called by the GUC machinery on M-header arrival, SET, or
 * parallel-worker RestoreGUCState).  Read by the emit_log_hook in
 * otel_log.c. */
OtelContext otel_ctx;

/* GUC variables --- canonical storage. */
static char *otel_traceparent_guc;
static char *otel_tracestate_guc;


static bool parse_traceparent(const char *s, OtelContext *out);
static bool all_hex(const char *p, size_t n);
static bool all_zeros(const char *p, size_t n);
static void otel_ctx_reset(void);

/* GUC check / assign hooks. */
static bool check_traceparent(char **newval, void **extra, GucSource source);
static void assign_traceparent(const char *newval, void *extra);


/*
 * GUC check-hook for otel.traceparent --- validates the W3C format.
 * An empty string or NULL is accepted as "clear".
 */
static bool
check_traceparent(char **newval, void **extra, GucSource source)
{
	OtelContext tmp;

	if (*newval == NULL || (*newval)[0] == '\0')
		return true;

	if (!parse_traceparent(*newval, &tmp))
	{
		GUC_check_errmsg("invalid W3C traceparent format: \"%s\"", *newval);
		GUC_check_errdetail("Expected \"00-{32 hex}-{16 hex}-{2 hex}\" with non-zero trace-id and parent-id.");
		return false;
	}
	return true;
}

/*
 * GUC assign-hook for otel.traceparent --- populates the in-memory
 * derived state.  Called on every value change, including by
 * RestoreGUCState in a parallel worker.
 */
static void
assign_traceparent(const char *newval, void *extra)
{
	OtelContext tmp;

	if (newval == NULL || newval[0] == '\0')
	{
		otel_ctx_reset();
		return;
	}

	/*
	 * parse_traceparent was already vetted in the check-hook; this
	 * cannot fail.  Defensively still check.
	 */
	if (parse_traceparent(newval, &tmp))
		otel_ctx = tmp;
	else
		otel_ctx_reset();
}

/*
 * Header set callback: invoked once per matching entry in an incoming
 * RequestHeaders message.  Routes through SetConfigOption so the GUC
 * machinery propagates to parallel workers and triggers the assign
 * hook (which updates the in-memory derived state).
 */
static void
otel_set_cb(const char *key, const char *value, void *cb_ctx)
{
	const char *guc;

	if (strcmp(key, "otel.traceparent") == 0)
		guc = "otel.traceparent";
	else if (strcmp(key, "otel.tracestate") == 0)
		guc = "otel.tracestate";
	else
		return;					/* unknown otel.* key */

	/*
	 * An empty value is the documented "clear this key" convention; map
	 * it to a GUC reset by passing NULL as the value.  Malformed
	 * traceparent values are rejected by the GUC check-hook with a LOG
	 * (we swallow the error so a misbehaving client cannot abort the
	 * caller's operation; the operation proceeds without a trace
	 * context).
	 */
	(void) set_config_option(guc,
							 value[0] == '\0' ? NULL : value,
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SET, true, LOG, false);
}

/*
 * Header clear callback: invoked at the handler's scope boundary
 * (transaction end).  Clears both GUCs back to default.
 */
static void
otel_clear_cb(void *cb_ctx)
{
	(void) set_config_option("otel.traceparent", NULL,
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SET, true, LOG, false);
	(void) set_config_option("otel.tracestate", NULL,
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SET, true, LOG, false);
}

/*
 * SQL function: return the currently-active traceparent in W3C
 * header format, or NULL if none is set.
 */
PG_FUNCTION_INFO_V1(otel_current_traceparent);

Datum
otel_current_traceparent(PG_FUNCTION_ARGS)
{
	char		buf[OTEL_TRACEPARENT_LEN + 1];

	if (!otel_ctx.is_set)
		PG_RETURN_NULL();

	snprintf(buf, sizeof(buf), "00-%s-%s-%s",
			 otel_ctx.trace_id, otel_ctx.span_id, otel_ctx.trace_flags);
	PG_RETURN_TEXT_P(cstring_to_text(buf));
}

/*
 * Module entrypoint.  Must run in the postmaster (via
 * shared_preload_libraries) so the GUCs are defined and the header
 * handler is in place before any backend, including any future
 * parallel worker, needs them.
 */
void		_PG_init(void);

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errmsg("otel must be loaded via shared_preload_libraries")));

	/*
	 * Custom GUCs --- canonical storage for trace context.  Defined
	 * with PGC_USERSET so the postgres GUC machinery propagates them
	 * to parallel workers without any bespoke plumbing.  Both default
	 * to the empty string (= "no context set"); the assign-hook
	 * collapses empty -> no context.
	 */
	DefineCustomStringVariable("otel.traceparent",
							   "W3C Trace Context traceparent for the current operation.",
							   "Format: \"00-{32 hex}-{16 hex}-{2 hex}\". Empty means no trace context.",
							   &otel_traceparent_guc,
							   "",
							   PGC_USERSET,
							   0,
							   check_traceparent,
							   assign_traceparent,
							   NULL);

	DefineCustomStringVariable("otel.tracestate",
							   "W3C Trace Context tracestate companion value.",
							   "Stored opaquely; not interpreted by this module.",
							   &otel_tracestate_guc,
							   "",
							   PGC_USERSET,
							   0,
							   NULL,
							   NULL,
							   NULL);

	MarkGUCPrefixReserved("otel");

	RegisterProtocolHeaderHandler("otel.",
								  PROTOCOL_HEADER_SCOPE_TRANSACTION,
								  otel_set_cb,
								  otel_clear_cb,
								  NULL);

	otel_log_install_hooks();
}

/*
 * Parse a W3C traceparent value into *out.  Returns true on success.
 *
 * Format: "{version}-{trace-id}-{parent-id}-{flags}" where
 *	 version		2 lowercase hex chars; only "00" is currently
 *					accepted (per the W3C spec, future versions may
 *					append fields after the flags).
 *	 trace-id		32 lowercase hex chars, not all-zeros.
 *	 parent-id		16 lowercase hex chars, not all-zeros.
 *	 flags			2 lowercase hex chars.
 */
static bool
parse_traceparent(const char *s, OtelContext *out)
{
	if (strlen(s) != OTEL_TRACEPARENT_LEN)
		return false;
	if (s[2] != '-' || s[35] != '-' || s[52] != '-')
		return false;

	/* only version "00" is supported */
	if (s[0] != '0' || s[1] != '0')
		return false;

	if (!all_hex(s + 3, OTEL_TRACE_ID_LEN))
		return false;
	if (!all_hex(s + 36, OTEL_SPAN_ID_LEN))
		return false;
	if (!all_hex(s + 53, OTEL_TRACE_FLAGS_LEN))
		return false;

	/* per W3C: all-zero trace-id and all-zero parent-id are invalid */
	if (all_zeros(s + 3, OTEL_TRACE_ID_LEN))
		return false;
	if (all_zeros(s + 36, OTEL_SPAN_ID_LEN))
		return false;

	memcpy(out->trace_id, s + 3, OTEL_TRACE_ID_LEN);
	out->trace_id[OTEL_TRACE_ID_LEN] = '\0';
	memcpy(out->span_id, s + 36, OTEL_SPAN_ID_LEN);
	out->span_id[OTEL_SPAN_ID_LEN] = '\0';
	memcpy(out->trace_flags, s + 53, OTEL_TRACE_FLAGS_LEN);
	out->trace_flags[OTEL_TRACE_FLAGS_LEN] = '\0';
	out->is_set = true;
	return true;
}

static void
otel_ctx_reset(void)
{
	otel_ctx.is_set = false;
	otel_ctx.trace_id[0] = '\0';
	otel_ctx.span_id[0] = '\0';
	otel_ctx.trace_flags[0] = '\0';
}

static bool
all_hex(const char *p, size_t n)
{
	for (size_t i = 0; i < n; i++)
	{
		char		c = p[i];

		if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
			return false;
	}
	return true;
}

static bool
all_zeros(const char *p, size_t n)
{
	for (size_t i = 0; i < n; i++)
		if (p[i] != '0')
			return false;
	return true;
}
