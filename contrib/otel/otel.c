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
 * Recognized header keys:
 *
 *	 otel.traceparent  W3C TraceContext header value
 *					   ("00-{32 hex}-{16 hex}-{2 hex}"); see
 *					   https://www.w3.org/TR/trace-context/.
 *	 otel.tracestate   Companion W3C header, stored opaquely and
 *					   not interpreted further by this module.
 *
 * Behaviour:
 *
 *	 * The handler is registered at PROTOCOL_HEADER_SCOPE_TRANSACTION;
 *	   the trace context set with a single 'M' message applies for the
 *	   remainder of the current transaction and is cleared at
 *	   COMMIT/ROLLBACK (or at the end of the implicit transaction
 *	   around a single Parse/Bind/Execute cycle).
 *
 *	 * An emit_log_hook (in otel_log.c) fills the ErrorData trace_id /
 *	   span_id / trace_flags fields whenever a trace context is active,
 *	   so the built-in JSON-log writer, CSV-log writer, and
 *	   log_line_prefix %T / %S escapes pick the values up automatically.
 *
 *	 * Malformed traceparent values are silently ignored (at DEBUG1
 *	   level).  An empty value is the documented "clear this key"
 *	   convention.
 *
 * Module must be loaded via shared_preload_libraries so the header
 * handler is registered before any client connects.  CREATE EXTENSION
 * otel installs the introspection SQL function but is not required to
 * receive trace context from clients.
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
#include "utils/memutils.h"

#include "otel_internal.h"

PG_MODULE_MAGIC;

/*
 * Currently-active trace context for this backend.  Storage is in
 * TopMemoryContext via MemoryContextStrdup, so values survive across
 * statements within a transaction until cleared by the handler's
 * scope callback.
 */
OtelContext otel_ctx;

static bool parse_traceparent(const char *s, OtelContext *out);
static bool all_hex(const char *p, size_t n);
static bool all_zeros(const char *p, size_t n);
static void otel_reset(void);

/*
 * Header set callback: invoked once per matching entry in an incoming
 * RequestHeaders message.
 */
static void
otel_set_cb(const char *key, const char *value, void *cb_ctx)
{
	if (strcmp(key, "otel.traceparent") == 0)
	{
		if (value[0] == '\0')
		{
			/* documented "clear this key" convention */
			otel_reset();
			return;
		}

		if (!parse_traceparent(value, &otel_ctx))
			ereport(DEBUG1,
					(errmsg("otel: ignoring malformed traceparent value")));
	}
	else if (strcmp(key, "otel.tracestate") == 0)
	{
		if (otel_ctx.tracestate)
		{
			pfree(otel_ctx.tracestate);
			otel_ctx.tracestate = NULL;
		}
		if (value[0] != '\0')
			otel_ctx.tracestate =
				MemoryContextStrdup(TopMemoryContext, value);
	}
	/* unknown otel.* keys are silently ignored */
}

/*
 * Header clear callback: invoked once at the handler's scope boundary
 * (transaction end).
 */
static void
otel_clear_cb(void *cb_ctx)
{
	otel_reset();
}

/*
 * SQL function: return the currently-active traceparent, in W3C
 * traceparent header format, or NULL if none is set.  Useful for
 * application-side introspection and for testing.
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
 * shared_preload_libraries) so the handler is in place before any
 * backend processes its first 'M' message.
 */
void		_PG_init(void);

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errmsg("otel must be loaded via shared_preload_libraries")));

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
	/* tracestate is set independently and not touched here */
	return true;
}

static void
otel_reset(void)
{
	otel_ctx.is_set = false;
	otel_ctx.trace_id[0] = '\0';
	otel_ctx.span_id[0] = '\0';
	otel_ctx.trace_flags[0] = '\0';
	if (otel_ctx.tracestate)
	{
		pfree(otel_ctx.tracestate);
		otel_ctx.tracestate = NULL;
	}
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
