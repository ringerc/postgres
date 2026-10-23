/*-------------------------------------------------------------------------
 *
 * otel.c
 *	  OpenTelemetry trace-context support for PostgreSQL --- module
 *	  setup, coordination, and trace-context input layer.
 *
 * Loadable module that consumes the TraceContext ('M') protocol message
 * to receive a W3C Trace Context from the client and attach
 * the trace-id, span-id and trace-flags to every log message emitted
 * for the remainder of the current transaction.
 *
 * State storage:
 *
 * The active trace context is stored in two custom GUCs, mirroring the
 * two HTTP headers the W3C Trace Context spec defines:
 *
 *	 otel_api.traceparent  Required, fixed format:
 *					   "{version}-{trace-id}-{parent-id}-{flags}".
 *					   Every component has spec-defined semantics;
 *					   every conformant tracing participant MUST
 *					   understand it.  Our assign-hook parses and
 *					   decomposes the value into the in-memory
 *					   otel_root_ctx, read by span start and the emit_log_hook.
 *					   This is the load-bearing piece for log
 *					   correlation.
 *
 *	 otel_api.tracestate   Optional, vendor-extensible: a comma-separated
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
 *						  otel_api.tracestate when operators are
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
 *					   `otel_api.tracestate`.
 *
 * Using GUCs as the canonical storage has a deliberate side effect:
 * GUC values automatically propagate to parallel workers via
 * RestoreGUCState during ParallelWorkerMain startup.  The workers'
 * assign-hooks then populate their own copies of the in-memory
 * otel_root_ctx, so worker-side log emission picks up the trace context
 * exactly as the leader's does.  No bespoke parallel-state plumbing
 * is required.
 *
 * Behaviour:
 *
 *	 * The header handler is registered at the dispatcher's single
 *	   on-set entry point; we install our own XactCallback to clear
 *	   the in-memory derived state at top-level transaction end.
 *	   One 'M' before BEGIN (or before a one-shot Parse/Bind/Execute)
 *	   installs context for every statement until COMMIT/ROLLBACK or
 *	   the end of the implicit transaction.  Last-write-wins.
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
 * tries to restore them.  CREATE EXTENSION otel_api installs the
 * introspection SQL function but is not required for receiving
 * trace context from clients.
 *
 * Client-side propagation:
 *
 *	 The PRIMARY entry point is the TraceContext ('M') protocol message
 *	 (protocol 3.3+): the client sends traceparent and tracestate fields,
 *	 the handler attaches the context immediately and our clear_cb
 *	 auto-clears it at the next ReadyForQuery boundary.  Drivers
 *	 that implement the 'M' message (e.g. via libpq's
 *	 PQattachTraceContext, see src/test/modules/libpq_trace_context/) get
 *	 the scoping and the zero-extra-round-trip behaviour for free.
 *
 *	 ALTERNATIVELY, clients may set the GUCs directly with SQL:
 *
 *		 SET otel_api.traceparent = '00-{trace-id}-{span-id}-{flags}';
 *		 SET otel_api.tracestate  = 'vendor1=...,vendor2=...';
 *
 *	 or, scoped to one transaction:
 *
 *		 BEGIN;
 *		 SET LOCAL otel_api.traceparent = '...';
 *		 SELECT ...;
 *		 COMMIT;
 *
 *	 This works because otel_api.traceparent / otel_api.tracestate are
 *	 ordinary PGC_USERSET GUCs.  Use it when:
 *
 *	   * the driver doesn't support the 'M' message yet;
 *	   * an ORM / pooler intercepts protocol-level extensions;
 *	   * you want explicit SQL-visible audit of context changes.
 *
 *	 Costs versus the 'M'-header path:
 *
 *	   * Extra round-trip.  A SET is its own statement; each query
 *		 you want traced now costs two client/server round trips
 *		 instead of one.  Noticeable for high-rate workloads of
 *		 short queries; irrelevant when the traced query is itself
 *		 slow.  The 'M' header rides on the same Query message and
 *		 adds no round-trip.
 *	   * Cannot achieve statement scope.  GUC scoping options are:
 *		   - plain SET: session-scoped (persists across statements
 *			 AND transactions; survives auto-commit; the operator
 *			 must explicitly clear with `SET otel_api.traceparent = ''`
 *			 or `RESET otel_api.traceparent`).  Real risk of stale
 *			 context leaking from one logical operation to the
 *			 next, especially in long-lived connections.
 *		   - SET LOCAL: transaction-scoped (cleared at COMMIT or
 *			 ROLLBACK).  Better, but still NOT statement-scoped:
 *			 multiple statements in the same explicit transaction
 *			 all share the same context, even if they represent
 *			 different logical operations being traced separately.
 *		 The 'M' (TraceContext) message is naturally statement-scoped
 *		 in single-statement-per-message protocol use (each Query
 *		 message can carry its own context) and naturally
 *		 transaction-scoped under explicit BEGIN/COMMIT because
 *		 ClearTraceContext() is called at ReadyForQuery boundaries,
 *		 which clears the in-memory derived state.
 *	   * Connection-pooler interaction.  SET state on a pooled
 *		 connection in TRANSACTION or STATEMENT mode pooling can
 *		 leak the trace context to whichever client happens to
 *		 borrow the connection next, unless the pooler explicitly
 *		 discards GUC state between clients (PgBouncer in
 *		 transaction mode does NOT discard PGC_USERSET GUC state).
 *		 The 'M' header is per-message and confined to its own
 *		 transaction, so it has no equivalent leak.
 *	   * Visibility in pg_stat_statements / log_statement.  The SET
 *		 statements appear in query logs and pg_stat_statements
 *		 entries, mixing trace propagation with workload metrics.
 *		 The 'M' header is below the SQL layer and doesn't appear.
 *
 *	 Use SET / SET LOCAL when you have no other choice; prefer 'M'
 *	 when both are available.
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

#include "access/xact.h"
#include "fmgr.h"
#include "libpq/trace_context.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#include "otel.h"
#include "otel_internal.h"

PG_MODULE_MAGIC;

/*
 * The backend's root trace context: the parent of a span started with an
 * empty active stack.  Set by the otel_api.traceparent assign-hook (on
 * 'M' header arrival, SET, or parallel-worker RestoreGUCState), or by a
 * sqlcommenter parse, which is statement-scoped and bypasses the GUC so
 * SHOW doesn't report it and it doesn't outlive the statement.
 * ctx.tracestate is left NULL here; readers take it from
 * otel_tracestate_guc.
 */
OtelRootContext otel_root_ctx;

/* GUC variables --- canonical storage. */
static char *otel_traceparent_guc;
char	   *otel_tracestate_guc;
/* Phase 3: otel_current_span_id_guc removed; see otel_parallel.c. */

/* Behaviour-controlling GUCs.  trace_all_queries moved to
 * contrib/otel_postgres_tracing along with the query-tracing
 * hooks in Phase 4. */
bool		otel_emit_spans_to_log = false;
bool		otel_parse_sqlcommenter = false;

/* Span-storage and attribute limits; see _PG_init. */
int			otel_max_open_spans = 64;
int			otel_max_session_spans = 16;
int			otel_attr_value_max = 4096;
int			otel_max_span_bytes = 65536;

/*
 * otel_api's sampler policy.  Consulted by otel_run_sampler()
 * (otel_api.c) only for a brand-new root span or a remote parent ---
 * every other span simply inherits its local parent's decision.  See
 * OtelApiSampler for the OTEL_TRACES_SAMPLER-style values.
 */
int			otel_sampler_mode = OTEL_SAMPLER_PARENTBASED_ALWAYS_ON;
double		otel_sampler_arg = 1.0;

static const struct config_enum_entry otel_sampler_options[] = {
	{"always_on", OTEL_SAMPLER_ALWAYS_ON, false},
	{"always_off", OTEL_SAMPLER_ALWAYS_OFF, false},
	{"traceidratio", OTEL_SAMPLER_TRACEIDRATIO, false},
	{"parentbased_always_on", OTEL_SAMPLER_PARENTBASED_ALWAYS_ON, false},
	{"parentbased_always_off", OTEL_SAMPLER_PARENTBASED_ALWAYS_OFF, false},
	{"parentbased_traceidratio", OTEL_SAMPLER_PARENTBASED_TRACEIDRATIO, false},
	{NULL, 0, false},
};


static void assign_emit_spans_to_log(bool newval, void *extra);
static void assign_sampler(int newval, void *extra);

/* GUC check / assign hooks. */
static bool check_traceparent(char **newval, void **extra, GucSource source);
static void assign_traceparent(const char *newval, void *extra);

/* sqlcommenter extraction. */
static bool find_block_comment(const char *sql,
							   const char **body, size_t *bodylen);
static int	decode_percent_escape(const char *p, size_t remaining, char *out);
static bool extract_traceparent_from_comment(const char *body, size_t bodylen,
											 char *out, size_t outlen);


/*
 * GUC check-hook for otel_api.traceparent --- validates the W3C format.
 * An empty string or NULL is accepted as "clear".
 */
static bool
check_traceparent(char **newval, void **extra, GucSource source)
{
	OtelSpanContext tmp;

	if (*newval == NULL || (*newval)[0] == '\0')
		return true;

	if (!otel_traceparent_parse(*newval, &tmp))
	{
		GUC_check_errmsg("invalid W3C traceparent format: \"%s\"", *newval);
		GUC_check_errdetail("Expected \"00-{32 hex}-{16 hex}-{2 hex}\" with non-zero trace-id and parent-id.");
		return false;
	}
	return true;
}

/*
 * GUC assign-hook for otel_api.traceparent --- populates the in-memory
 * derived state.  Called on every value change, including by
 * RestoreGUCState in a parallel worker.
 */
static void
assign_traceparent(const char *newval, void *extra)
{
	OtelSpanContext tmp;

	otel_root_ctx_reset();
	if (newval == NULL || newval[0] == '\0')
		return;

	/* Vetted by the check-hook, so this can't fail; check anyway. */
	if (otel_traceparent_parse(newval, &tmp))
	{
		otel_root_ctx.ctx = tmp;
		otel_root_ctx.is_set = true;
	}
}

/*
 * Tracks whether the in-memory otel_root_ctx was last populated by an 'M'
 * TraceContext message (as opposed to a user-issued SET or a sqlcommenter
 * parse).  Used by otel_trace_context_clear_cb to decide whether to
 * clear at the ReadyForQuery boundary.
 */
static bool otel_ctx_from_M_header = false;

/*
 * Trace-context apply callback: invoked immediately on 'M' receipt
 * (protocol 3.3+).  Routes traceparent/tracestate through SetConfigOption
 * so the GUC machinery propagates to parallel workers and triggers the
 * assign hook (which updates the in-memory derived state).
 *
 * Malformed values are rejected by the GUC check-hook at LOG level so the
 * operation proceeds untagged rather than failing.
 */
static void
otel_trace_context_apply_cb(const char *traceparent, const char *tracestate,
							void *cb_ctx)
{
	/*
	 * An empty traceparent means "clear".  Map it to a GUC reset by
	 * passing NULL.
	 */
	(void) set_config_option("otel_api.traceparent",
							 (traceparent[0] == '\0') ? NULL : traceparent,
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SET, true, LOG, false);

	(void) set_config_option("otel_api.tracestate",
							 (tracestate[0] == '\0') ? NULL : tracestate,
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SET, true, LOG, false);

	/*
	 * Mark as M-installed only when traceparent was actually accepted (a
	 * malformed value leaves otel_root_ctx.is_set false).
	 */
	if (otel_root_ctx.is_set)
		otel_ctx_from_M_header = true;
}

/*
 * Trace-context clear callback: invoked at the ReadyForQuery boundary by
 * ClearTraceContext in core.  Resets the in-memory derived state for
 * M-installed context.
 *
 * Context installed via SET / SET LOCAL is owned by the GUC machinery;
 * we only clear what we installed via apply_cb.
 *
 * Resets otel_root_ctx directly rather than via set_config_option to avoid any
 * GUC-rollback interaction when the reset occurs during error recovery.
 */
static void
otel_trace_context_clear_cb(void *cb_ctx)
{
	if (otel_ctx_from_M_header)
	{
		otel_root_ctx_reset();
		otel_ctx_from_M_header = false;
	}
}

static void
assign_emit_spans_to_log(bool newval, void *extra)
{
	otel_emit_spans_to_log = newval;
	otel_update_recording_possible();
}

/*
 * GUC assign-hook for otel_api.sampler.  Sets otel_sampler_mode from
 * newval itself (the GUC machinery's own write to *valueAddr happens
 * only after this hook returns, same as assign_emit_spans_to_log()
 * above), then recomputes the recording_possible gate.
 */
static void
assign_sampler(int newval, void *extra)
{
	otel_sampler_mode = newval;
	otel_update_recording_possible();
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

	if (!otel_root_ctx.is_set)
		PG_RETURN_NULL();

	otel_traceparent_format(&otel_root_ctx.ctx, buf);
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
	DefineCustomStringVariable("otel_api.traceparent",
							   "W3C Trace Context traceparent for the current operation.",
							   "Format: \"00-{32 hex}-{16 hex}-{2 hex}\". Empty means no trace context.",
							   &otel_traceparent_guc,
							   "",
							   PGC_USERSET,
							   0,
							   check_traceparent,
							   assign_traceparent,
							   NULL);

	DefineCustomStringVariable("otel_api.tracestate",
							   "W3C Trace Context tracestate companion value.",
							   "Stored opaquely; not interpreted by this module.",
							   &otel_tracestate_guc,
							   "",
							   PGC_USERSET,
							   0,
							   NULL,
							   NULL,
							   NULL);

	/* Phase 3: the previous otel.current_span_id GUC has been
	 * superseded by the per-backend shared-memory slot mechanism in
	 * otel_parallel.c.  See OtelParallelContext in otel.h. */

	DefineCustomBoolVariable("otel_api.emit_spans_to_log",
							 "Emit completed spans as structured log lines.",
							 NULL,
							 &otel_emit_spans_to_log,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL, assign_emit_spans_to_log, NULL);

	DefineCustomIntVariable("otel_api.max_open_spans",
							"Maximum number of recording spans open at once in a backend.",
							"Spans started beyond this are not recorded.  Ended spans free "
							"their slot, so this doesn't limit spans per trace.",
							&otel_max_open_spans,
							64, 4, 4096,
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("otel_api.max_session_spans",
							"Maximum number of open session spans in a backend.",
							"Session spans have no transaction-scoped resource owner, "
							"so this budget stops leaked ones using up otel_api.max_open_spans.",
							&otel_max_session_spans,
							16, 0, 4096,
							PGC_SUSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("otel_api.attr_value_max",
							"Maximum length of a span attribute value; longer values are truncated.",
							NULL,
							&otel_attr_value_max,
							4096, 16, 1024 * 1024,
							PGC_SUSET,
							GUC_UNIT_BYTE,
							NULL, NULL, NULL);

	DefineCustomIntVariable("otel_api.max_span_bytes",
							"Maximum memory for one span's attributes, events and links.",
							"Attributes, events and links past this are dropped and counted.",
							&otel_max_span_bytes,
							65536, 1024, 64 * 1024 * 1024,
							PGC_SUSET,
							GUC_UNIT_BYTE,
							NULL, NULL, NULL);

	DefineCustomEnumVariable("otel_api.sampler",
							 "The sampler otel_api uses for a new root span or a remote parent.",
							 "Standard OTEL_TRACES_SAMPLER names.  parentbased_* variants use the "
							 "remote parent's W3C sampled bit when there is one, and fall back to "
							 "the non-parentbased behaviour of the same name for a brand-new root. "
							 "A local parent's own decision is always inherited regardless of this "
							 "setting.  Raising the sample rate cluster-wide is an operator decision "
							 "(PGC_SUSET); see otel.trace_all_queries for the per-session opt-in that "
							 "starts new root traces at all.  always_off disables tracing entirely: "
							 "no span IDs are generated, force_sample is ignored, and incoming trace "
							 "context still passes through.  Use traceidratio with sampler_arg = 0 "
							 "for unsampled spans that are still tracked.  See README \"Sampling\".",
							 &otel_sampler_mode,
							 OTEL_SAMPLER_PARENTBASED_ALWAYS_ON,
							 otel_sampler_options,
							 PGC_SUSET,
							 0,
							 NULL, assign_sampler, NULL);

	DefineCustomRealVariable("otel_api.sampler_arg",
							 "Sampling probability for the traceidratio and parentbased_traceidratio samplers.",
							 "Ignored by the other sampler values.  OTel consistent probability "
							 "sampling on the trace ID's low 56 bits: 1.0 always samples, 0.0 never "
							 "does.",
							 &otel_sampler_arg,
							 1.0, 0.0, 1.0,
							 PGC_SUSET,
							 0,
							 NULL, NULL, NULL);

	/* trace_all_queries is owned by otel_postgres_tracing; see its
	 * _PG_init for that GUC. */

	DefineCustomStringVariable("otel_api.service_name",
							   "OTel Resource service.name attribute for this postmaster.",
							   "Identifies the service emitting traces / metrics.  When unset "
							   "(the default), the value is resolved in order: "
							   "(1) this GUC if explicitly set to a non-empty value, "
							   "(2) the OTEL_SERVICE_NAME environment variable, "
							   "(3) the service.name key inside OTEL_RESOURCE_ATTRIBUTES, "
							   "(4) the hard-coded fallback \"postgres\".",
							   &otel_service_name_guc,
							   NULL,
							   PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("otel_api.service_instance_id",
							   "OTel Resource service.instance.id attribute for this postmaster.",
							   "Uniquely identifies this postmaster instance among instances "
							   "of the same service.  When unset (empty string, the default), "
							   "the value is resolved in order: "
							   "(1) this GUC if explicitly set to a non-empty value, "
							   "(2) the OTEL_SERVICE_INSTANCE_ID environment variable, "
							   "(3) the service.instance.id key inside OTEL_RESOURCE_ATTRIBUTES, "
							   "(4) the cluster's pg_control system identifier, which is stable "
							   "across postmaster restarts and unique per initdb.",
							   &otel_service_instance_id_guc,
							   "",
							   PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);

	DefineCustomBoolVariable("otel_api.parse_sqlcommenter",
							 "Extract trace context from sqlcommenter SQL comments when no other context is set.",
							 "Default off.  WARNING: structurally broken under driver-side or "
							 "server-side prepared statements --- the SQL text (and its comment) "
							 "is captured at Parse time and frozen in the cached plan, so every "
							 "subsequent Execute reuses the original traceparent.  Prefer the "
							 "'M' protocol header, or SET LOCAL otel_api.traceparent with "
							 "pipelining, for any workload that uses prepared statements.",
							 &otel_parse_sqlcommenter,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	/*
	 * Reserve the otel_api.* GUC namespace this module owns
	 * exclusively.  Other modules in the stack use different
	 * namespaces:
	 *   otel_postgres_tracing.*  --- the query-tracing consumer
	 *   oteltracingdemo.*        --- the Rust demo exporter
	 *   otel.trace_all_queries   --- legacy single-GUC carve-out
	 *                                (lives in otel_postgres_tracing
	 *                                 for back-compat)
	 *
	 * No cross-module shared namespace, so this reservation is safe
	 * to do at _PG_init time and gives operators a typo-protection
	 * warning on any unknown otel_api.* settings.
	 */
	MarkGUCPrefixReserved("otel_api");

	/*
	 * Register the trace-context handler.  Core invokes apply_cb on 'M'
	 * receipt and clear_cb at the next ReadyForQuery boundary.
	 */
	RegisterTraceContextHandler(otel_trace_context_apply_cb,
								otel_trace_context_clear_cb, NULL);

	/* otel_log_install_hooks() and otel_trace_install_hooks() moved
	 * to contrib/otel_postgres_tracing in Phase 4.  Operators must
	 * add 'otel_postgres_tracing' (after 'otel') to
	 * shared_preload_libraries to get query-tracing behaviour. */
	otel_producer_init();
	otel_parallel_init();
	otel_resource_init();
	otel_api_publish_rendezvous();
}

void
otel_root_ctx_reset(void)
{
	memset(&otel_root_ctx, 0, sizeof(otel_root_ctx));
}


/*
 * sqlcommenter --- in-band trace context smuggling.
 *
 * The spec (https://google.github.io/sqlcommenter/spec/) embeds
 * key=value pairs inside a SQL block comment at the start OR end
 * of the query, e.g.
 *
 *	 /+ traceparent='00-{32 hex}-{16 hex}-{2 hex}' +/ SELECT 1
 *	 SELECT 1 /+ traceparent='...',action='do%20thing' +/
 *
 * (using +/ for *\/ in this comment to avoid ending the C
 * comment.)
 *
 * Values are SQL-string-literal escaped, then URL-encoded.  Our
 * extractor only cares about `traceparent`, has a fixed-size
 * output buffer (a W3C traceparent is exactly 55 chars), and
 * refuses anything that doesn't round-trip cleanly.
 *
 * Why this exists in contrib/otel: Most OTel auto-
 * instrumentations emit sqlcommenter by default today and
 * expect server-side extraction.  See the top-of-file comment
 * for the limitations of this approach versus the protocol
 * header / GUC paths.
 */
#define OTEL_SQLCOMMENTER_SCAN_BUDGET	4096

/*
 * Find an open block comment at the start of `sql` (skipping
 * leading whitespace), or one at the end (skipping trailing
 * whitespace + a single trailing semicolon).  On success
 * returns true and writes [body, body+bodylen) for the comment
 * INTERIOR (between /\* and *\/).  No comment nesting handling
 * --- nested block comments aren't valid sqlcommenter anyway.
 */
static bool
find_block_comment(const char *sql, const char **body, size_t *bodylen)
{
	size_t		len = strlen(sql);
	size_t		i;
	size_t		end;
	const char *p;

	/* ---- Leading-comment scan ---- */
	i = 0;
	while (i < len && (sql[i] == ' ' || sql[i] == '\t' ||
					   sql[i] == '\n' || sql[i] == '\r'))
		i++;
	if (i + 4 <= len && sql[i] == '/' && sql[i + 1] == '*')
	{
		p = memmem(sql + i + 2, len - (i + 2), "*/", 2);
		if (p)
		{
			*body = sql + i + 2;
			*bodylen = (size_t) (p - (sql + i + 2));
			return true;
		}
		/* leading /+ with no close --- fall through to trailing check */
	}

	/* ---- Trailing-comment scan ---- */
	end = len;
	while (end > 0 && (sql[end - 1] == ' ' || sql[end - 1] == '\t' ||
					   sql[end - 1] == '\n' || sql[end - 1] == '\r' ||
					   sql[end - 1] == ';'))
		end--;
	if (end >= 4 && sql[end - 2] == '*' && sql[end - 1] == '/')
	{
		/* Found *\/; search backwards for the matching /+.  Bound
		 * the scan to a budget to avoid pathological behaviour on
		 * megabyte queries with no opening comment. */
		size_t		floor = (end > OTEL_SQLCOMMENTER_SCAN_BUDGET)
			? end - OTEL_SQLCOMMENTER_SCAN_BUDGET : 0;

		for (i = end - 2; i > floor; i--)
		{
			if (sql[i - 1] == '/' && sql[i] == '*')
			{
				*body = sql + i + 1;
				*bodylen = (size_t) ((end - 2) - (i + 1));
				return true;
			}
		}
	}

	return false;
}

/*
 * Decode a single %HH escape into out (single byte).  Returns the
 * number of input bytes consumed (3) on success, or 0 on
 * malformed input.
 */
static int
decode_percent_escape(const char *p, size_t remaining, char *out)
{
	int			hi,
				lo;

	if (remaining < 3)
		return 0;

	hi = p[1];
	lo = p[2];

	if (hi >= '0' && hi <= '9')
		hi = hi - '0';
	else if (hi >= 'a' && hi <= 'f')
		hi = hi - 'a' + 10;
	else if (hi >= 'A' && hi <= 'F')
		hi = hi - 'A' + 10;
	else
		return 0;

	if (lo >= '0' && lo <= '9')
		lo = lo - '0';
	else if (lo >= 'a' && lo <= 'f')
		lo = lo - 'a' + 10;
	else if (lo >= 'A' && lo <= 'F')
		lo = lo - 'A' + 10;
	else
		return 0;

	*out = (char) ((hi << 4) | lo);
	return 3;
}

/*
 * Search `body` (the interior of the block comment) for a
 * `traceparent='...'` key/value pair.  Comma-separated keys per
 * spec.  Value is URL-decoded into `out` (max `outlen` bytes
 * including NUL terminator); returns true on a clean decode.
 *
 * We don't try to be a full sqlcommenter parser --- only
 * traceparent is required for our purposes, and the spec's
 * other fields (action, controller, framework, ...) are
 * application metadata we don't surface today.
 */
static bool
extract_traceparent_from_comment(const char *body, size_t bodylen,
								 char *out, size_t outlen)
{
	const char *p = body;
	const char *end = body + bodylen;

	/* Walk the comma-separated key=value list. */
	while (p < end)
	{
		const char *key_start;
		size_t		key_len;
		const char *val_start;
		size_t		val_len;
		bool		matched_traceparent;

		/* Skip whitespace. */
		while (p < end && (*p == ' ' || *p == '\t'))
			p++;
		if (p >= end)
			return false;

		/* Key. */
		key_start = p;
		while (p < end && *p != '=' && *p != ',')
			p++;
		key_len = (size_t) (p - key_start);
		matched_traceparent = (key_len == 11 &&
							   strncmp(key_start, "traceparent", 11) == 0);

		if (p >= end || *p != '=')
		{
			/* No '=' for this key; skip to next ',' and continue. */
			while (p < end && *p != ',')
				p++;
			if (p < end)
				p++;	/* eat the comma */
			continue;
		}
		p++;	/* eat the '=' */

		/* Value: single-quoted string per the spec. */
		if (p >= end || *p != '\'')
		{
			/* Malformed --- bail entirely. */
			return false;
		}
		p++;	/* eat opening quote */
		val_start = p;
		while (p < end && *p != '\'')
			p++;
		val_len = (size_t) (p - val_start);
		if (p >= end)
			return false;
		p++;	/* eat closing quote */

		if (matched_traceparent)
		{
			/*
			 * URL-decode val_start..val_start+val_len into out.
			 * + does NOT mean space in sqlcommenter (the spec uses
			 * RFC-3986 percent-encoding, not form-encoding); pass
			 * '+' through verbatim.
			 */
			size_t		oi = 0;
			size_t		i;

			for (i = 0; i < val_len; )
			{
				if (oi + 1 >= outlen) /* +1 leaves room for NUL */
					return false;
				if (val_start[i] == '%')
				{
					int			used;

					used = decode_percent_escape(val_start + i,
												 val_len - i,
												 &out[oi]);
					if (used == 0)
						return false;
					i += used;
				}
				else
				{
					out[oi] = val_start[i];
					i++;
				}
				oi++;
			}
			out[oi] = '\0';
			return true;
		}

		/* Wrong key; skip to next comma and continue. */
		while (p < end && *p != ',')
			p++;
		if (p < end)
			p++;
	}

	return false;
}

/*
 * Orchestrator: if otel_api.parse_sqlcommenter is on AND otel_root_ctx is
 * not already populated by a higher-priority source ('M' header or
 * SET / SET LOCAL), try to extract a traceparent from a comment in
 * `sql` and apply it to otel_root_ctx directly (NOT via the GUC path
 * --- comment-derived context must NOT outlive the statement).
 *
 * Returns true if otel_root_ctx was populated from the comment; in that
 * case caller must arrange for finalize_span to clear it (via the
 * otel_root_ctx.from_comment flag this fn sets).
 *
 * On any malformed-comment or parse-validation failure: silently
 * proceed without context.  Tracing is best-effort.
 */
bool
otel_try_apply_sqlcommenter_context(const char *sql)
{
	const char *body;
	size_t		bodylen;
	char		raw[OTEL_TRACEPARENT_LEN + 1];
	OtelSpanContext tmp;

	if (!otel_parse_sqlcommenter)
		return false;
	if (otel_root_ctx.is_set)
		return false;	/* 'M' header / GUC always wins */
	if (sql == NULL || sql[0] == '\0')
		return false;

	if (!find_block_comment(sql, &body, &bodylen))
		return false;
	if (!extract_traceparent_from_comment(body, bodylen, raw, sizeof(raw)))
		return false;
	if (!otel_traceparent_parse(raw, &tmp))
		return false;

	otel_root_ctx.ctx = tmp;
	otel_root_ctx.is_set = true;
	otel_root_ctx.from_comment = true;
	return true;
}
