/*-------------------------------------------------------------------------
 *
 * otel.h
 *	  Public API for the contrib/otel OpenTelemetry trace-context module.
 *
 * This header defines the data model that contrib/otel produces for
 * every backend query operation it observes, and the hook through
 * which out-of-tree exporter extensions consume those spans.
 *
 * Architecture: contrib/otel does NOT ship a wire-format exporter ---
 * OTLP/protobuf/gRPC/libcurl etc. would all be dependencies that
 * disqualify it as a contrib.  Concrete exporters live as separate
 * loadable modules that register a callback against
 * otel_span_emit_hook and translate OtelSpan into whatever wire
 * format they need.  For zero-config users, contrib/otel ships a
 * built-in JSON-log fallback emitter gated by a GUC.
 *
 * Memory ownership and the exporter contract:
 *
 *	 * The OtelSpan passed to the hook, and all char* pointers it
 *	   transitively contains, are valid only for the duration of the
 *	   hook call.  An exporter that needs to defer work must copy.
 *	 * Some const char* pointers (notably OtelEventCore.filename and
 *	   .funcname) point into postgres rodata and are valid forever;
 *	   exporters may safely store these pointers without copying.
 *	   This is not true of OtelSpan.name, span attributes, or event
 *	   message/detail/hint, which may live in transient memory.
 *	 * Any of the optional extended-event fields (message, detail,
 *	   hint, attrs) may be NULL independently of the others, meaning
 *	   "this field was not captured" (typically because allocation
 *	   failed under memory pressure).  Treat NULL as omitted, not
 *	   empty.
 *	 * The hook MAY be invoked under allocation-failure conditions
 *	   (e.g. when finalizing a span on the error path after an OOM
 *	   ereport).  Exporters that allocate should guard against that.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/otel/otel.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONTRIB_OTEL_H
#define CONTRIB_OTEL_H

#include "datatype/timestamp.h"

/*
 * W3C / OpenTelemetry span status.  UNSET is the default; OK is set
 * only when the producer explicitly knows the operation succeeded;
 * ERROR is set on failure.
 */
typedef enum OtelSpanStatus
{
	OTEL_STATUS_UNSET = 0,
	OTEL_STATUS_OK = 1,
	OTEL_STATUS_ERROR = 2,
} OtelSpanStatus;

/*
 * OpenTelemetry span kind.  PostgreSQL backends are always SERVER
 * when they originate a span (responding to a client request).
 * INTERNAL is used for nested phases of work where there's no remote
 * caller-callee relationship.  CLIENT / PRODUCER / CONSUMER are
 * included for completeness and possible future use (e.g. FDW
 * callouts, logical replication).
 */
typedef enum OtelSpanKind
{
	OTEL_SPAN_KIND_INTERNAL = 0,
	OTEL_SPAN_KIND_SERVER = 1,
	OTEL_SPAN_KIND_CLIENT = 2,
	OTEL_SPAN_KIND_PRODUCER = 3,
	OTEL_SPAN_KIND_CONSUMER = 4,
} OtelSpanKind;

/*
 * Generic key/value pair used for span attributes and event
 * attributes.  Values are always strings for the POC; richer typing
 * (int, bool, double, array) can be added later without breaking the
 * exporter API.
 */
typedef struct OtelKeyValue
{
	const char *key;
	const char *value;
} OtelKeyValue;

/*
 * Common substrate of every captured event.
 *
 * The core is what gets captured first, unconditionally, with NO
 * string allocation --- only scalars and pointers to never-freed
 * constants.  filename and funcname point into postgres rodata
 * (__FILE__ / __func__ literals from the originating ereport site)
 * and are valid forever.  sqlstate is stored by-value (a SQLSTATE
 * is always 5 ASCII chars + NUL), not as a pointer, so it can be
 * captured without copying.
 *
 * The core is the substrate of every event, NOT an emergency
 * fallback used in lieu of the real thing --- see OtelSpanEvent.
 */
typedef struct OtelEventCore
{
	TimestampTz time;
	int			elevel;			/* WARNING / ERROR / FATAL / PANIC */
	char		sqlstate[6];	/* by-value: 5 chars + NUL */
	const char *filename;		/* __FILE__ literal; const for life */
	int			lineno;
	const char *funcname;		/* __func__ literal; const for life */
} OtelEventCore;

/*
 * One captured ereport on a span.
 *
 * core is always populated when the event slot is in use.  The
 * extended fields (message, detail, hint, attrs) are best-effort: any
 * of them may be NULL independently if that specific allocation
 * failed under memory pressure.  Exporters MUST tolerate NULL on each
 * field independently and treat NULL as "not captured."
 */
typedef struct OtelSpanEvent
{
	OtelEventCore core;
	const char *message;
	const char *detail;
	const char *hint;
	int			n_attrs;
	OtelKeyValue *attrs;
} OtelSpanEvent;

/*
 * Number of attribute slots stored inline on every OtelSpan.  Spans
 * with more attributes spill into an allocated overflow array.  Sized
 * to comfortably cover the OTel SQL semantic conventions
 * (db.system, db.name, db.statement, db.operation, db.user,
 *  net.peer.addr/port, application_name, query_id) without overflow.
 */
#define OTEL_INLINE_ATTRS 12

/*
 * A complete span.  Lifetime is from the producing hook (typically
 * ExecutorStart_hook) to the next finalization point
 * (ExecutorEnd_hook or XACT_EVENT_ABORT).  Storage may be a static
 * per-backend slab with a per-backend MemoryContext for variable
 * data --- contrib/otel uses that pattern internally to avoid
 * per-query palloc on the hot path.
 */
typedef struct OtelSpan
{
	/* W3C identity (lowercase hex, NUL-terminated).  trace_id and
	 * trace_flags come from the propagated trace context; span_id is
	 * generated locally; parent_span_id is the propagated parent
	 * (or the leader's span_id, for parallel workers --- see
	 * otel.current_span_id GUC). */
	char		trace_id[33];
	char		span_id[17];
	char		parent_span_id[17];
	char		trace_flags[3];
	const char *tracestate;		/* may be NULL */

	/* Descriptive */
	const char *name;
	OtelSpanKind kind;
	OtelSpanStatus status;
	const char *status_description; /* error message on ERROR; NULL otherwise */

	TimestampTz start_time;
	TimestampTz end_time;

	/* Attributes: inline up to OTEL_INLINE_ATTRS, then overflow. */
	int			n_attrs;		/* count of valid entries in attrs[] */
	OtelKeyValue attrs[OTEL_INLINE_ATTRS];
	int			n_overflow_attrs;
	OtelKeyValue *overflow_attrs;	/* NULL if not used or alloc failed */

	/* Events: first event has inline storage so its core can always
	 * be captured without allocation.  Additional events go to
	 * overflow_events; if that allocation fails, additional events
	 * are silently dropped (span status is still updated). */
	bool		inline_event_used;
	OtelSpanEvent inline_event;
	int			n_overflow_events;
	OtelSpanEvent *overflow_events; /* NULL if not used or alloc failed */
} OtelSpan;

/*
 * Hook for exporters.  Called once per span at finalization, in the
 * backend's memory context.
 *
 * The OtelSpan pointer and all referenced strings are valid only for
 * the duration of the call; an exporter that wants to defer work
 * (e.g. async batching) must copy what it needs.  The hook may be
 * invoked under allocation-failure conditions; exporters that
 * allocate should be prepared for that to fail.
 *
 * To chain multiple consumers, an exporter should stash the previous
 * value at registration time and forward to it after doing its own
 * work:
 *
 *	 static otel_span_emit_hook_type prev_hook;
 *
 *	 static void my_emit(const OtelSpan *s) {
 *	   ... do work ...
 *	   if (prev_hook) prev_hook(s);
 *	 }
 *
 *	 void _PG_init(void) {
 *	   prev_hook = otel_span_emit_hook;
 *	   otel_span_emit_hook = my_emit;
 *	 }
 */
typedef void (*otel_span_emit_hook_type) (const OtelSpan *span);
extern PGDLLIMPORT otel_span_emit_hook_type otel_span_emit_hook;

#endif							/* CONTRIB_OTEL_H */
