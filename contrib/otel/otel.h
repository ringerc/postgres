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
 * Sampler decision returned by the sampler hook, mirroring the
 * categories of OTel's SDK Sampler interface.
 *
 * DROP: the span should not be recorded or emitted; contrib/otel
 * skips allocation entirely.
 *
 * RECORD_ONLY: the span should be recorded locally but, if/when
 * contrib/otel ever propagates a child traceparent downstream, the
 * sampled bit should NOT be set (because we are recording for our
 * own purposes, not because the global trace is being sampled).
 *
 * RECORD_AND_SAMPLE: the span should be recorded locally AND
 * propagated as sampled.  This is what we use for the
 * upstream-says-sampled (W3C sampled-bit-set) path; sampler hooks
 * may also choose this to "promote" an unsampled trace.
 *
 * Mirrors the OpenTelemetry SDK's `SamplingDecision` enum.
 */
typedef enum OtelSamplerDecision
{
	OTEL_SAMPLE_DROP = 0,
	OTEL_SAMPLE_RECORD_ONLY = 1,
	OTEL_SAMPLE_RECORD_AND_SAMPLE = 2,
} OtelSamplerDecision;

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

	/* Sampling decision under which this span was recorded.
	 * RECORD_AND_SAMPLE for the propagated-sampled-bit-set path and
	 * for trace_all_queries; otherwise whatever the sampler hook
	 * returned (RECORD_ONLY or RECORD_AND_SAMPLE; DROP never reaches
	 * here because the span isn't created). */
	OtelSamplerDecision sampler_decision;

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
 * Input to the sampler hook.  Populated with the minimum context
 * the hook needs to make a decision WITHOUT contrib/otel having
 * performed any allocation yet.  All pointers are valid only for
 * the duration of the call; a sampler that wants to defer must
 * copy.  All pointers may point at backend-owned long-lived
 * memory (string literals, GUC values) and must not be freed by
 * the hook.
 *
 * Hooks that want richer context (db.name, user, application_name,
 * etc.) can read it from postgres globals (MyDatabaseId,
 * MyProcPort, application_name GUC) at the cost of their own
 * allocation/lookup --- which is the sampler's choice to make,
 * not contrib/otel's.
 */
typedef struct OtelSamplerInput
{
	const char *trace_id;			/* 32 hex chars, NUL-terminated */
	const char *parent_span_id;		/* 16 hex chars (the propagated parent) */
	const char *trace_flags;		/* 2 hex chars */
	const char *tracestate;			/* may be NULL */
	const char *name;				/* proposed span name (command tag) */
	OtelSpanKind kind;
} OtelSamplerInput;

/*
 * Hook called BEFORE contrib/otel allocates anything for a span,
 * when the propagated traceparent has the W3C sampled bit UNSET.
 *
 * Default (hook NULL) is OTel-SDK ParentBasedSampler behaviour:
 * respect the propagated unsampled state and skip the span
 * entirely.  An exporter module that needs richer sampling
 * semantics --- ratio-based, rate-limited, tail-based,
 * tenant-aware --- can register a hook here and return its own
 * decision.  The hook MUST be fast (~nanoseconds) and MUST NOT
 * allocate if it can possibly avoid it, since its whole purpose
 * is to be cheaper than the work we are about to do.
 *
 * The hook is NOT called for the sampled-bit-set path; an upstream
 * positive sampling signal is always honoured.  An exporter that
 * wants to override sampled=1 (e.g. to drop on local rate-limit)
 * can do so at emit time by returning early from the
 * otel_span_emit_hook.
 */
typedef OtelSamplerDecision (*otel_sampler_hook_type) (const OtelSamplerInput *in);

/*
 * Policy controlling WHEN the registered sampler hook is consulted.
 *
 * Default (no exporter calls set_sampler_policy) is
 * HOOK_ON_UNSAMPLED_BIT --- contrib/otel respects W3C `sampled=1` as a
 * binding "yes, record" signal and only consults the hook when the
 * propagated bit is unset.  Exporters that want different semantics
 * (always defer to their SDK, always record, ignore the hook entirely
 * and just use the wire bit) can override at _PG_init time via
 * OtelTracingApi.set_sampler_policy.
 *
 * The four values correspond to the four common decision regimes an
 * out-of-tree exporter might want:
 *
 *	  HOOK_ON_UNSAMPLED_BIT   (default)
 *		W3C-compliant.  sampled=1 → RECORD_AND_SAMPLE.  sampled=0 →
 *		call hook; if no hook, DROP.  This is what
 *		contrib/otel has done since the sampler hook was introduced.
 *
 *	  HOOK_ALWAYS
 *		Defer every sampling decision to the hook, regardless of the
 *		propagated bit.  Useful for exporters whose SDK has its own
 *		opinion about overriding upstream signals (rate limiters,
 *		tail-based samplers).  Risk: violates W3C TraceContext spec's
 *		"sampled=1 means recorded" guarantee for downstream
 *		consumers.  Caller's responsibility to know what they're doing.
 *
 *	  NEVER_HOOK_RESPECT_BIT
 *		Pure W3C ParentBased.  sampled=1 → record, sampled=0 → drop,
 *		hook is never invoked.  Useful for exporters that want zero
 *		policy code on the hot path and trust the upstream's wire
 *		signal exclusively.
 *
 *	  NEVER_HOOK_ALWAYS_SAMPLE
 *		Record everything that reached gate 4 (i.e. has a propagated
 *		context).  Equivalent to "no sampler hook, no
 *		trace_all_queries, but always record what we see."  Useful
 *		for debug-mode operators who want to capture every traced
 *		query without paying for a sampler call.
 *
 * Set via api->set_sampler_policy(policy) from _PG_init.  Setting
 * after _PG_init is permitted (it's just a single atomic word write)
 * but has no defined synchronization with in-flight queries.
 */
typedef enum OtelSamplerHookPolicy
{
	OTEL_SAMPLER_HOOK_ON_UNSAMPLED_BIT = 0,	/* default */
	OTEL_SAMPLER_HOOK_ALWAYS = 1,
	OTEL_SAMPLER_HOOK_NEVER_RESPECT_BIT = 2,
	OTEL_SAMPLER_HOOK_NEVER_ALWAYS_SAMPLE = 3,
} OtelSamplerHookPolicy;

/*
 * Hook for exporters.  Called once per span at finalization, in the
 * backend's memory context.
 *
 * The OtelSpan pointer and all referenced strings are valid only for
 * the duration of the call; an exporter that wants to defer work
 * (e.g. async batching) must copy what it needs.  The hook may be
 * invoked under allocation-failure conditions; exporters that
 * allocate should be prepared for that to fail.
 */
typedef void (*otel_span_emit_hook_type) (const OtelSpan *span);

/*
 * Backward-compatibility umbrella: the OtelTracingApi registration
 * surface lived in this file before contrib/otel split its public
 * headers.  Pull it in so existing consumers that include only
 * <otel/otel.h> see the same symbols they always did.
 */
#include "otel_api.h"

#endif							/* CONTRIB_OTEL_H */
