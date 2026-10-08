/*-------------------------------------------------------------------------
 *
 * otel_producer.h
 *	  The producer API: for extensions that create spans.
 *
 * otel_api owns all span storage.  A producer holds an 8-byte
 * OtelSpanRef handle, never a span struct:
 *
 *	   static OtelTracer my_tracer = {.name = "my_ext", .version = "1.0"};
 *
 *	   OtelSpanRef s = otel_span_start(.tracer = &my_tracer,
 *									   .name = "my_ext.work");
 *	   otel_span_set_int(s, "my_ext.rows", nrows);
 *	   ... work ...
 *	   otel_span_end(s);
 *
 * Every function accepts any handle, including OTEL_SPAN_NONE, so no
 * "is tracing on?" check is needed around the calls.  All the functions
 * are inline wrappers: for a span that isn't recording, a setter is one
 * compare and no call.  Build an expensive value only when it will be
 * used:
 *
 *	   if (otel_span_recording(s))
 *		   otel_span_set_str(s, "db.query.text", deparse(...));
 *
 * or use OTEL_SPAN_SET_STR_IF_RECORDING(), which does the same.
 *
 * Rules, all checked:
 *	 - Never call the API in a critical section.  cassert builds fail an
 *	   Assert; other builds get OTEL_SPAN_NONE or a no-op.
 *	 - Never call the API from an emit hook (see otel_exporter.h), except
 *	   otel_span_current() and otel_span_context_of().  Same checks.
 *	 - A handle is dead after otel_span_end().  Using it again is a no-op
 *	   that is counted, and fails an Assert in cassert builds.
 *	 - Spans on the active stack should end in LIFO order.  Ending a span
 *	   with others above it ends those first, each exported with ERROR
 *	   status, with a WARNING, and fails an Assert in cassert builds.
 *	 - So a span on the active stack must end within the call that started
 *	   it (or a callee), not in a later SQL statement.  Other producers
 *	   push spans of their own in between: otel_postgres_tracing wraps
 *	   every statement, including each statement of a plpgsql function.
 *	   A span that starts in one statement and ends in another must be
 *	   .detached.
 *	 - A .detached span can still be made current, so later work parents
 *	   to it: otel_span_activate(s) pushes it onto the active stack and
 *	   returns a token; otel_span_deactivate(token) pops it again.  Only
 *	   a .detached, still-open span may be activated; a non-detached
 *	   span is already on the stack from otel_span_start(), and
 *	   activating it again, or activating it twice, is refused (counted;
 *	   an Assert failure in cassert builds).  An activation is checked
 *	   the same way as any other stack entry: deactivating one with
 *	   others pushed above it ends those first (LIFO violation,
 *	   WARNING), exactly as otel_span_end() does.  Ending the span
 *	   itself while it is still active pops it off the stack like any
 *	   other entry -- deactivate it first if you also hold the token, or
 *	   the token becomes a stale handle (same as using one after
 *	   otel_span_end() elsewhere).
 *
 * Lifetime.  Every recording span belongs to a resource owner: by default
 * CurrentResourceOwner, or the one given in .owner.  When the owner is
 * released on abort, the span is exported with ERROR status.  When it is
 * released on commit with the span still open, that is a leak: core
 * prints "resource was not closed", otel_api counts it, and the span is
 * dropped.  With no resource owner (outside a transaction, e.g. in a
 * background worker loop), or with .owner = OTEL_OWNER_SESSION, the span
 * is a session span: it lives until it is ended, or until backend exit,
 * and session spans have their own budget (otel_api.max_session_spans).
 *
 * All strings passed in are copied.  Attribute values longer than
 * otel_api.attr_value_max bytes are truncated.
 *
 * To make otel_api an optional dependency, include otel_producer_stub.h
 * instead of this header when otel_api's headers aren't available.  It
 * declares the same names as no-ops.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * otel_api/otel_producer.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef OTEL_PRODUCER_H
#define OTEL_PRODUCER_H

#include "miscadmin.h"
#include "utils/elog.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"

#include "otel_api.h"

#define OTEL_PRODUCER_API_MAJOR		1
#define OTEL_PRODUCER_API_MINOR		1
#define OTEL_PRODUCER_API_VERSION	OTEL_MAKE_VERSION(OTEL_PRODUCER_API_MAJOR, \
													  OTEL_PRODUCER_API_MINOR)

/*
 * Handle to a span.
 *	 v > 0	a recording span
 *	 v < 0	an unsampled span: it has a trace context that propagates, but
 *			records nothing
 *	 v == 0	no span (OTEL_SPAN_NONE)
 * Opaque otherwise.  Valid only in the backend that created it.
 */
typedef struct OtelSpanRef
{
	int64		v;
} OtelSpanRef;

#define OTEL_SPAN_NONE	((OtelSpanRef) {0})

static inline bool
otel_span_recording(OtelSpanRef s)
{
	return s.v > 0;
}

/*
 * Token from otel_span_activate(), for otel_span_deactivate().  Opaque;
 * valid only in the backend that created it, and only until the
 * activated span ends or is deactivated.
 */
typedef struct OtelActivation
{
	int64		v;
} OtelActivation;

#define OTEL_ACTIVATION_NONE	((OtelActivation) {0})

/*
 * A producer's tracer (OTel InstrumentationScope).  Declare one static
 * per producer, with name set; otel_api fills scope on first use.
 */
typedef struct OtelTracer
{
	const char *name;
	const char *version;		/* may be NULL */
	const char *schema_url;		/* may be NULL */
	const OtelInstrumentationScope *scope;	/* set by otel_api */
} OtelTracer;

/* Where a new span's parent comes from. */
typedef enum OtelSpanParent
{
	/*
	 * The top of the active stack.  If the stack is empty: in a parallel
	 * worker, the leader's published context; otherwise the backend's
	 * root context (the 'M' protocol header, otel_api.traceparent, or
	 * sqlcommenter).  If there is none, a new trace.
	 */
	OTEL_PARENT_ACTIVE = 0,
	OTEL_PARENT_CONTEXT,		/* .parent_ctx; NULL or invalid = new trace */
	OTEL_PARENT_SPAN,			/* .parent_span; OTEL_SPAN_NONE = new trace */
	OTEL_PARENT_ROOT,			/* always a new trace */
} OtelSpanParent;

/* Special value for OtelSpanStartArgs.owner. */
#define OTEL_OWNER_SESSION	((ResourceOwner) (uintptr_t) 1)

typedef struct OtelSpanStartArgs
{
	uint32		struct_size;	/* set by otel_span_start() */
	OtelTracer *tracer;
	const char *name;			/* copied; required */
	OtelSpanKind kind;
	OtelSpanParent parent;
	const OtelSpanContext *parent_ctx;
	OtelSpanRef parent_span;

	/*
	 * NULL: CurrentResourceOwner, or the session if there is none.
	 * OTEL_OWNER_SESSION: the session.  Otherwise this owner.
	 */
	ResourceOwner owner;

	/*
	 * Don't push onto the active stack.  For spans that aren't nested in
	 * the C call stack, e.g. a transaction span ended from a later
	 * callback.  Children must name it with OTEL_PARENT_SPAN.
	 */
	bool		detached;

	/*
	 * The span must end in this C stack frame or one of its callees.
	 * cassert builds fail an Assert when a later start or end finds the
	 * frame has returned with the span still open (a leak via early
	 * return).  No effect in other builds.
	 */
	bool		scoped;

	/* Start time; 0 means now. */
	TimestampTz start_time;

	/*
	 * Record the span even if otel_api.sampler would drop it ---
	 * but only when the span starts a new root trace (no parent
	 * context at all).  For operator settings that ask to trace
	 * everything with no client-supplied context, such as
	 * otel.trace_all_queries.  Has no effect when the span has a
	 * parent (an active local span, recording or not, or a remote
	 * context): the span then always follows the parent's sampling
	 * decision, sampled or not.  Forcing a child to record under an
	 * unrecorded parent would produce an orphan span, since the
	 * parent is never exported.  The span is exported with
	 * sampled=1, and its children inherit that as usual.
	 */
	bool		force_sample;
} OtelSpanStartArgs;

typedef struct OtelProducerApi
{
	uint32		version;		/* OTEL_PRODUCER_API_VERSION */
	uint32		struct_size;	/* sizeof(OtelProducerApi) */

	/*
	 * false when no span can be recorded: no emit hook is registered and
	 * log emission is off.  Read inline by otel_span_start() so that case
	 * costs no call.
	 */
	const bool *recording_possible;

	OtelSpanRef (*span_start) (const OtelSpanStartArgs *args);
	void		(*span_end) (OtelSpanRef s, TimestampTz end_time);

	void		(*span_set_str) (OtelSpanRef s, const char *key, const char *val);
	void		(*span_set_int) (OtelSpanRef s, const char *key, int64 val);
	void		(*span_set_double) (OtelSpanRef s, const char *key, double val);
	void		(*span_set_bool) (OtelSpanRef s, const char *key, bool val);
	void		(*span_set_vprintf) (OtelSpanRef s, const char *key,
									 const char *fmt, va_list ap)
				pg_attribute_printf(3, 0);

	/* Rename the span, e.g. once the operation is known. */
	void		(*span_set_name) (OtelSpanRef s, const char *name);
	void		(*span_set_status) (OtelSpanRef s, OtelSpanStatus code,
									const char *description);
	void		(*span_add_event) (OtelSpanRef s, const char *name,
								   TimestampTz ts, const OtelAttribute *attrs,
								   int n_attrs);
	void		(*span_add_link) (OtelSpanRef s, const OtelSpanContext *target);

	/*
	 * Copy the error currently being handled into the span and set ERROR
	 * status.  For PG_CATCH blocks, before FlushErrorState() or a
	 * re-throw.  Errors that reach the top level are captured
	 * automatically; this is for errors that are caught.
	 */
	void		(*span_capture_error) (OtelSpanRef s);
	/* The same, from an ErrorData the caller already has. */
	void		(*span_record_error) (OtelSpanRef s, const ErrorData *edata);

	/* The top of the active stack, or OTEL_SPAN_NONE. */
	OtelSpanRef (*span_current) (void);

	/*
	 * Fill *out with the span's context, for propagation.  For
	 * OTEL_SPAN_NONE, the context a new child would inherit: the top of
	 * the active stack, else the root or parallel-leader context.
	 * Returns false, with *out zeroed, if there is no context.  The
	 * tracestate pointer is valid until the stack or root context
	 * changes.
	 */
	bool		(*span_context_of) (OtelSpanRef s, OtelSpanContext *out);

	/* Add or replace a process-level Resource attribute.  Copied. */
	void		(*resource_add) (const char *key, const char *value);

	/*
	 * Drop the span without exporting it.  It is removed from wherever it
	 * is on the active stack; spans above it are left alone.  For a
	 * producer that abandons spans it knows won't be ended, e.g. from an
	 * abort callback.
	 */
	void		(*span_discard) (OtelSpanRef s);

	/*
	 * Push a .detached, still-open span onto the active stack so it
	 * becomes current; new spans using OTEL_PARENT_ACTIVE parent to it.
	 * Returns OTEL_ACTIVATION_NONE, refused and counted, for: s ==
	 * OTEL_SPAN_NONE; a stale s; a non-.detached span (it is already on
	 * the stack); a span already active; or the active stack being full.
	 */
	OtelActivation (*span_activate) (OtelSpanRef s);

	/*
	 * Pop an activation.  Checked exactly like otel_span_end(): if other
	 * entries were pushed above it since, they are ended first (LIFO
	 * violation, WARNING, each exported with ERROR status), same as
	 * ending a span out of order.  A stale or already-deactivated token
	 * is refused and counted, same as a stale span handle.
	 */
	void		(*span_deactivate) (OtelActivation tok);
} OtelProducerApi;


/* ----------------------------------------------------------------
 * Inline entry points.  Use these, not the table.
 * ---------------------------------------------------------------- */

/* The producer table, or NULL.  Cached per translation unit. */
static inline const OtelProducerApi *
otel_producer_api(void)
{
	static const void *cache = NULL;

	if (likely(cache != NULL))
		return cache == OTEL_API_MISSING ? NULL
			: (const OtelProducerApi *) cache;
	{
		const OtelApi *api = otel_api_get();
		const OtelProducerApi *p = api ? (const OtelProducerApi *) api->producer : NULL;

		if (p == NULL ||
			!otel_api_table_ok("producer", p->version, p->struct_size,
							   OTEL_PRODUCER_API_MAJOR,
							   OTEL_PRODUCER_API_MINOR,
							   sizeof(OtelProducerApi)))
		{
			cache = OTEL_API_MISSING;
			return NULL;
		}
		cache = p;
		return p;
	}
}

/*
 * False when no span started now could be recorded: otel_api isn't
 * loaded, nothing consumes spans (no exporter registered, log emission
 * off), or otel_api.sampler is plain always_off.  True doesn't mean a
 * span will be sampled (e.g. traceidratio with sampler_arg = 0 still
 * returns true here, and drops the span in the sampler instead).  Lets
 * a producer skip its own per-call setup in the common untraced case;
 * otel_span_start() already checks it, before building its arguments.
 */
static inline bool
otel_recording_possible(void)
{
	const OtelProducerApi *p = otel_producer_api();

	return p != NULL && *p->recording_possible;
}

static inline OtelSpanRef
otel_span_start_args(const OtelSpanStartArgs *args)
{
	if (!otel_recording_possible())
		return OTEL_SPAN_NONE;
	return otel_producer_api()->span_start(args);
}

/*
 * otel_span_start(.name = ..., ...): designated initialisers for args.
 *
 * The gate is checked first, as the condition of a ?:, so the compound
 * literal is only ever constructed on the branch that is actually taken:
 * with recording not possible, nothing is stored to build it.
 */
#define otel_span_start(...) \
	(otel_recording_possible() \
	 ? otel_span_start_args(&(OtelSpanStartArgs) { \
			.struct_size = sizeof(OtelSpanStartArgs), __VA_ARGS__ }) \
	 : OTEL_SPAN_NONE)

static inline void
otel_span_end(OtelSpanRef s)
{
	if (s.v != 0)
		otel_producer_api()->span_end(s, 0);
}

static inline void
otel_span_end_at(OtelSpanRef s, TimestampTz end_time)
{
	if (s.v != 0)
		otel_producer_api()->span_end(s, end_time);
}

static inline void
otel_span_discard(OtelSpanRef s)
{
	if (s.v != 0)
		otel_producer_api()->span_discard(s);
}

static inline OtelActivation
otel_span_activate(OtelSpanRef s)
{
	const OtelProducerApi *p = otel_producer_api();

	if (p == NULL || s.v == 0)
		return OTEL_ACTIVATION_NONE;
	return p->span_activate(s);
}

static inline void
otel_span_deactivate(OtelActivation tok)
{
	if (tok.v != 0)
		otel_producer_api()->span_deactivate(tok);
}

static inline void
otel_span_set_str(OtelSpanRef s, const char *key, const char *val)
{
	if (s.v > 0)
		otel_producer_api()->span_set_str(s, key, val);
}

static inline void
otel_span_set_int(OtelSpanRef s, const char *key, int64 val)
{
	if (s.v > 0)
		otel_producer_api()->span_set_int(s, key, val);
}

static inline void
otel_span_set_double(OtelSpanRef s, const char *key, double val)
{
	if (s.v > 0)
		otel_producer_api()->span_set_double(s, key, val);
}

static inline void
otel_span_set_bool(OtelSpanRef s, const char *key, bool val)
{
	if (s.v > 0)
		otel_producer_api()->span_set_bool(s, key, val);
}

static inline void otel_span_set_printf(OtelSpanRef s, const char *key,
										const char *fmt,...) pg_attribute_printf(3, 4);

static inline void
otel_span_set_printf(OtelSpanRef s, const char *key, const char *fmt,...)
{
	if (s.v > 0)
	{
		va_list		ap;

		va_start(ap, fmt);
		otel_producer_api()->span_set_vprintf(s, key, fmt, ap);
		va_end(ap);
	}
}

/* Evaluate expr only if s is recording. */
#define OTEL_SPAN_SET_STR_IF_RECORDING(s, key, expr) \
	do { \
		OtelSpanRef _otel_s = (s); \
		if (otel_span_recording(_otel_s)) \
			otel_span_set_str(_otel_s, (key), (expr)); \
	} while (0)

static inline void
otel_span_set_name(OtelSpanRef s, const char *name)
{
	if (s.v > 0)
		otel_producer_api()->span_set_name(s, name);
}

static inline void
otel_span_set_status(OtelSpanRef s, OtelSpanStatus code, const char *description)
{
	if (s.v > 0)
		otel_producer_api()->span_set_status(s, code, description);
}

static inline void
otel_span_add_event(OtelSpanRef s, const char *name, TimestampTz ts,
					const OtelAttribute *attrs, int n_attrs)
{
	if (s.v > 0)
		otel_producer_api()->span_add_event(s, name, ts, attrs, n_attrs);
}

static inline void
otel_span_add_link(OtelSpanRef s, const OtelSpanContext *target)
{
	if (s.v > 0)
		otel_producer_api()->span_add_link(s, target);
}

static inline void
otel_span_capture_error(OtelSpanRef s)
{
	if (s.v > 0)
		otel_producer_api()->span_capture_error(s);
}

static inline void
otel_span_record_error(OtelSpanRef s, const ErrorData *edata)
{
	if (s.v > 0)
		otel_producer_api()->span_record_error(s, edata);
}

static inline OtelSpanRef
otel_span_current(void)
{
	const OtelProducerApi *p = otel_producer_api();

	return p ? p->span_current() : OTEL_SPAN_NONE;
}

static inline bool
otel_span_context_of(OtelSpanRef s, OtelSpanContext *out)
{
	const OtelProducerApi *p = otel_producer_api();

	if (p == NULL)
	{
		memset(out, 0, sizeof(*out));
		return false;
	}
	return p->span_context_of(s, out);
}

static inline void
otel_resource_add(const char *key, const char *value)
{
	const OtelProducerApi *p = otel_producer_api();

	if (p != NULL)
		p->resource_add(key, value);
}

#endif							/* OTEL_PRODUCER_H */
