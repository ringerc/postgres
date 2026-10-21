/*-------------------------------------------------------------------------
 *
 * otel_internal_api.h
 *	  The internal API: for otel_postgres_tracing, and for tests.
 *
 * Access to the backend's root trace context (from the 'M' protocol
 * header, otel_api.traceparent or sqlcommenter), the parallel-worker
 * context handoff, and otel_api's own counters.  Ordinary producers
 * don't need any of this: otel_span_start() and otel_span_context_of()
 * already use the root and parallel-leader contexts.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * otel_api/otel_internal_api.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef OTEL_INTERNAL_API_H
#define OTEL_INTERNAL_API_H

#include "otel_api.h"

#define OTEL_INTERNAL_API_MAJOR		1
#define OTEL_INTERNAL_API_MINOR		2
#define OTEL_INTERNAL_API_VERSION	OTEL_MAKE_VERSION(OTEL_INTERNAL_API_MAJOR, \
													  OTEL_INTERNAL_API_MINOR)

typedef struct OtelRootContext
{
	bool		is_set;
	bool		from_comment;	/* set by sqlcommenter; cleared per statement */
	OtelSpanContext ctx;		/* tracestate valid until the next change */
} OtelRootContext;

/*
 * Per-backend counts of everything otel_api dropped, refused or
 * repaired.  A trace with a gap should show up here.
 */
typedef struct OtelApiCounters
{
	uint64		spans_started;		/* recording spans */
	uint64		spans_unsampled;	/* non-recording stack entries */
	uint64		spans_emitted;
	uint64		spans_discarded;	/* otel_span_discard() */

	/* otel_span_start refused, returning OTEL_SPAN_NONE */
	uint64		start_no_slot;		/* otel_api.max_open_spans reached */
	uint64		start_no_session_slot;	/* otel_api.max_session_spans reached */
	uint64		start_stack_full;	/* active stack depth limit */
	uint64		start_bad_args;		/* no name, bad struct_size, stale parent */

	uint64		in_crit_section;	/* any producer call, refused */
	uint64		in_emit_hook;		/* any producer call from an emit hook,
									 * refused */
	uint64		stale_handle;		/* used after end, or foreign */
	uint64		non_lifo_end;		/* spans ended out of stack order */
	uint64		unwound;			/* ended by owner release or by an enclosing
									 * span ending first, exported as ERROR */
	uint64		leaked_at_commit;
	uint64		dropped_in_dispatch;	/* released again by its owner after a
										 * FATAL during its dispatch; not
										 * exported again */
	uint64		open_at_exit;		/* session spans open at backend exit */

	uint64		attr_truncated;
	uint64		attr_dropped;		/* per-span byte limit or OOM */
	uint64		event_dropped;
	uint64		link_dropped;
	uint64		error_capture_failed;
	uint64		emit_hook_errors;	/* ERRORs raised by emit hooks */

	/* otel_span_activate() / otel_span_deactivate() misuse, refused */
	uint64		activate_not_detached;	/* span isn't .detached */
	uint64		activate_already_active;	/* span is already on the stack */
	uint64		deactivate_not_active;	/* token isn't currently active */

	/*
	 * An activation popped without otel_span_deactivate(): by a LIFO
	 * unwind passing over it (the span it names is left open, owned
	 * elsewhere), or by (sub)transaction end.  leaked_at_commit_count
	 * is the subset popped at top-level COMMIT/PREPARE -- the caller
	 * forgot to deactivate before committing, same shape as a leaked
	 * span, but the activation itself is not ended.  Popped at ABORT
	 * (top-level or subtransaction) is ordinary cleanup and not
	 * counted as a leak, only in activation_unwound if that also
	 * happened to be via an unwind.
	 */
	uint64		activation_unwound;
	uint64		activation_leaked_at_commit;
} OtelApiCounters;

typedef struct OtelInternalApi
{
	uint32		version;		/* OTEL_INTERNAL_API_VERSION */
	uint32		struct_size;	/* sizeof(OtelInternalApi) */

	void		(*get_root_context) (OtelRootContext *out);
	void		(*reset_root_context) (void);

	/*
	 * Apply a traceparent from a sqlcommenter comment in sql, if
	 * otel_api.parse_sqlcommenter is on and no root context is set.
	 * Returns true if one was applied.
	 */
	bool		(*try_apply_sqlcommenter_context) (const char *sql);

	/*
	 * Parallel query: the leader publishes the context its workers'
	 * spans should use as parent, and clears it when done.  Workers read
	 * it automatically when their active stack is empty.
	 */
	void		(*parallel_publish_leader_context) (const OtelSpanContext *ctx);
	void		(*parallel_clear_leader_context) (void);
	bool		(*parallel_get_leader_context) (OtelSpanContext *out);

	void		(*get_counters) (OtelApiCounters *out);
} OtelInternalApi;

static inline const OtelInternalApi *
otel_internal_api(void)
{
	static const void *cache = NULL;

	if (likely(cache != NULL))
		return cache == OTEL_API_MISSING ? NULL
			: (const OtelInternalApi *) cache;
	{
		const OtelApi *api = otel_api_get();
		const OtelInternalApi *i = api ? (const OtelInternalApi *) api->internal : NULL;

		if (i == NULL ||
			!otel_api_table_ok("internal", i->version, i->struct_size,
							   OTEL_INTERNAL_API_MAJOR,
							   OTEL_INTERNAL_API_MINOR,
							   sizeof(OtelInternalApi)))
		{
			cache = OTEL_API_MISSING;
			return NULL;
		}
		cache = i;
		return i;
	}
}

#endif							/* OTEL_INTERNAL_API_H */
