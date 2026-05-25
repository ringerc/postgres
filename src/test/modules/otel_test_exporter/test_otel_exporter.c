/*-------------------------------------------------------------------------
 *
 * test_otel_exporter.c
 *	  Tiny test-only exporter for contrib/otel.
 *
 * Registers a callback against contrib/otel's otel_span_emit_hook;
 * captures completed spans into a fixed-size per-backend ring buffer;
 * exposes SQL functions that TAP tests use to read out the captured
 * spans and assert on their contents.
 *
 * Captures spans by deep-copying everything we care about into
 * private storage at hook time, so the test SQL can fetch them later
 * (even after the originating transaction has ended).
 *
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/modules/otel_test_exporter/test_otel_exporter.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include "../../../../contrib/otel/otel.h"

PG_MODULE_MAGIC;

/*
 * Captured span.  String fields are deep-copied into otel_test_cxt at
 * capture time so they survive past the originating transaction.
 */
typedef struct CapturedKV
{
	char	   *key;
	char	   *value;
} CapturedKV;

typedef struct CapturedEvent
{
	OtelEventCore core;			/* core is by-value; safe */
	char	   *message;
	char	   *detail;
	char	   *hint;
	int			n_attrs;
	CapturedKV *attrs;
} CapturedEvent;

typedef struct CapturedSpan
{
	char		trace_id[33];
	char		span_id[17];
	char		parent_span_id[17];
	char		trace_flags[3];
	char	   *tracestate;
	char	   *name;
	OtelSpanKind kind;
	OtelSpanStatus status;
	char	   *status_description;
	TimestampTz start_time;
	TimestampTz end_time;
	int			n_attrs;
	CapturedKV *attrs;
	int			n_events;
	CapturedEvent *events;
} CapturedSpan;

#define CAPTURE_RING_SIZE 32

static CapturedSpan ring[CAPTURE_RING_SIZE];
static int	ring_head;			/* next slot to write */
static int	ring_count;			/* number of valid entries */

static MemoryContext otel_test_cxt = NULL;

static otel_span_emit_hook_type prev_emit_hook = NULL;

/* ----- helpers ----- */

static char *
copy_str(const char *s)
{
	if (s == NULL)
		return NULL;
	return MemoryContextStrdup(otel_test_cxt, s);
}

static CapturedKV *
copy_kv_array(const OtelKeyValue *src, int n)
{
	CapturedKV *out;
	int			i;

	if (n <= 0 || src == NULL)
		return NULL;
	out = MemoryContextAllocZero(otel_test_cxt, sizeof(CapturedKV) * n);
	for (i = 0; i < n; i++)
	{
		out[i].key = copy_str(src[i].key);
		out[i].value = copy_str(src[i].value);
	}
	return out;
}

static void
clear_slot(CapturedSpan *slot)
{
	/* All allocations are in otel_test_cxt; we reset the whole
	 * context only on explicit clear.  Per-slot we just zero out
	 * the pointers so we don't dangle. */
	memset(slot, 0, sizeof(*slot));
}

static void
copy_span(const OtelSpan *span, CapturedSpan *slot)
{
	int			n_events;
	int			n_attrs;

	clear_slot(slot);

	memcpy(slot->trace_id, span->trace_id, sizeof(slot->trace_id));
	memcpy(slot->span_id, span->span_id, sizeof(slot->span_id));
	memcpy(slot->parent_span_id, span->parent_span_id,
		   sizeof(slot->parent_span_id));
	memcpy(slot->trace_flags, span->trace_flags, sizeof(slot->trace_flags));
	slot->tracestate = copy_str(span->tracestate);
	slot->name = copy_str(span->name);
	slot->kind = span->kind;
	slot->status = span->status;
	slot->status_description = copy_str(span->status_description);
	slot->start_time = span->start_time;
	slot->end_time = span->end_time;

	/* Flatten inline + overflow attribute arrays into one. */
	n_attrs = span->n_attrs + span->n_overflow_attrs;
	if (n_attrs > 0)
	{
		CapturedKV *out =
			MemoryContextAllocZero(otel_test_cxt,
								   sizeof(CapturedKV) * n_attrs);
		int			j = 0;
		int			i;

		for (i = 0; i < span->n_attrs; i++)
		{
			out[j].key = copy_str(span->attrs[i].key);
			out[j].value = copy_str(span->attrs[i].value);
			j++;
		}
		for (i = 0; i < span->n_overflow_attrs; i++)
		{
			out[j].key = copy_str(span->overflow_attrs[i].key);
			out[j].value = copy_str(span->overflow_attrs[i].value);
			j++;
		}
		slot->n_attrs = n_attrs;
		slot->attrs = out;
	}

	/* Flatten inline + overflow events. */
	n_events = (span->inline_event_used ? 1 : 0) + span->n_overflow_events;
	if (n_events > 0)
	{
		CapturedEvent *out =
			MemoryContextAllocZero(otel_test_cxt,
								   sizeof(CapturedEvent) * n_events);
		int			j = 0;
		int			i;

		if (span->inline_event_used)
		{
			out[j].core = span->inline_event.core;
			out[j].message = copy_str(span->inline_event.message);
			out[j].detail = copy_str(span->inline_event.detail);
			out[j].hint = copy_str(span->inline_event.hint);
			out[j].n_attrs = span->inline_event.n_attrs;
			out[j].attrs = copy_kv_array(span->inline_event.attrs,
										 span->inline_event.n_attrs);
			j++;
		}
		for (i = 0; i < span->n_overflow_events; i++)
		{
			const OtelSpanEvent *e = &span->overflow_events[i];

			out[j].core = e->core;
			out[j].message = copy_str(e->message);
			out[j].detail = copy_str(e->detail);
			out[j].hint = copy_str(e->hint);
			out[j].n_attrs = e->n_attrs;
			out[j].attrs = copy_kv_array(e->attrs, e->n_attrs);
			j++;
		}
		slot->n_events = n_events;
		slot->events = out;
	}
}

static void
otel_test_emit_hook(const OtelSpan *span)
{
	/* Allocations could fail under OOM --- per the contract we
	 * silently swallow rather than escalate. */
	PG_TRY();
	{
		copy_span(span, &ring[ring_head]);
		ring_head = (ring_head + 1) % CAPTURE_RING_SIZE;
		if (ring_count < CAPTURE_RING_SIZE)
			ring_count++;
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();

	if (prev_emit_hook)
		prev_emit_hook(span);
}

void		_PG_init(void);

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errmsg("test_otel_exporter must be loaded via shared_preload_libraries")));

	otel_test_cxt = AllocSetContextCreate(TopMemoryContext,
										  "test_otel_exporter",
										  ALLOCSET_DEFAULT_SIZES);

	prev_emit_hook = otel_span_emit_hook;
	otel_span_emit_hook = otel_test_emit_hook;
}

/* ----- SQL surface ----- */

/*
 * Number of spans currently held in the per-backend ring.
 */
PG_FUNCTION_INFO_V1(test_otel_span_count);
Datum
test_otel_span_count(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(ring_count);
}

/*
 * Pop the oldest captured span and return it as a single text blob.
 * Returns NULL if the ring is empty.  Format is a stable
 * key=value\n flat representation chosen for cheap regex assertion
 * in TAP --- not OTLP, not stable across postgres versions.
 *
 * Event entries are formatted on indented lines, attribute pairs
 * are listed once per attribute.
 */
PG_FUNCTION_INFO_V1(test_otel_pop_span);
Datum
test_otel_pop_span(PG_FUNCTION_ARGS)
{
	int			idx;
	CapturedSpan *s;
	StringInfoData buf;
	int			i;

	if (ring_count == 0)
		PG_RETURN_NULL();

	idx = (ring_head - ring_count + CAPTURE_RING_SIZE) % CAPTURE_RING_SIZE;
	s = &ring[idx];

	initStringInfo(&buf);
	appendStringInfo(&buf, "name=%s\n", s->name ? s->name : "");
	appendStringInfo(&buf, "kind=%d\n", (int) s->kind);
	appendStringInfo(&buf, "status=%d\n", (int) s->status);
	appendStringInfo(&buf, "trace_id=%s\n", s->trace_id);
	appendStringInfo(&buf, "span_id=%s\n", s->span_id);
	appendStringInfo(&buf, "parent_span_id=%s\n", s->parent_span_id);
	appendStringInfo(&buf, "trace_flags=%s\n", s->trace_flags);
	appendStringInfo(&buf, "tracestate=%s\n",
					 s->tracestate ? s->tracestate : "");
	appendStringInfo(&buf, "start_time=%" PRId64 "\n",
					 (int64) s->start_time);
	appendStringInfo(&buf, "end_time=%" PRId64 "\n",
					 (int64) s->end_time);
	if (s->status_description)
		appendStringInfo(&buf, "status_description=%s\n",
						 s->status_description);
	for (i = 0; i < s->n_attrs; i++)
		appendStringInfo(&buf, "attr=%s=%s\n",
						 s->attrs[i].key ? s->attrs[i].key : "",
						 s->attrs[i].value ? s->attrs[i].value : "");
	for (i = 0; i < s->n_events; i++)
	{
		const CapturedEvent *e = &s->events[i];
		int			j;

		appendStringInfo(&buf, "event.elevel=%d\n", e->core.elevel);
		appendStringInfo(&buf, "event.sqlstate=%s\n", e->core.sqlstate);
		appendStringInfo(&buf, "event.filename=%s\n",
						 e->core.filename ? e->core.filename : "");
		appendStringInfo(&buf, "event.lineno=%d\n", e->core.lineno);
		if (e->message)
			appendStringInfo(&buf, "event.message=%s\n", e->message);
		if (e->detail)
			appendStringInfo(&buf, "event.detail=%s\n", e->detail);
		if (e->hint)
			appendStringInfo(&buf, "event.hint=%s\n", e->hint);
		for (j = 0; j < e->n_attrs; j++)
			appendStringInfo(&buf, "event.attr=%s=%s\n",
							 e->attrs[j].key ? e->attrs[j].key : "",
							 e->attrs[j].value ? e->attrs[j].value : "");
	}

	/* Advance past this slot. */
	ring_count--;

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * Empty the ring without returning anything.
 */
PG_FUNCTION_INFO_V1(test_otel_clear);
Datum
test_otel_clear(PG_FUNCTION_ARGS)
{
	int			i;

	for (i = 0; i < CAPTURE_RING_SIZE; i++)
		clear_slot(&ring[i]);
	ring_head = 0;
	ring_count = 0;

	if (otel_test_cxt)
		MemoryContextReset(otel_test_cxt);

	PG_RETURN_VOID();
}
