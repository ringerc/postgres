/*-------------------------------------------------------------------------
 *
 * test_otel_exporter.c
 *	  Tiny test-only exporter for otel_api.
 *
 * Registers a callback against otel_api's exporter API; captures
 * completed spans into a fixed-size per-backend ring buffer; exposes
 * SQL functions that TAP tests use to read out the captured spans and
 * assert on their contents.
 *
 * Captures spans by deep-copying everything we care about into
 * private storage at hook time, so the test SQL can fetch them later
 * (even after the originating transaction has ended).  Binary trace
 * and span IDs are copied by value (fixed-size structs); hex is
 * produced only when formatting the flat text dump for TAP.
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/modules/otel_test_exporter/test_otel_exporter.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include <otel_api/otel.h>

PG_MODULE_MAGIC;

/*
 * Captured span.  String fields are deep-copied into otel_test_cxt at
 * capture time so they survive past the originating transaction.
 * Attribute values are captured as strings (formatted at capture
 * time from the typed OtelAttribute) since the flat text dump this
 * module produces is untyped.
 */
typedef struct CapturedKV
{
	char	   *key;
	char	   *value;
} CapturedKV;

typedef struct CapturedEvent
{
	char	   *name;			/* required; "exception" for ereport-derived */
	TimestampTz time;
	int			n_attrs;
	CapturedKV *attrs;
} CapturedEvent;

typedef struct CapturedSpan
{
	/* InstrumentationScope copied by value at capture time so the
	 * test fixture survives past producer teardown. */
	char	   *scope_name;
	char	   *scope_version;
	char	   *scope_schema_url;

	OtelTraceId trace_id;
	OtelSpanId	span_id;
	OtelSpanId	parent_span_id;
	uint8		trace_flags;
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

/*
 * test_otel_exporter.capture --- when off, the emit hook does nothing
 * but bump otel_test_uncaptured_spans and return.  Used by the overhead
 * benchmark (postgres-cdq.24) as a cheap "null exporter": any registered
 * exporter makes otel_api's recording_possible true, but an exporter
 * that doesn't deep-copy avoids measuring the ring-buffer copy cost
 * alongside otel_api's own per-span cost.
 */
static bool otel_test_capture = true;
static int64 otel_test_uncaptured_spans = 0;

/*
 * This module's own tracer, for the producer-API roundtrip test
 * (test_otel_producer_roundtrip).  otel_api fills in ->scope on first
 * use; no explicit registration call is needed.
 */
static OtelTracer test_tracer = {.name = "test_otel_exporter", .version = "1.0"};

/* ----- helpers ----- */

static char *
copy_str(const char *s)
{
	if (s == NULL)
		return NULL;
	return MemoryContextStrdup(otel_test_cxt, s);
}

/*
 * Format a typed OtelAttribute's value as text, for this module's flat
 * text dump.  Copied (like every other captured string) into
 * otel_test_cxt.
 */
static char *
format_attr_value(const OtelAttribute *a)
{
	char		buf[64];

	switch (a->type)
	{
		case OTEL_ATTR_STRING:
			return copy_str(a->v.s);
		case OTEL_ATTR_INT:
			snprintf(buf, sizeof(buf), INT64_FORMAT, a->v.i);
			return copy_str(buf);
		case OTEL_ATTR_DOUBLE:
			snprintf(buf, sizeof(buf), "%g", a->v.d);
			return copy_str(buf);
		case OTEL_ATTR_BOOL:
			return copy_str(a->v.b ? "true" : "false");
	}
	return NULL;
}

static CapturedKV *
copy_attr_array(const OtelAttribute *src, int n)
{
	CapturedKV *out;
	int			i;

	if (n <= 0 || src == NULL)
		return NULL;
	out = MemoryContextAllocZero(otel_test_cxt, sizeof(CapturedKV) * n);
	for (i = 0; i < n; i++)
	{
		out[i].key = copy_str(src[i].key);
		out[i].value = format_attr_value(&src[i]);
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
	clear_slot(slot);

	if (span->scope)
	{
		slot->scope_name = copy_str(span->scope->name);
		slot->scope_version = copy_str(span->scope->version);
		slot->scope_schema_url = copy_str(span->scope->schema_url);
	}

	slot->trace_id = span->trace_id;
	slot->span_id = span->span_id;
	slot->parent_span_id = span->parent_span_id;
	slot->trace_flags = span->trace_flags;
	slot->tracestate = copy_str(span->tracestate);
	slot->name = copy_str(span->name);
	slot->kind = span->kind;
	slot->status = span->status;
	slot->status_description = copy_str(span->status_description);
	slot->start_time = span->start_time;
	slot->end_time = span->end_time;

	if (span->n_attrs > 0)
	{
		slot->n_attrs = span->n_attrs;
		slot->attrs = copy_attr_array(span->attrs, span->n_attrs);
	}

	/*
	 * Copy the generic event list.  otel_api has already lowered any
	 * captured error into an "exception" event in span->events before
	 * the emit hook fires, so this path is fully generic: name, time,
	 * and attrs.  ereport fields (sqlstate, message, elevel, code.*,
	 * detail, hint) arrive as ordinary event attributes.
	 */
	if (span->n_events > 0)
	{
		CapturedEvent *out =
			MemoryContextAllocZero(otel_test_cxt,
								   sizeof(CapturedEvent) * span->n_events);
		int			i;

		for (i = 0; i < span->n_events; i++)
		{
			const OtelSpanEvent *e = &span->events[i];

			out[i].name = copy_str(e->name);
			out[i].time = e->time;
			out[i].n_attrs = e->n_attrs;
			out[i].attrs = copy_attr_array(e->attrs, e->n_attrs);
		}
		slot->n_events = span->n_events;
		slot->events = out;
	}
}

static void
otel_test_emit_hook(const OtelSpan *span)
{
	if (!otel_exporter_span_ok(span))
		return;

	if (!otel_test_capture)
	{
		otel_test_uncaptured_spans++;
		if (prev_emit_hook)
			prev_emit_hook(span);
		return;
	}

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

/*
 * Pending-registration node for the two-phase deferred registration
 * path (when this module loads before otel_api).  Allocated in the
 * global BSS; filled and registered at _PG_init.  The provider drains
 * it when it publishes its slot.
 */
static OtelPendingRegistration pending_reg;

void
_PG_init(void)
{
	/*
	 * No load-time guard.  All preload mechanisms (shared, session,
	 * local) and a direct LOAD are accepted; the provider is resolved
	 * via otel_exporter_register_when_ready() below.
	 */

	otel_test_cxt = AllocSetContextCreate(TopMemoryContext,
										  "test_otel_exporter",
										  ALLOCSET_DEFAULT_SIZES);

	DefineCustomBoolVariable("test_otel_exporter.capture",
							 "Deep-copy captured spans into the test ring buffer.",
							 "Off makes this a null exporter: the emit hook only counts "
							 "spans (see test_otel_uncaptured_span_count()).  For the "
							 "otel_api overhead benchmark, which needs a registered "
							 "exporter (to make recording_possible true) that doesn't "
							 "itself add per-span copying cost.",
							 &otel_test_capture,
							 true,
							 PGC_SUSET,
							 0,
							 NULL, NULL, NULL);

	/*
	 * Two-phase deferred registration.  If otel_api is already present
	 * (provider loads first), register immediately.  Otherwise the
	 * request is queued and the provider drains it when it publishes.
	 */
	memset(&pending_reg, 0, sizeof(pending_reg));
	pending_reg.emit_hook = otel_test_emit_hook;
	pending_reg.emit_prev_out = &prev_emit_hook;
	otel_exporter_register_when_ready(&pending_reg);
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
 * Number of spans the emit hook saw while test_otel_exporter.capture was
 * off (counted, not copied).  See otel_test_capture above.
 */
PG_FUNCTION_INFO_V1(test_otel_uncaptured_span_count);
Datum
test_otel_uncaptured_span_count(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(otel_test_uncaptured_spans);
}

/*
 * Format a captured span into a stable key=value\n text blob used by
 * pop_span and pop_span_by_name.  Caller must have called initStringInfo
 * before passing buf.  IDs are formatted as lowercase hex here (the
 * "edge" where binary IDs become text); everything else is unchanged.
 */
static void
format_span(const CapturedSpan *s, StringInfoData *buf)
{
	int			i;
	char		trace_id_hex[OTEL_TRACE_ID_HEX_LEN + 1];
	char		span_id_hex[OTEL_SPAN_ID_HEX_LEN + 1];
	char		parent_span_id_hex[OTEL_SPAN_ID_HEX_LEN + 1];
	char		trace_flags_hex[3];

	otel_trace_id_to_hex(&s->trace_id, trace_id_hex);
	otel_span_id_to_hex(&s->span_id, span_id_hex);
	otel_span_id_to_hex(&s->parent_span_id, parent_span_id_hex);
	otel_bytes_to_hex(&s->trace_flags, 1, trace_flags_hex);

	appendStringInfo(buf, "scope.name=%s\n",
					 s->scope_name ? s->scope_name : "");
	appendStringInfo(buf, "scope.version=%s\n",
					 s->scope_version ? s->scope_version : "");
	appendStringInfo(buf, "scope.schema_url=%s\n",
					 s->scope_schema_url ? s->scope_schema_url : "");
	appendStringInfo(buf, "name=%s\n", s->name ? s->name : "");
	appendStringInfo(buf, "kind=%d\n", (int) s->kind);
	appendStringInfo(buf, "status=%d\n", (int) s->status);
	appendStringInfo(buf, "trace_id=%s\n", trace_id_hex);
	appendStringInfo(buf, "span_id=%s\n", span_id_hex);
	appendStringInfo(buf, "parent_span_id=%s\n",
					 otel_span_id_is_valid(&s->parent_span_id) ? parent_span_id_hex : "");
	appendStringInfo(buf, "trace_flags=%s\n", trace_flags_hex);
	appendStringInfo(buf, "tracestate=%s\n",
					 s->tracestate ? s->tracestate : "");
	appendStringInfo(buf, "start_time=%" PRId64 "\n",
					 (int64) s->start_time);
	appendStringInfo(buf, "end_time=%" PRId64 "\n",
					 (int64) s->end_time);
	if (s->status_description)
		appendStringInfo(buf, "status_description=%s\n",
						 s->status_description);
	for (i = 0; i < s->n_attrs; i++)
		appendStringInfo(buf, "attr=%s=%s\n",
						 s->attrs[i].key ? s->attrs[i].key : "",
						 s->attrs[i].value ? s->attrs[i].value : "");
	for (i = 0; i < s->n_events; i++)
	{
		const CapturedEvent *e = &s->events[i];
		int			j;

		/*
		 * Generic per-event dump: a name line, a time line, then one
		 * line per attribute.  ereport-derived events surface as
		 * event.name=exception with their sqlstate / message / elevel /
		 * code.* / detail / hint carried under event.attr= keys.
		 */
		appendStringInfo(buf, "event.name=%s\n", e->name ? e->name : "");
		appendStringInfo(buf, "event.time=%" PRId64 "\n", (int64) e->time);
		for (j = 0; j < e->n_attrs; j++)
			appendStringInfo(buf, "event.attr=%s=%s\n",
							 e->attrs[j].key ? e->attrs[j].key : "",
							 e->attrs[j].value ? e->attrs[j].value : "");
	}
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

	if (ring_count == 0)
		PG_RETURN_NULL();

	idx = (ring_head - ring_count + CAPTURE_RING_SIZE) % CAPTURE_RING_SIZE;
	s = &ring[idx];

	initStringInfo(&buf);
	format_span(s, &buf);

	/* Advance past this slot. */
	ring_count--;

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * Pop the oldest captured span whose name exactly matches the argument.
 * Scans the ring in FIFO order (oldest first), removes the first matching
 * entry (shifting later entries one position toward the head to close the
 * gap), and returns the span text in the same format as test_otel_pop_span.
 * Returns NULL if no match is found.
 */
PG_FUNCTION_INFO_V1(test_otel_pop_span_by_name);
Datum
test_otel_pop_span_by_name(PG_FUNCTION_ARGS)
{
	text	   *name_arg = PG_GETARG_TEXT_PP(0);
	const char *name_to_find = text_to_cstring(name_arg);
	int			match_lpos = -1;
	int			i;
	StringInfoData buf;

	/* Find the oldest span matching the requested name. */
	for (i = 0; i < ring_count; i++)
	{
		int			phys = (ring_head - ring_count + i + CAPTURE_RING_SIZE)
			% CAPTURE_RING_SIZE;

		if (ring[phys].name && strcmp(ring[phys].name, name_to_find) == 0)
		{
			match_lpos = i;
			break;
		}
	}

	if (match_lpos < 0)
		PG_RETURN_NULL();

	/* Format the matched span before we overwrite its slot. */
	{
		int			phys = (ring_head - ring_count + match_lpos + CAPTURE_RING_SIZE)
			% CAPTURE_RING_SIZE;

		initStringInfo(&buf);
		format_span(&ring[phys], &buf);
	}

	/*
	 * Close the gap: shift all later entries one logical position toward
	 * the oldest end, then shrink ring_head by one.  This preserves the
	 * circular layout and the ring_head / ring_count invariants.
	 */
	for (i = match_lpos; i < ring_count - 1; i++)
	{
		int			src = (ring_head - ring_count + i + 1 + CAPTURE_RING_SIZE)
			% CAPTURE_RING_SIZE;
		int			dst = (ring_head - ring_count + i + CAPTURE_RING_SIZE)
			% CAPTURE_RING_SIZE;

		ring[dst] = ring[src];	/* shallow copy; all strings in otel_test_cxt */
	}
	ring_head = (ring_head - 1 + CAPTURE_RING_SIZE) % CAPTURE_RING_SIZE;
	ring_count--;

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * Count spans in the ring whose name exactly matches the argument.
 * Does not remove any entries.
 */
PG_FUNCTION_INFO_V1(test_otel_count_spans_by_name);
Datum
test_otel_count_spans_by_name(PG_FUNCTION_ARGS)
{
	text	   *name_arg = PG_GETARG_TEXT_PP(0);
	const char *name_to_find = text_to_cstring(name_arg);
	int			count = 0;
	int			i;

	for (i = 0; i < ring_count; i++)
	{
		int			phys = (ring_head - ring_count + i + CAPTURE_RING_SIZE)
			% CAPTURE_RING_SIZE;

		if (ring[phys].name && strcmp(ring[phys].name, name_to_find) == 0)
			count++;
	}

	PG_RETURN_INT32(count);
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

/*
 * test_otel_producer_roundtrip(name text) → text
 *
 * Exercises the producer-side API end-to-end in a single SQL call:
 * otel_span_start → otel_span_set_str ×2 → otel_span_add_event →
 * otel_span_set_status → otel_span_end.
 *
 * Returns the generated span_id (hex) so the TAP test can correlate
 * it with what the emit-hook captures.
 */
PG_FUNCTION_INFO_V1(test_otel_producer_roundtrip);
Datum
test_otel_producer_roundtrip(PG_FUNCTION_ARGS)
{
	text	   *name_arg = PG_GETARG_TEXT_PP(0);
	const char *name;
	OtelSpanRef s;
	OtelSpanRef before;
	OtelSpanRef during;
	OtelSpanContext ctx;
	OtelSpanContext my_ctx;
	char		span_id_hex[OTEL_SPAN_ID_HEX_LEN + 1];

	/*
	 * Copy the name into long-lived storage so it stays valid through
	 * the span-start call.  text_to_cstring palloc's in
	 * CurrentMemoryContext --- fine for a single SQL call.
	 */
	name = text_to_cstring(name_arg);

	before = otel_span_current();

	s = otel_span_start(.tracer = &test_tracer,
					   .name = name,
					   .kind = OTEL_SPAN_KIND_INTERNAL);
	if (s.v == 0)
		ereport(ERROR,
				(errmsg("test_otel_producer_roundtrip: otel_span_start returned OTEL_SPAN_NONE "
						"(otel_api absent, or nothing can currently record)")));

	/* Verify the span is now on top of the active stack. */
	during = otel_span_current();
	if (during.v != s.v)
		elog(ERROR, "producer roundtrip: span_current() did not return the started span "
			 "(started=" INT64_FORMAT " current=" INT64_FORMAT ")",
			 s.v, during.v);

	/* Verify otel_span_context_of(OTEL_SPAN_NONE, ...) agrees. */
	if (!otel_span_context_of(OTEL_SPAN_NONE, &ctx) ||
		!otel_span_context_of(s, &my_ctx) ||
		!otel_span_id_equal(&ctx.span_id, &my_ctx.span_id))
		elog(ERROR, "producer roundtrip: active-stack context did not match the started span");

	otel_span_set_str(s, "test.case", "roundtrip");
	otel_span_set_str(s, "test.name", name);

	/*
	 * Exercise the generic event API: attach a named event with two
	 * attributes so the TAP test can assert it round-trips (name +
	 * attrs) through the log dump.  Values are copied by otel_api, so
	 * a transient on-stack array is fine.  ts=0 => "now".
	 */
	{
		OtelAttribute evattrs[2] = {
			OTEL_ATTR_STR("event.kind", "generic"),
			OTEL_ATTR_STR("event.seq", "1"),
		};

		otel_span_add_event(s, "test.event", 0, evattrs, 2);
	}

	otel_span_set_status(s, OTEL_STATUS_OK, NULL);
	otel_span_id_to_hex(&my_ctx.span_id, span_id_hex);

	otel_span_end(s);

	/* Verify the active stack returned to baseline. */
	if (otel_span_current().v != before.v)
		elog(ERROR, "producer roundtrip: active stack did not return to baseline");

	PG_RETURN_TEXT_P(cstring_to_text(span_id_hex));
}

/*
 * test_otel_resource_attributes() → text
 *
 * Fetches the postmaster's Resource attribute array and serialises it
 * as "key1=val1;key2=val2;..." for the TAP test to pattern-match.
 * Attribute order matches what otel_api's resource init pushes.
 */
PG_FUNCTION_INFO_V1(test_otel_resource_attributes);
Datum
test_otel_resource_attributes(PG_FUNCTION_ARGS)
{
	const OtelResourceAttribute *attrs;
	int			n_attrs = 0;
	StringInfoData buf;
	const OtelExporterApi *api;

	api = otel_exporter_api();
	if (api == NULL)
		ereport(ERROR,
				(errmsg("test_otel_resource_attributes: otel_api provider is not available")));

	attrs = api->get_resource_attributes(&n_attrs);

	initStringInfo(&buf);
	for (int i = 0; i < n_attrs; i++)
	{
		if (i > 0)
			appendStringInfoChar(&buf, ';');
		appendStringInfo(&buf, "%s=%s", attrs[i].key, attrs[i].value);
	}

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}
