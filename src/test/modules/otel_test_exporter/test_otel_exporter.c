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
#include "utils/guc.h"
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
	/* InstrumentationScope copied by value at capture time so the
	 * test fixture survives past producer teardown. */
	char	   *scope_name;
	char	   *scope_version;
	char	   *scope_schema_url;

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
static otel_sampler_hook_type prev_sampler_hook = NULL;

/*
 * Cached api pointer so SQL surface fns can call set_sampler_policy
 * without re-running find_rendezvous_variable each time.  Populated
 * in _PG_init.
 */
static const OtelTracingApi *cached_api = NULL;

/*
 * InstrumentationScope handle for spans this module produces via
 * the producer-API path (test_otel_producer_roundtrip).  Registered
 * once at _PG_init.
 */
static const OtelInstrumentationScope *test_tracer = NULL;

/*
 * GUC controlling what test_otel_sampler_hook returns.  Encoded as
 * int matching OtelSamplerDecision (0=DROP, 1=RECORD_ONLY,
 * 2=RECORD_AND_SAMPLE).  PGC_USERSET so TAP tests can flip it
 * mid-session without restart.
 *
 * Default is DROP so that under the default policy
 * (HOOK_ON_UNSAMPLED_BIT) a session without explicit override
 * behaves the same as if no sampler hook were registered at all
 * --- the existing 001_basic test predates the sampler hook and
 * still expects "no span when wire bit is unset."
 */
static int test_otel_sampler_decision = OTEL_SAMPLE_DROP;

static const struct config_enum_entry sampler_decision_options[] = {
	{"drop", OTEL_SAMPLE_DROP, false},
	{"record_only", OTEL_SAMPLE_RECORD_ONLY, false},
	{"record_and_sample", OTEL_SAMPLE_RECORD_AND_SAMPLE, false},
	{NULL, 0, false},
};

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

	if (span->scope)
	{
		slot->scope_name = copy_str(span->scope->name);
		slot->scope_version = copy_str(span->scope->version);
		slot->scope_schema_url = copy_str(span->scope->schema_url);
	}

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

/*
 * Sampler hook --- returns whatever the test_otel_exporter.sampler_decision
 * GUC currently holds.  TAP tests flip the GUC + the policy GUC (via
 * test_otel_set_policy) to walk the policy matrix.
 *
 * Note: this hook is the LAST-registered sampler; PREV_SAMPLER chaining
 * is not interesting for tests and is omitted.
 */
static OtelSamplerDecision
otel_test_sampler_hook(const OtelSamplerInput *in)
{
	(void) in;
	return (OtelSamplerDecision) test_otel_sampler_decision;
}

void		_PG_init(void);

void
_PG_init(void)
{
	void	  **slot;

	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errmsg("test_otel_exporter must be loaded via shared_preload_libraries")));

	/*
	 * Locate contrib/otel's API table via its rendezvous variable.
	 * If the slot is NULL the operator has misordered
	 * shared_preload_libraries: contrib/otel must come BEFORE this
	 * module so that its _PG_init has populated the slot.
	 */
	slot = find_rendezvous_variable(OTEL_TRACING_API_RENDEZVOUS_NAME);
	cached_api = (const OtelTracingApi *) *slot;
	if (cached_api == NULL)
		ereport(ERROR,
				(errmsg("test_otel_exporter requires contrib/otel to be loaded first"),
				 errhint("Add 'otel' before '%s' in shared_preload_libraries.",
						 "test_otel_exporter")));
	if (OTEL_API_MAJOR(cached_api->version) != OTEL_TRACING_API_MAJOR ||
		OTEL_API_MINOR(cached_api->version) < OTEL_TRACING_API_MINOR)
		ereport(ERROR,
				(errmsg("OtelTracingApi version mismatch"),
				 errdetail("Loaded contrib/otel exposes api version %u.%u; this module was built against version %u.%u.",
						   OTEL_API_MAJOR(cached_api->version),
						   OTEL_API_MINOR(cached_api->version),
						   OTEL_TRACING_API_MAJOR,
						   OTEL_TRACING_API_MINOR)));

	otel_test_cxt = AllocSetContextCreate(TopMemoryContext,
										  "test_otel_exporter",
										  ALLOCSET_DEFAULT_SIZES);

	/*
	 * GUC for sampler decision.  TAP tests SET this in-session to
	 * control what the sampler hook returns for each iteration of
	 * the policy matrix.
	 */
	DefineCustomEnumVariable("test_otel_exporter.sampler_decision",
							 "Decision returned by test_otel_exporter's sampler hook.",
							 NULL,
							 &test_otel_sampler_decision,
							 OTEL_SAMPLE_DROP,
							 sampler_decision_options,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	MarkGUCPrefixReserved("test_otel_exporter");

	cached_api->register_emit_hook(otel_test_emit_hook, &prev_emit_hook);
	cached_api->register_sampler_hook(otel_test_sampler_hook, &prev_sampler_hook);

	test_tracer = cached_api->tracer_register("test_otel_exporter", "1.0", NULL);
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
	appendStringInfo(&buf, "scope.name=%s\n",
					 s->scope_name ? s->scope_name : "");
	appendStringInfo(&buf, "scope.version=%s\n",
					 s->scope_version ? s->scope_version : "");
	appendStringInfo(&buf, "scope.schema_url=%s\n",
					 s->scope_schema_url ? s->scope_schema_url : "");
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

/*
 * Set the sampler-hook invocation policy via the v2 api.  Accepts
 * the same four string values the contrib/otel rust demo accepts
 * (hook_on_unsampled_bit, hook_always, never_respect_bit,
 * never_always_sample); easier to test from TAP than a numeric enum.
 *
 * Bumps contrib/otel's policy GUC-equivalent without us having to
 * expose the OtelSamplerHookPolicy enum to SQL.
 */
PG_FUNCTION_INFO_V1(test_otel_set_policy);
Datum
test_otel_set_policy(PG_FUNCTION_ARGS)
{
	text	   *t = PG_GETARG_TEXT_PP(0);
	const char *s = text_to_cstring(t);
	OtelSamplerHookPolicy policy;

	if (strcmp(s, "hook_on_unsampled_bit") == 0)
		policy = OTEL_SAMPLER_HOOK_ON_UNSAMPLED_BIT;
	else if (strcmp(s, "hook_always") == 0)
		policy = OTEL_SAMPLER_HOOK_ALWAYS;
	else if (strcmp(s, "never_respect_bit") == 0)
		policy = OTEL_SAMPLER_HOOK_NEVER_RESPECT_BIT;
	else if (strcmp(s, "never_always_sample") == 0)
		policy = OTEL_SAMPLER_HOOK_NEVER_ALWAYS_SAMPLE;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid sampler-hook policy: %s", s),
				 errhint("Valid: hook_on_unsampled_bit, hook_always, never_respect_bit, never_always_sample.")));

	cached_api->set_sampler_policy(policy);
	PG_RETURN_VOID();
}

/*
 * test_otel_producer_roundtrip(name text) → text
 *
 * Exercises the producer-side API end-to-end in a single SQL
 * call:  otel_span_init → set_unwind_policy → api->span_link_to
 * _active_and_push → otel_span_add_attribute_string ×2 →
 * otel_span_set_status → otel_span_finalize → api->span_emit.
 *
 * Returns the generated span_id so the TAP test can correlate it
 * with what the emit-hook captures.
 */
PG_FUNCTION_INFO_V1(test_otel_producer_roundtrip);
Datum
test_otel_producer_roundtrip(PG_FUNCTION_ARGS)
{
	text	   *name_arg = PG_GETARG_TEXT_PP(0);
	const char *name;
	OtelSpan	span;
	int			depth_before;
	int			depth_during;
	int			depth_after;
	const OtelSpanContext *ctx;

	/*
	 * Copy the name into long-lived storage so it stays valid through
	 * the producer-API call sequence.  text_to_cstring palloc's in
	 * CurrentMemoryContext --- fine for a single SQL call.
	 */
	name = text_to_cstring(name_arg);

	depth_before = cached_api->span_stack_depth();

	cached_api->span_init(&span, test_tracer, name, OTEL_SPAN_KIND_INTERNAL);
	otel_span_set_unwind_policy(&span, OTEL_UNWIND_DROP);

	cached_api->span_link_to_active_and_push(&span);

	depth_during = cached_api->span_stack_depth();
	if (depth_during != depth_before + 1)
		elog(ERROR, "producer roundtrip: stack depth did not increase (before=%d during=%d)",
			 depth_before, depth_during);

	/* Verify span_current_context returns this span. */
	ctx = cached_api->span_current_context();
	if (ctx == NULL || strcmp(ctx->span_id, span.span_id) != 0)
		elog(ERROR, "producer roundtrip: span_current_context did not return the pushed span (ctx=%s pushed=%s)",
			 ctx ? ctx->span_id : "(null)", span.span_id);

	cached_api->span_add_attribute_string(&span, "test.case", "roundtrip");
	cached_api->span_add_attribute_string(&span, "test.name", name);
	otel_span_set_status(&span, OTEL_STATUS_OK, NULL);
	otel_span_finalize(&span);

	cached_api->span_emit(&span);

	depth_after = cached_api->span_stack_depth();
	if (depth_after != depth_before)
		elog(ERROR, "producer roundtrip: stack depth did not return to baseline (before=%d after=%d)",
			 depth_before, depth_after);

	PG_RETURN_TEXT_P(cstring_to_text(span.span_id));
}

/*
 * test_otel_resource_attributes() → text
 *
 * Fetches the postmaster's Resource attribute array via the v2.1 API
 * and serialises it as "key1=val1;key2=val2;..." for the TAP test to
 * pattern-match.  Attribute order matches what otel_resource_init()
 * pushes.
 */
PG_FUNCTION_INFO_V1(test_otel_resource_attributes);
Datum
test_otel_resource_attributes(PG_FUNCTION_ARGS)
{
	const OtelResourceAttribute *attrs;
	int			n_attrs = 0;
	StringInfoData buf;

	attrs = cached_api->get_resource_attributes(&n_attrs);

	initStringInfo(&buf);
	for (int i = 0; i < n_attrs; i++)
	{
		if (i > 0)
			appendStringInfoChar(&buf, ';');
		appendStringInfo(&buf, "%s=%s", attrs[i].key, attrs[i].value);
	}

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}
