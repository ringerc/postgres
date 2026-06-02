/*-------------------------------------------------------------------------
 *
 * otel_metrics.c
 *	  Process-local OTel Counter implementation.
 *
 * The initial metrics implementation keeps all state in per-backend
 * memory: each backend has its own instrument table (built up
 * idempotently as producers call metric_instrument_register from
 * their _PG_init) and its own counter slots.  The hot path is a
 * single pg_atomic_fetch_add_u64 on a process-local cell --- no
 * shared-memory contention, no LWLocks.
 *
 * Cross-backend aggregation is deliberately not done here.  When
 * the metrics-collection bgworker lands (see
 * contrib-otel-metrics-plan.md), it will move the slot array into
 * shared memory and aggregate across MaxBackends.  Today, exporters
 * see per-backend snapshots via metric_collect_self; aggregation
 * across backends is the OTel collector's job after that.
 *
 * The OtelInstrument handle is just a pointer into the backend's
 * instrument table; producers cache it in module-static state and
 * pass it to metric_counter_add.  The lookup-by-name path used at
 * registration time is O(n_instruments) which is bounded by
 * OTEL_MAX_INSTRUMENTS and only runs once per process per
 * instrument, so the linear scan is fine.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/otel/otel_metrics.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "port/atomics.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include "otel.h"
#include "otel_internal.h"


/*
 * Compile-time bounds.
 *
 * OTEL_MAX_INSTRUMENTS:
 *     Total instruments that may be registered process-wide.
 *
 * OTEL_MAX_VALUES_PER_INSTRUMENT:
 *     For an instrument that declares an attribute, the maximum
 *     number of distinct *values* allowed for that attribute key
 *     (and therefore the maximum number of counter cells the
 *     instrument exposes; one cell per attribute value).
 *
 *     This is NOT a cap on the number of attribute *keys* --- the
 *     API supports exactly one attribute key per instrument by
 *     design (OtelInstrumentSpec carries a single attr_key); this
 *     cap is purely about the cardinality of values for that one
 *     key.  An instrument with no attribute uses one cell at
 *     slots[0]; an instrument with N declared values uses cells
 *     slots[0..N-1] where N <= OTEL_MAX_VALUES_PER_INSTRUMENT.
 *
 *     A consumer that needs higher cardinality should split the
 *     observation across multiple instruments rather than
 *     bumping this cap; multi-key attribute combinations belong
 *     at the OTel-collector aggregation tier, not in the
 *     bounded-storage per-backend slot array.
 *
 * Both are bumpable by recompiling contrib/otel.  They are not
 * GUCs because per-backend storage cost is fully determined at
 * compile time by their product (and by MaxBackends, once the
 * shared-memory storage planned in contrib-otel-metrics-plan.md
 * lands).
 */
#define OTEL_MAX_INSTRUMENTS			64
#define OTEL_MAX_VALUES_PER_INSTRUMENT	8

/*
 * Per-backend instrument table entry.  Strings are pstrdup'd into
 * TopMemoryContext at registration so they outlive any per-statement
 * memory context.
 *
 * The hot-path slot array is embedded directly in the entry, padded
 * to cacheline-distance from siblings: counter increments from
 * different instruments don't ping-pong cachelines.
 */
struct OtelInstrument
{
	OtelInstrumentKind kind;

	/* InstrumentationScope. */
	char	   *meter_name;
	char	   *meter_version;
	char	   *schema_url;

	/* Instrument identity. */
	char	   *instrument_name;
	char	   *description;
	char	   *unit;

	/* Attribute model.  Each instrument carries at most ONE
	 * attribute key (attr_key) with a closed set of allowed values
	 * (attr_values, n of them used).  The N-attribute-keys case is
	 * not supported in this minimal API; consumers that need it
	 * register N separate instruments, one per attribute key.
	 *
	 * (attr_key == NULL && n_attr_values == 0) means the instrument
	 * has no attribute at all --- one cell at slots[0], metric_counter_
	 * add must pass attr_value=NULL.
	 *
	 * n_attr_values is bounded by OTEL_MAX_VALUES_PER_INSTRUMENT; it
	 * is NOT a bound on the number of keys (which is always 1 by
	 * design). */
	char	   *attr_key;
	char	   *attr_values[OTEL_MAX_VALUES_PER_INSTRUMENT];
	int			n_attr_values;

	TimestampTz start_timestamp;

	/* Counter cells, one per attribute value (or slots[0] only when
	 * attr_key is NULL).  Cell count therefore equals
	 * max(1, n_attr_values), capped by
	 * OTEL_MAX_VALUES_PER_INSTRUMENT. */
	pg_atomic_uint64 slots[OTEL_MAX_VALUES_PER_INSTRUMENT];
};


/*
 * Process-local instrument table.  Entries are appended at
 * registration; never removed.  Pointers into this array are stable
 * for the backend's lifetime.
 */
static OtelInstrument instruments[OTEL_MAX_INSTRUMENTS];
static int	n_instruments = 0;

/*
 * Has otel_metrics_init() run?  Defensive guard: registration calls
 * before _PG_init has finished are a programming error.
 */
static bool metrics_initialised = false;


/* ---- name validation ---------------------------------------------- */

/*
 * Validate an OTel instrument / meter name against the spec syntax:
 * first character alphabetic, remaining characters alphanumeric or
 * one of "_./-", max 255 chars.  ereport(ERROR)s on violation; the
 * registration call site is _PG_init so this catches typos at module
 * load.
 */
static void
validate_name(const char *what, const char *s)
{
	size_t		len;
	size_t		i;

	if (s == NULL || s[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("otel: %s must be non-empty", what)));

	len = strlen(s);
	if (len > 255)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("otel: %s \"%s\" exceeds 255 characters", what, s)));

	if (!((s[0] >= 'A' && s[0] <= 'Z') || (s[0] >= 'a' && s[0] <= 'z')))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("otel: %s \"%s\" must start with a letter", what, s)));

	for (i = 1; i < len; i++)
	{
		char		c = s[i];

		if ((c >= 'A' && c <= 'Z') ||
			(c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') ||
			c == '_' || c == '.' || c == '/' || c == '-')
			continue;
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("otel: %s \"%s\" contains disallowed character at position %zu",
						what, s, i)));
	}
}


/* ---- registration -------------------------------------------------- */

/*
 * Look up an existing instrument by (meter_name, instrument_name).
 * Returns NULL if not found.  Linear scan; only runs at _PG_init
 * registration time.
 */
static OtelInstrument *
find_existing(const char *meter_name, const char *instrument_name)
{
	int			i;

	for (i = 0; i < n_instruments; i++)
	{
		OtelInstrument *inst = &instruments[i];

		if (strcmp(inst->meter_name, meter_name) == 0 &&
			strcmp(inst->instrument_name, instrument_name) == 0)
			return inst;
	}
	return NULL;
}

OtelInstrument *
otel_metric_instrument_register(const OtelInstrumentSpec *spec)
{
	OtelInstrument *inst;
	MemoryContext oldcxt;
	int			i;

	if (!metrics_initialised)
		ereport(ERROR,
				(errmsg("otel: metric_instrument_register called before _PG_init completed")));

	if (spec == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("otel: metric_instrument_register: spec must not be NULL")));

	if (spec->kind != OTEL_INSTRUMENT_COUNTER)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("otel: only Counter instruments are supported in this build")));

	validate_name("meter_name", spec->meter_name);
	validate_name("instrument_name", spec->instrument_name);

	if ((spec->attr_key == NULL) != (spec->n_attr_values == 0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("otel: instrument \"%s\": attr_key and n_attr_values must agree (both set or both unset)",
						spec->instrument_name)));

	if (spec->n_attr_values > OTEL_MAX_VALUES_PER_INSTRUMENT)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("otel: instrument \"%s\" declares %d attribute values for attribute key \"%s\"; cap is OTEL_MAX_VALUES_PER_INSTRUMENT = %d",
						spec->instrument_name,
						spec->n_attr_values,
						spec->attr_key ? spec->attr_key : "(none)",
						OTEL_MAX_VALUES_PER_INSTRUMENT),
				 errhint("Reduce the value cardinality, split into multiple instruments, or increase OTEL_MAX_VALUES_PER_INSTRUMENT and rebuild contrib/otel.")));

	/* Idempotent: same (meter, instrument) -> same handle.  Repeated
	 * description / unit / attribute declarations are ignored. */
	inst = find_existing(spec->meter_name, spec->instrument_name);
	if (inst != NULL)
		return inst;

	if (n_instruments >= OTEL_MAX_INSTRUMENTS)
	{
		ereport(WARNING,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("otel: instrument table full (cap %d); not registering \"%s/%s\"",
						OTEL_MAX_INSTRUMENTS,
						spec->meter_name, spec->instrument_name),
				 errhint("Increase OTEL_MAX_INSTRUMENTS and rebuild contrib/otel.")));
		return NULL;
	}

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	inst = &instruments[n_instruments];
	memset(inst, 0, sizeof(*inst));
	inst->kind = spec->kind;
	inst->meter_name = pstrdup(spec->meter_name);
	inst->meter_version = spec->meter_version ? pstrdup(spec->meter_version) : NULL;
	inst->schema_url = spec->schema_url ? pstrdup(spec->schema_url) : NULL;
	inst->instrument_name = pstrdup(spec->instrument_name);
	inst->description = spec->description ? pstrdup(spec->description) : NULL;
	inst->unit = spec->unit ? pstrdup(spec->unit) : NULL;
	inst->start_timestamp = GetCurrentTimestamp();

	if (spec->attr_key != NULL)
	{
		inst->attr_key = pstrdup(spec->attr_key);
		for (i = 0; i < spec->n_attr_values; i++)
			inst->attr_values[i] = pstrdup(spec->attr_values[i]);
		inst->n_attr_values = spec->n_attr_values;
	}
	else
	{
		/* No-attribute instrument: single slot at index 0. */
		inst->attr_key = NULL;
		inst->n_attr_values = 0;
	}

	for (i = 0; i < OTEL_MAX_VALUES_PER_INSTRUMENT; i++)
		pg_atomic_init_u64(&inst->slots[i], 0);

	n_instruments++;

	MemoryContextSwitchTo(oldcxt);

	return inst;
}


/* ---- recording ----------------------------------------------------- */

/*
 * Linear-scan attr_value -> slot index.  n_attr_values is small (cap 8)
 * so this is faster than a hashtable.  Returns -1 if no match.
 */
static int
find_attrset(const OtelInstrument *inst, const char *attr_value)
{
	int			i;

	for (i = 0; i < inst->n_attr_values; i++)
	{
		if (strcmp(inst->attr_values[i], attr_value) == 0)
			return i;
	}
	return -1;
}

void
otel_metric_counter_add(OtelInstrument *inst, uint64 value, const char *attr_value)
{
	int			slot_idx;

	/* NULL inst is a documented no-op so callers don't need to guard
	 * when registration returned NULL (capacity exhaustion). */
	if (inst == NULL || value == 0)
		return;

	if (inst->attr_key == NULL)
	{
		/* No-attribute instrument: attr_value MUST be NULL. */
		if (attr_value != NULL)
			return;					/* silent drop --- caller programming error */
		slot_idx = 0;
	}
	else
	{
		if (attr_value == NULL)
			return;					/* caller passed NULL but instrument has attribute */
		slot_idx = find_attrset(inst, attr_value);
		if (slot_idx < 0)
			return;					/* unknown value --- silent drop */
	}

	pg_atomic_fetch_add_u64(&inst->slots[slot_idx], value);
}


/* ---- collection ---------------------------------------------------- */

/*
 * Walk this backend's instrument table and invoke `visitor` once per
 * (instrument, attribute-set) cell.  Skips zero-valued cells: an
 * exporter cares about cells that have actually been touched, and
 * the cumulative-temporality convention says you don't emit
 * datapoints for series with no observation.
 *
 * Snapshots are built on the stack inside this function; their
 * pointer fields borrow from the instrument table and are valid
 * only for the duration of the visitor call.  Visitors that defer
 * must copy.
 */
void
otel_metric_collect_self(otel_metric_visitor visitor, void *ctx)
{
	TimestampTz now;
	int			i;
	int			j;

	if (visitor == NULL)
		return;

	now = GetCurrentTimestamp();

	for (i = 0; i < n_instruments; i++)
	{
		OtelInstrument *inst = &instruments[i];
		int			n_cells = (inst->attr_key == NULL) ? 1 : inst->n_attr_values;

		for (j = 0; j < n_cells; j++)
		{
			OtelMetricSnapshot snap;
			uint64		value = pg_atomic_read_u64(&inst->slots[j]);

			if (value == 0)
				continue;

			snap.meter_name = inst->meter_name;
			snap.meter_version = inst->meter_version;
			snap.schema_url = inst->schema_url;
			snap.instrument_name = inst->instrument_name;
			snap.description = inst->description;
			snap.unit = inst->unit;
			snap.kind = inst->kind;
			snap.temporality = OTEL_AGGREGATION_TEMPORALITY_CUMULATIVE;
			snap.attr_key = inst->attr_key;
			snap.attr_value = (inst->attr_key == NULL) ? NULL : inst->attr_values[j];
			snap.start_timestamp = inst->start_timestamp;
			snap.collection_time = now;
			snap.value = value;

			visitor(&snap, ctx);
		}
	}
}


/* ---- dispatch ------------------------------------------------------ */

/*
 * Registered emit-hook chain.  Single slot; chained exporters
 * follow the same prev-hook convention as the span-emit chain.
 */
static otel_metrics_emit_hook_type otel_metrics_emit_hook = NULL;

void
otel_register_metrics_emit_hook(otel_metrics_emit_hook_type new_hook,
								otel_metrics_emit_hook_type *prev_out)
{
	if (prev_out)
		*prev_out = otel_metrics_emit_hook;
	otel_metrics_emit_hook = new_hook;
}

/*
 * Visitor used internally by dispatch_metrics_now to gather snapshots
 * into a single OtelMetricBatch.  The snapshot pointers borrow from
 * the instrument table; we copy by value into a palloc'd array, with
 * pointer fields preserved since they reference TopMemoryContext
 * strings that outlive any per-dispatch context.
 */
typedef struct
{
	OtelMetricSnapshot *snapshots;
	int			n;
	int			capacity;
	MemoryContext cxt;
} DispatchCollect;

static void
dispatch_collect_visitor(const OtelMetricSnapshot *snap, void *vctx)
{
	DispatchCollect *dc = (DispatchCollect *) vctx;

	if (dc->n >= dc->capacity)
	{
		int			newcap = dc->capacity ? dc->capacity * 2 : 16;

		if (dc->snapshots == NULL)
			dc->snapshots = (OtelMetricSnapshot *)
				palloc(sizeof(OtelMetricSnapshot) * newcap);
		else
			dc->snapshots = (OtelMetricSnapshot *)
				repalloc(dc->snapshots, sizeof(OtelMetricSnapshot) * newcap);
		dc->capacity = newcap;
	}
	dc->snapshots[dc->n++] = *snap;
}

void
otel_dispatch_metrics_now(void)
{
	OtelMetricBatch batch;
	DispatchCollect dc;
	int			n_res;
	const OtelResourceAttribute *res;
	MemoryContext oldcxt;
	MemoryContext dispatch_cxt;

	if (otel_metrics_emit_hook == NULL)
		return;					/* nothing to do --- no consumer */

	/* All allocations from here on go into a short-lived dispatch
	 * context so emit hooks don't see leftovers across calls. */
	dispatch_cxt = AllocSetContextCreate(CurrentMemoryContext,
										 "otel metrics dispatch",
										 ALLOCSET_SMALL_SIZES);
	oldcxt = MemoryContextSwitchTo(dispatch_cxt);

	dc.snapshots = NULL;
	dc.n = 0;
	dc.capacity = 0;
	dc.cxt = dispatch_cxt;

	otel_metric_collect_self(dispatch_collect_visitor, &dc);

	res = otel_resource_attrs_get(&n_res);

	batch.resource_attrs = res;
	batch.n_resource_attrs = n_res;
	batch.snapshots = dc.snapshots;
	batch.n_snapshots = dc.n;
	batch.collection_time = (dc.n > 0) ? dc.snapshots[0].collection_time
									   : GetCurrentTimestamp();

	otel_metrics_emit_hook(&batch);

	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(dispatch_cxt);
}


/* ---- init ---------------------------------------------------------- */

void
otel_metrics_init(void)
{
	n_instruments = 0;
	otel_metrics_emit_hook = NULL;
	metrics_initialised = true;
}
