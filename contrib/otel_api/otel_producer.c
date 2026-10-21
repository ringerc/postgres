/*-------------------------------------------------------------------------
 *
 * otel_producer.c
 *	  Span storage, the active span stack, and the producer API.
 *
 * Storage
 * -------
 * otel_api owns every span.  A recording span lives in a slot of a
 * per-backend array of otel_api.max_open_spans slots, allocated on the
 * first recorded span and never resized.  Slots are handed out from a
 * LIFO free list, and slots never used are never written, so a backend
 * only touches as many slots as it has spans open at its peak.  Each slot
 * has its own small memory context for the strings, attributes, events
 * and links copied into the span; ending the span resets it.
 *
 * An unsampled span takes no slot.  It is a "non-recording entry": a
 * trace context and a subtransaction level in a separate fixed array.
 * It exists so that its children are unsampled too (without calling the
 * sampler) and so that its context still propagates, with sampled=0.
 *
 * Handles
 * -------
 * OtelSpanRef.v is (generation << 32 | index): positive for a slot,
 * negated for a non-recording entry.  Generations are 31-bit, never 0,
 * and come from a per-backend counter that starts at a random value, so
 * a handle from another backend is very unlikely to match.  A handle
 * whose generation doesn't match is stale: a no-op, counted, and an
 * Assert failure in cassert builds.  The exception is a handle whose
 * span was ended by its resource owner (abort), which callers can't
 * always know about: using that is a quiet no-op.
 *
 * Lifetime
 * --------
 * Every recording span is remembered by a resource owner (by default
 * CurrentResourceOwner), or is a session span.  Owner release on abort
 * exports the span with ERROR status.  Owner release on commit with
 * the span still open is a leak: core calls the DebugPrint callback just
 * before ReleaseResource, only in that case, which is how the two are
 * told apart.  Non-recording entries are dropped by subtransaction and
 * transaction end callbacks, by subtransaction level.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  otel_api/otel_producer.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/parallel.h"
#include "access/xact.h"
#include "common/pg_prng.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/injection_point.h"
#include "utils/json.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"

#include "otel_internal.h"

/* Maximum active-stack depth, and number of non-recording entries. */
#define OTEL_MAX_STACK_DEPTH	128

/* Attributes stored in the slot before spilling to its context. */
#define OTEL_SLOT_INLINE_ATTRS	12

#define OTEL_GEN_MASK			0x7fffffffu

/* A captured error, lowered into an "exception" event at end. */
typedef struct OtelSlotError
{
	bool		used;
	int			elevel;
	int			lineno;
	char		sqlstate[6];
	TimestampTz time;
	const char *message;		/* all copied; any may be NULL */
	const char *detail;
	const char *hint;
	const char *filename;
	const char *funcname;
} OtelSlotError;

typedef struct OtelSlot
{
	OtelSpan	span;			/* the exporter's view */

	uint32		gen;			/* 0 = free */
	uint32		owner_released_gen; /* last generation ended by its owner */
	int			next_free;

	bool		detached;
	bool		session;
	bool		leaked;			/* set by DebugPrint on commit release */
	bool		dispatching;
	ResourceOwner owner;		/* NULL for a session span */
	void	   *scope_frame;	/* cassert: an address in the start frame */

	MemoryContext cxt;
	Size		bytes;			/* copied into cxt for this span */

	OtelAttribute inline_attrs[OTEL_SLOT_INLINE_ATTRS];
	OtelAttribute *attrs;
	int			attrs_cap;
	OtelSpanEvent *events;
	int			events_cap;
	OtelSpanContext *links;
	int			links_cap;

	OtelSlotError err;
} OtelSlot;

/* A non-recording (unsampled) span. */
typedef struct OtelNrec
{
	uint32		gen;			/* 0 = free */
	uint32		owner_released_gen; /* last generation dropped at xact end */
	int			next_free;
	int			nest_level;		/* subtransaction level at start */
	bool		detached;
	bool		have_span_id;	/* span_id is generated lazily */
	OtelSpanContext ctx;		/* tracestate always NULL */
} OtelNrec;

/*
 * The active stack.  Entry >= 0 is a slot index; < 0 is -(nrec index) - 1.
 *
 * span_stack_meta runs parallel to span_stack: an entry pushed by
 * otel_span_activate() (rather than otel_span_start()) is marked
 * is_activation, with the subtransaction nest level current at
 * activation time.  That is what tells apart a span's own stack entry
 * (ended, not merely popped, by otel_span_end()/abort/unwind) from an
 * activation of a .detached span the caller still owns elsewhere
 * (popped only -- never ended -- by otel_span_deactivate(), by a
 * (sub)transaction ending at or above that level, or by a LIFO unwind
 * passing over it).
 */
typedef struct StackEntryMeta
{
	bool		is_activation;
	int			nest_level;		/* subxact level when activated */
} StackEntryMeta;

static int32 span_stack[OTEL_MAX_STACK_DEPTH];
static StackEntryMeta span_stack_meta[OTEL_MAX_STACK_DEPTH];
static int	span_stack_depth = 0;

static OtelSlot *slots = NULL;
static int	nslots = 0;			/* otel_max_open_spans, fixed at first use */
static int	slots_used = 0;		/* slots below this have been initialised */
static int	slot_free_head = -1;
static int	session_spans_open = 0;
static int	dispatch_depth = 0;	/* > 0 while an emit hook runs */
static MemoryContext span_pool_cxt = NULL;

static OtelNrec nrecs[OTEL_MAX_STACK_DEPTH];
static int	nrecs_used = 0;
static int	nrec_free_head = -1;

static uint32 gen_counter = 0;
static pg_prng_state otel_prng;
static int	otel_prng_pid = 0;	/* pid that seeded otel_prng */

OtelApiCounters otel_counters;

static bool non_lifo_warned = false;
static bool exit_callback_registered = false;

static emit_log_hook_type prev_emit_log_hook = NULL;

static void otel_span_release_resource(Datum res);
static char *otel_span_debug_print(Datum res);

static const ResourceOwnerDesc otel_span_resowner_desc = {
	.name = "otel_api span",
	.release_phase = RESOURCE_RELEASE_BEFORE_LOCKS,
	.release_priority = RELEASE_PRIO_FIRST,
	.ReleaseResource = otel_span_release_resource,
	.DebugPrint = otel_span_debug_print,
};

static void end_slot(int idx, TimestampTz end_time, bool unwinding,
					 const char *unwind_reason);


/* ----------------------------------------------------------------
 * Handles, IDs, small helpers
 * ---------------------------------------------------------------- */

static inline OtelSpanRef
make_ref(uint32 gen, int idx, bool recording)
{
	int64		v = ((int64) gen << 32) | (uint32) idx;

	return (OtelSpanRef) {recording ? v : -v};
}

static inline uint32
ref_gen(OtelSpanRef s)
{
	int64		v = s.v < 0 ? -s.v : s.v;

	return (uint32) (v >> 32);
}

static inline int
ref_idx(OtelSpanRef s)
{
	int64		v = s.v < 0 ? -s.v : s.v;

	return (int) (uint32) v;
}

static void
seed_prng_if_needed(void)
{
	if (likely(otel_prng_pid == MyProcPid))
		return;
	if (!pg_prng_strong_seed(&otel_prng))
		pg_prng_seed(&otel_prng, (uint64) MyProcPid ^ (uint64) GetCurrentTimestamp());
	otel_prng_pid = MyProcPid;
	gen_counter = pg_prng_uint32(&otel_prng) & OTEL_GEN_MASK;
}

static uint32
next_gen(void)
{
	gen_counter = (gen_counter + 1) & OTEL_GEN_MASK;
	if (gen_counter == 0)
		gen_counter = 1;
	return gen_counter;
}

static void
new_span_id(OtelSpanId *id)
{
	uint64		v;

	do
		v = pg_prng_uint64(&otel_prng);
	while (v == 0);
	memcpy(id->b, &v, sizeof(v));
}

static void
new_trace_id(OtelTraceId *id)
{
	uint64		v[2];

	do
	{
		v[0] = pg_prng_uint64(&otel_prng);
		v[1] = pg_prng_uint64(&otel_prng);
	} while (v[0] == 0 && v[1] == 0);
	memcpy(id->b, v, sizeof(v));
}

/*
 * The slot for a positive handle, or NULL if stale.  A handle whose span
 * its owner ended is stale but not a caller bug, so it isn't counted.
 */
static OtelSlot *
slot_for_ref(OtelSpanRef s)
{
	int			idx = ref_idx(s);
	uint32		gen = ref_gen(s);

	Assert(s.v > 0);
	if (idx < slots_used && slots[idx].gen == gen)
		return &slots[idx];
	if (idx < slots_used && slots[idx].owner_released_gen == gen)
		return NULL;
	otel_counters.stale_handle++;
	Assert(false);				/* use after end, double end, or foreign handle */
	return NULL;
}

static OtelNrec *
nrec_for_ref(OtelSpanRef s)
{
	int			idx = ref_idx(s);
	uint32		gen = ref_gen(s);

	Assert(s.v < 0);
	if (idx < nrecs_used && nrecs[idx].gen == gen)
		return &nrecs[idx];
	if (idx < nrecs_used && nrecs[idx].owner_released_gen == gen)
		return NULL;
	otel_counters.stale_handle++;
	Assert(false);				/* use after end, double end, or foreign handle */
	return NULL;
}

/*
 * Test-only allocation-failure injection: compiles to a no-op / always
 * false without USE_INJECTION_POINTS (INJECTION_POINT() is a macro that
 * expands to nothing in that case; see utils/injection_point.h).  With
 * it, tests/otel_api_conformance can arm one of the named sites below
 * for the current backend so the next allocation at that site behaves
 * exactly as if MCXT_ALLOC_NO_OOM (or repalloc_extended/ResourceOwner-
 * Enlarge/otel_tracer_register) had failed -- otel_api's own code takes
 * the same NULL/ERROR path it would on a real OOM.
 *
 * Site names (each is "otel-api-oom-<site>"):
 *   pool             ensure_pool's slots array
 *   name             a span's name (start, or otel_span_set_name)
 *   tracestate       a copied parent tracestate
 *   attr-key         an attribute (or event-attribute) key
 *   attr-value       an attribute (or event-attribute) value
 *   event-name       an event's name
 *   status-description  a span's status description (auto or user-set)
 *   error-message    a captured error's message
 *   error-detail     a captured error's detail
 *   error-hint       a captured error's hint
 *   error-filename   a captured error's source file name
 *   error-funcname   a captured error's source function name
 *   attrs            growing the attributes array
 *   events           growing the events array
 *   links            growing the links array
 *   exception-attrs  the "exception" event's attribute array
 *   event-attrs      an event's attribute-array copy (otel_span_add_event)
 *   vprintf          the oversized buffer in otel_span_set_vprintf
 *
 * Four more sites precede a call that can raise ERROR for a reason other
 * than a real OOM, armed the same way but with the callback itself doing
 * the ereport (see tests/otel_api_conformance's injection callback):
 *   resowner-enlarge  ResourceOwnerEnlarge() in otel_span_start()
 *   slot-context      a slot's first AllocSetContextCreate() (take_slot)
 *   tracer-register   otel_tracer_register() in otel_span_start()
 *   owner-forget      ResourceOwnerForget() in end_slot(): models its
 *                     "after release started" ERROR, not an OOM
 *
 * No site sits on a path reachable from a critical section: otel_api's
 * own use of otel_span_context_of_internal() (from a commit-time trace-
 * context callback) allocates nothing on any of its paths.
 */
static inline bool
otel_inject_fail(const char *site)
{
	bool		fail = false;

	INJECTION_POINT(site, &fail);
	return fail;
}

/* Copy up to maxlen bytes of str, clipped at a character boundary. */
static char *
slot_strdup(OtelSlot *slot, const char *str, int maxlen, bool *truncated,
			const char *site)
{
	size_t		len = strlen(str);
	char	   *copy;

	if (truncated)
		*truncated = false;
	if (maxlen >= 0 && len > (size_t) maxlen)
	{
		len = pg_mbcliplen(str, len, maxlen);
		if (truncated)
			*truncated = true;
	}
	if (slot->bytes + len + 1 > (Size) otel_max_span_bytes)
		return NULL;
	if (otel_inject_fail(site))
		return NULL;
	copy = MemoryContextAllocExtended(slot->cxt, len + 1, MCXT_ALLOC_NO_OOM);
	if (copy == NULL)
		return NULL;
	memcpy(copy, str, len);
	copy[len] = '\0';
	slot->bytes += len + 1;
	return copy;
}

/* Grow an array in the slot's context to hold at least want elements. */
static bool
slot_grow(OtelSlot *slot, void **arr, int *cap, int want, Size elemsize,
		  void *inline_arr, const char *site)
{
	int			newcap;
	void	   *newarr;

	if (want <= *cap)
		return true;
	newcap = Max(want, *cap * 2);
	newcap = Max(newcap, 4);
	if (slot->bytes + (Size) (newcap - *cap) * elemsize > (Size) otel_max_span_bytes)
		return false;
	if (otel_inject_fail(site))
		return false;
	if (*arr == NULL || *arr == inline_arr)
	{
		newarr = MemoryContextAllocExtended(slot->cxt, newcap * elemsize,
											MCXT_ALLOC_NO_OOM);
		if (newarr != NULL && *arr != NULL)
			memcpy(newarr, *arr, *cap * elemsize);
	}
	else
		newarr = repalloc_extended(*arr, newcap * elemsize, MCXT_ALLOC_NO_OOM);
	if (newarr == NULL)
		return false;
	slot->bytes += (Size) (newcap - *cap) * elemsize;
	*arr = newarr;
	*cap = newcap;
	return true;
}

static const char *
stack_entry_name(int32 e)
{
	return e >= 0 ? slots[e].span.name : "(unsampled span)";
}

static void
nonlifo_warning(const char *what, int pos)
{
	otel_counters.non_lifo_end++;
	if (!non_lifo_warned)
	{
		non_lifo_warned = true;
		ereport(WARNING,
				errmsg("otel_api: %s", what),
				errdetail("Ending \"%s\" at stack depth %d; \"%s\" is on top at depth %d. "
						  "The spans above it were ended first. "
						  "Further occurrences in this backend are only counted.",
						  stack_entry_name(span_stack[pos]), pos + 1,
						  stack_entry_name(span_stack[span_stack_depth - 1]),
						  span_stack_depth));
	}
	Assert(false);				/* spans must end in LIFO order */
}


/* ----------------------------------------------------------------
 * The active stack
 * ---------------------------------------------------------------- */

static inline int32
stack_entry_for_slot(int idx)
{
	return idx;
}

static inline int32
stack_entry_for_nrec(int idx)
{
	return -idx - 1;
}

static int
stack_find(int32 entry)
{
	for (int i = span_stack_depth - 1; i >= 0; i--)
		if (span_stack[i] == entry)
			return i;
	return -1;
}

/* Push entry, recording whether it is an activation (see StackEntryMeta). */
static inline void
stack_push(int32 entry, bool is_activation, int nest_level)
{
	span_stack[span_stack_depth] = entry;
	span_stack_meta[span_stack_depth].is_activation = is_activation;
	span_stack_meta[span_stack_depth].nest_level = nest_level;
	span_stack_depth++;
}

/* Remove the entry at position pos, closing the gap. */
static void
stack_remove_at(int pos)
{
	Assert(pos >= 0 && pos < span_stack_depth);
	memmove(&span_stack[pos], &span_stack[pos + 1],
			(span_stack_depth - pos - 1) * sizeof(span_stack[0]));
	memmove(&span_stack_meta[pos], &span_stack_meta[pos + 1],
			(span_stack_depth - pos - 1) * sizeof(span_stack_meta[0]));
	span_stack_depth--;
}

static void
free_nrec(int idx, bool by_owner)
{
	OtelNrec   *n = &nrecs[idx];

	if (by_owner)
		n->owner_released_gen = n->gen;
	n->gen = 0;
	n->next_free = nrec_free_head;
	nrec_free_head = idx;
}

/*
 * End everything above position pos on the stack, as happens when a span
 * lower down ends first.  Recording spans end as if unwound.
 */
static void
stack_unwind_above(int pos, const char *reason)
{
	while (span_stack_depth > pos + 1)
	{
		int32		e = span_stack[span_stack_depth - 1];
		bool		is_activation = span_stack_meta[span_stack_depth - 1].is_activation;

		span_stack_depth--;
		if (is_activation)
		{
			/*
			 * An activation names a .detached span some other
			 * component still owns (e.g. a cursor's executor span).
			 * Unwinding past it must only pop it off the active
			 * stack, never end the span itself -- that would end a
			 * span out from under its real owner.
			 */
			otel_counters.activation_unwound++;
			continue;
		}
		if (e >= 0)
			end_slot(e, 0, true, reason);
		else
			free_nrec(-e - 1, false);
	}
}

/*
 * cassert only: a .scoped span on the stack whose start frame has
 * returned was leaked by an early return.  The stack grows down on every
 * supported platform, so a current address above an address in the
 * start frame means that frame is gone.  Frames of the same depth aren't
 * detected; this finds leaks, it doesn't prove their absence.
 */
static void
check_scoped_frames(void)
{
#ifdef USE_ASSERT_CHECKING
	char		here;

	for (int i = 0; i < span_stack_depth; i++)
	{
		int32		e = span_stack[i];

		if (e >= 0 && slots[e].scope_frame != NULL &&
			(char *) &here > (char *) slots[e].scope_frame)
		{
			elog(LOG, "otel_api: scoped span \"%s\" is still open after the function that started it returned",
				 slots[e].span.name);
			Assert(false);		/* .scoped span leaked by an early return */
		}
	}
#endif
}


/* ----------------------------------------------------------------
 * Parents and sampling
 * ---------------------------------------------------------------- */

typedef enum ParentKind
{
	PARENT_NONE,				/* new trace */
	PARENT_REMOTE,				/* a context from outside this backend */
	PARENT_SLOT,				/* a recording span here */
	PARENT_NREC,				/* an unsampled span here */
} ParentKind;

typedef struct ResolvedParent
{
	ParentKind	kind;
	int			idx;			/* PARENT_SLOT, PARENT_NREC */
	OtelSpanContext ctx;		/* PARENT_REMOTE */
} ResolvedParent;

static void
resolve_from_entry(int32 e, ResolvedParent *p)
{
	if (e >= 0)
	{
		p->kind = PARENT_SLOT;
		p->idx = e;
	}
	else
	{
		p->kind = PARENT_NREC;
		p->idx = -e - 1;
	}
}

/* The parent a span started with OTEL_PARENT_ACTIVE gets. */
static void
resolve_active_parent(ResolvedParent *p)
{
	memset(p, 0, sizeof(*p));
	if (span_stack_depth > 0)
	{
		resolve_from_entry(span_stack[span_stack_depth - 1], p);
		return;
	}
	if (IsParallelWorker() && otel_parallel_get_leader_context(&p->ctx))
	{
		p->kind = PARENT_REMOTE;
		return;
	}
	if (otel_root_ctx.is_set)
	{
		p->kind = PARENT_REMOTE;
		p->ctx = otel_root_ctx.ctx;
		p->ctx.tracestate = (otel_tracestate_guc && otel_tracestate_guc[0])
			? otel_tracestate_guc : NULL;
	}
}

static bool
resolve_parent(const OtelSpanStartArgs *args, ResolvedParent *p)
{
	memset(p, 0, sizeof(*p));
	switch (args->parent)
	{
		case OTEL_PARENT_ACTIVE:
			resolve_active_parent(p);
			return true;
		case OTEL_PARENT_CONTEXT:
			if (args->parent_ctx != NULL &&
				otel_span_context_is_valid(args->parent_ctx))
			{
				p->kind = PARENT_REMOTE;
				p->ctx = *args->parent_ctx;
			}
			return true;
		case OTEL_PARENT_SPAN:
			if (args->parent_span.v > 0)
			{
				OtelSlot   *ps = slot_for_ref(args->parent_span);

				if (ps == NULL)
					return false;
				p->kind = PARENT_SLOT;
				p->idx = ps - slots;
			}
			else if (args->parent_span.v < 0)
			{
				OtelNrec   *pn = nrec_for_ref(args->parent_span);

				if (pn == NULL)
					return false;
				p->kind = PARENT_NREC;
				p->idx = pn - nrecs;
			}
			return true;
		case OTEL_PARENT_ROOT:
			return true;
	}
	return false;
}


/* ----------------------------------------------------------------
 * Slot allocation and release
 * ---------------------------------------------------------------- */

static bool
ensure_pool(void)
{
	if (likely(slots != NULL))
		return true;
	if (span_pool_cxt == NULL)
		span_pool_cxt = AllocSetContextCreate(TopMemoryContext, "otel_api span pool",
											  ALLOCSET_SMALL_SIZES);
	nslots = otel_max_open_spans;
	if (otel_inject_fail("otel-api-oom-pool"))
		return false;
	/* Not zeroed: slots are initialised when first handed out. */
	slots = MemoryContextAllocExtended(span_pool_cxt, sizeof(OtelSlot) * nslots,
									   MCXT_ALLOC_NO_OOM | MCXT_ALLOC_HUGE);
	return slots != NULL;
}

/* Take a free slot, creating its memory context if needed. */
static int
take_slot(void)
{
	int			idx;
	OtelSlot   *slot;

	if (slot_free_head >= 0)
	{
		idx = slot_free_head;
		slot_free_head = slots[idx].next_free;
	}
	else if (slots_used < nslots)
	{
		idx = slots_used++;
		memset(&slots[idx], 0, sizeof(OtelSlot));
	}
	else
		return -1;

	slot = &slots[idx];
	if (slot->cxt == NULL)
	{
		/* Can raise ERROR on OOM; the slot is still free if it does. */
		PG_TRY();
		{
			INJECTION_POINT("otel-api-oom-slot-context", NULL);
			slot->cxt = AllocSetContextCreate(span_pool_cxt, "otel_api span",
											  ALLOCSET_SMALL_SIZES);
		}
		PG_CATCH();
		{
			slot->next_free = slot_free_head;
			slot_free_head = idx;
			PG_RE_THROW();
		}
		PG_END_TRY();
	}
	return idx;
}

static void
release_slot(int idx, bool by_owner)
{
	OtelSlot   *slot = &slots[idx];
	MemoryContext cxt = slot->cxt;
	uint32		released = by_owner ? slot->gen : slot->owner_released_gen;

	if (slot->owner != NULL && !by_owner)
		ResourceOwnerForget(slot->owner, Int64GetDatum(make_ref(slot->gen, idx, true).v),
							&otel_span_resowner_desc);
	if (slot->session)
		session_spans_open--;
	MemoryContextReset(cxt);
	memset(slot, 0, sizeof(OtelSlot));
	slot->cxt = cxt;
	slot->owner_released_gen = released;
	slot->next_free = slot_free_head;
	slot_free_head = idx;
}


/* ----------------------------------------------------------------
 * Errors
 * ---------------------------------------------------------------- */

/*
 * Record edata in the slot, keeping the most severe error (the latest,
 * on a tie).  Copies everything, with MCXT_ALLOC_NO_OOM: a field that
 * can't be copied is left NULL.  Never raises an error.
 */
static void
slot_record_error(OtelSlot *slot, const ErrorData *edata)
{
	OtelSlotError *err = &slot->err;
	bool		oom = edata->sqlerrcode == ERRCODE_OUT_OF_MEMORY;

	if (err->used && edata->elevel < err->elevel)
		return;

	memset(err, 0, sizeof(*err));
	err->used = true;
	err->elevel = edata->elevel;
	err->lineno = edata->lineno;
	err->time = GetCurrentTimestamp();
	strlcpy(err->sqlstate, unpack_sql_state(edata->sqlerrcode), sizeof(err->sqlstate));

	/* After an OOM, don't make the allocator's day worse. */
	if (!oom)
	{
		if (edata->message)
			err->message = slot_strdup(slot, edata->message, otel_attr_value_max, NULL,
										"otel-api-oom-error-message");
		if (edata->detail)
			err->detail = slot_strdup(slot, edata->detail, otel_attr_value_max, NULL,
									  "otel-api-oom-error-detail");
		if (edata->hint)
			err->hint = slot_strdup(slot, edata->hint, otel_attr_value_max, NULL,
									"otel-api-oom-error-hint");
		/* May point into JIT code unloaded at transaction end: copy. */
		if (edata->filename)
			err->filename = slot_strdup(slot, edata->filename, -1, NULL,
										"otel-api-oom-error-filename");
		if (edata->funcname)
			err->funcname = slot_strdup(slot, edata->funcname, -1, NULL,
										"otel-api-oom-error-funcname");
		if ((edata->message && !err->message) || (edata->detail && !err->detail) ||
			(edata->hint && !err->hint))
			otel_counters.error_capture_failed++;
	}
	else
		otel_counters.error_capture_failed++;

	if (edata->elevel >= ERROR)
		slot->span.status = OTEL_STATUS_ERROR;
}

/*
 * "SQLSTATE / message" in the slot's context, for a status description.
 * NULL if it can't be allocated.
 */
static const char *
error_description(OtelSlot *slot, const char *sqlstate, const char *message)
{
	char	   *buf;

	if (otel_inject_fail("otel-api-oom-status-description"))
		return NULL;
	buf = MemoryContextAllocExtended(slot->cxt, 256, MCXT_ALLOC_NO_OOM);

	if (buf)
	{
		if (message)
			snprintf(buf, 256, "%s / %s", sqlstate, message);
		else
			strlcpy(buf, sqlstate, 256);
	}
	return buf;
}

/*
 * emit_log_hook: record WARNING and worse into the innermost recording
 * span.  An ERROR goes into the innermost recording span that isn't a
 * session span, since the abort that follows ends and exports it; that
 * span gets the exception event.  The other non-session spans on the
 * stack are ended by the same abort: they get ERROR status and the
 * SQLSTATE and message as their status description, but no event.
 * Session spans outlive the abort and are left alone.
 *
 * This runs for errors that reach EmitErrorReport, which for a top-level
 * ERROR is before transaction abort releases the spans.  Errors caught
 * before that (plpgsql EXCEPTION, C PG_CATCH) need
 * otel_span_capture_error().
 */
static void
otel_emit_log_hook(ErrorData *edata)
{
	if (edata->elevel >= WARNING && CritSectionCount == 0)
	{
		bool		recorded = false;

		for (int i = span_stack_depth - 1; i >= 0; i--)
		{
			int32		e = span_stack[i];
			OtelSlot   *slot;

			if (e < 0 || slots[e].dispatching)
				continue;
			slot = &slots[e];
			if (edata->elevel < ERROR)
			{
				slot_record_error(slot, edata);
				break;
			}
			if (slot->session)
				continue;
			if (!recorded)
			{
				slot_record_error(slot, edata);
				recorded = true;
			}
			else
			{
				slot->span.status = OTEL_STATUS_ERROR;
				if (slot->span.status_description == NULL &&
					edata->sqlerrcode != ERRCODE_OUT_OF_MEMORY)
					slot->span.status_description =
						error_description(slot, unpack_sql_state(edata->sqlerrcode),
										  edata->message);
			}
		}
	}
	if (prev_emit_log_hook)
		prev_emit_log_hook(edata);
}

/* Lower a captured error into an "exception" event and status text. */
static void
lower_error_event(OtelSlot *slot)
{
	OtelSlotError *err = &slot->err;
	OtelAttribute a[8];
	int			n = 0;
	OtelSpanEvent *ev;

	a[n++] = OTEL_ATTR_STR(OTEL_SC_EXCEPTION_TYPE, err->sqlstate);
	if (err->message)
		a[n++] = OTEL_ATTR_STR(OTEL_SC_EXCEPTION_MESSAGE, err->message);
	a[n++] = OTEL_ATTR_I64(OTEL_PG_ERROR_ELEVEL, err->elevel);
	if (err->detail)
		a[n++] = OTEL_ATTR_STR(OTEL_PG_ERROR_DETAIL, err->detail);
	if (err->hint)
		a[n++] = OTEL_ATTR_STR(OTEL_PG_ERROR_HINT, err->hint);
	if (err->funcname)
		a[n++] = OTEL_ATTR_STR(OTEL_SC_CODE_FUNCTION_NAME, err->funcname);
	if (err->filename)
		a[n++] = OTEL_ATTR_STR(OTEL_SC_CODE_FILE_PATH, err->filename);
	a[n++] = OTEL_ATTR_I64(OTEL_SC_CODE_LINE_NUMBER, err->lineno);

	/* The strings are already in the slot's context: store, don't copy. */
	if (slot_grow(slot, (void **) &slot->events, &slot->events_cap,
				  slot->span.n_events + 1, sizeof(OtelSpanEvent), NULL,
				  "otel-api-oom-events"))
	{
		OtelAttribute *attrs = otel_inject_fail("otel-api-oom-exception-attrs") ? NULL :
			MemoryContextAllocExtended(slot->cxt, sizeof(a[0]) * n, MCXT_ALLOC_NO_OOM);

		ev = &slot->events[slot->span.n_events++];
		ev->name = OTEL_SC_EXCEPTION_EVENT;
		ev->time = err->time;
		ev->n_attrs = attrs ? n : 0;
		ev->attrs = attrs;
		if (attrs)
			memcpy(attrs, a, sizeof(a[0]) * n);
	}
	else
		slot->span.dropped_events++;

	if (slot->span.status == OTEL_STATUS_ERROR && slot->span.status_description == NULL)
		slot->span.status_description = error_description(slot, err->sqlstate, err->message);
}


/* ----------------------------------------------------------------
 * Ending and dispatching
 * ---------------------------------------------------------------- */

static void
dispatch_span(const OtelSpan *span)
{
	otel_span_emit_hook_type emit_hook = otel_get_span_emit_hook();
	uint32		save_holdoff;
	uint32		save_query_cancel_holdoff;
	MemoryContext save_cxt;
	int			save_depth = dispatch_depth;

	if (emit_hook == NULL && !otel_emit_spans_to_log)
		return;

	save_holdoff = InterruptHoldoffCount;
	save_query_cancel_holdoff = QueryCancelHoldoffCount;
	save_cxt = CurrentMemoryContext;
	PG_TRY();
	{
		dispatch_depth++;
		if (emit_hook)
			emit_hook(span);
		if (otel_emit_spans_to_log)
			otel_emit_span_as_log_line(span);
		dispatch_depth--;
	}
	PG_CATCH();
	{
		/*
		 * A tracing failure must not break the traced operation.
		 * errfinish() zeroes InterruptHoldoffCount and
		 * QueryCancelHoldoffCount before throwing, and error handling
		 * leaves CurrentMemoryContext as ErrorContext; restore all three
		 * (see the comment in errfinish()).
		 */
		MemoryContextSwitchTo(save_cxt);
		dispatch_depth = save_depth;
		InterruptHoldoffCount = save_holdoff;
		QueryCancelHoldoffCount = save_query_cancel_holdoff;
		FlushErrorState();
		otel_counters.emit_hook_errors++;
	}
	PG_END_TRY();
	otel_counters.spans_emitted++;
}

/*
 * End the span in slot idx: export it and free the slot.  The caller
 * has taken it off the active stack.  unwinding means the span didn't
 * reach otel_span_end(): it is exported with ERROR status, since
 * storage is owned by otel_api and exporting is always memory-safe.
 *
 * Forget the resource-owner entry before dispatching.
 * ResourceOwnerForget() raises ERROR if the owner has started releasing;
 * the span is then still unexported and still owned, and the owner's
 * release exports it once.  From the forget to the slot reset in
 * release_slot(), nothing may raise ERROR: dispatch_span() catches
 * errors, and lower_error_event() allocates with MCXT_ALLOC_NO_OOM.
 */
static void
end_slot(int idx, TimestampTz end_time, bool unwinding, const char *unwind_reason)
{
	OtelSlot   *slot = &slots[idx];

	Assert(!slot->dispatching);

	if (slot->owner != NULL)
	{
		/* Test-only: see the injection-point doc comment above. */
		INJECTION_POINT("otel-api-oom-owner-forget", NULL);
		ResourceOwnerForget(slot->owner, Int64GetDatum(make_ref(slot->gen, idx, true).v),
							&otel_span_resowner_desc);
		slot->owner = NULL;
	}

	if (unwinding)
	{
		slot->span.status = OTEL_STATUS_ERROR;
		if (!slot->err.used && slot->span.status_description == NULL)
			slot->span.status_description = unwind_reason;
		otel_counters.unwound++;
	}

	slot->span.end_time = end_time ? end_time : GetCurrentTimestamp();
	if (slot->err.used)
		lower_error_event(slot);
	slot->span.attrs = slot->attrs;
	slot->span.events = slot->events;
	slot->span.links = slot->links;
	slot->dispatching = true;
	dispatch_span(&slot->span);
	slot->dispatching = false;
	release_slot(idx, false);
}

/* ResourceOwnerDesc.DebugPrint: called only for a span leaked at commit. */
static char *
otel_span_debug_print(Datum res)
{
	OtelSpanRef s = {DatumGetInt64(res)};
	int			idx = ref_idx(s);

	if (idx < slots_used && slots[idx].gen == ref_gen(s))
	{
		slots[idx].leaked = true;
		return psprintf("otel_api span \"%s\"", slots[idx].span.name);
	}
	return pstrdup("otel_api span (stale)");
}

/*
 * ResourceOwnerDesc.ReleaseResource: the owner is being released.
 *
 * ResourceOwnerReleaseAll() drops an entry only after this returns.  If
 * an emit hook raises FATAL while the span is dispatched from here, the
 * entry stays, and backend exit (AbortOutOfAnyTransaction) releases the
 * owner again.  The span is then still dispatching: free it without
 * exporting it a second time.
 */
static void
otel_span_release_resource(Datum res)
{
	OtelSpanRef s = {DatumGetInt64(res)};
	int			idx = ref_idx(s);
	OtelSlot   *slot;
	int			pos;

	if (idx >= slots_used || slots[idx].gen != ref_gen(s))
		return;
	slot = &slots[idx];
	slot->owner = NULL;			/* the owner has already forgotten it */

	pos = stack_find(stack_entry_for_slot(idx));
	if (pos >= 0)
		stack_remove_at(pos);

	if (slot->leaked || slot->dispatching)
	{
		if (slot->leaked)
			otel_counters.leaked_at_commit++;
		else
			otel_counters.dropped_in_dispatch++;
		release_slot(idx, true);
		return;
	}

	/* end_slot releases with by_owner = false; record the owner release. */
	{
		uint32		gen = slot->gen;

		end_slot(idx, 0, true, "ended by transaction or subtransaction abort");
		slots[idx].owner_released_gen = gen;
	}
}

/* Drop non-recording entries at subtransaction level >= level. */
static void
drop_nrecs_from_level(int level)
{
	for (int i = span_stack_depth - 1; i >= 0; i--)
	{
		int32		e = span_stack[i];

		if (e < 0 && nrecs[-e - 1].nest_level >= level)
			stack_remove_at(i);
	}
	for (int i = 0; i < nrecs_used; i++)
		if (nrecs[i].gen != 0 && nrecs[i].nest_level >= level)
			free_nrec(i, true);
}

/*
 * Pop (never end) activation entries made at subtransaction level >=
 * level.  Nothing else tears an activation down: the span it names
 * has its own, independent lifecycle (owned by a resource owner, or a
 * session/nrec that outlives the transaction), so otel_api must stop
 * treating it as current without touching that lifecycle at all.
 *
 * is_leak counts each one in activation_leaked_at_commit: at a
 * top-level COMMIT/PREPARE, a remaining activation means the caller
 * never deactivated it, same shape as a leaked span.  At ABORT (top-
 * level or subtransaction) this is ordinary unwind-time cleanup, not a
 * caller mistake, so it isn't counted here.
 */
static void
drop_activations_from_level(int level, bool is_leak)
{
	for (int i = span_stack_depth - 1; i >= 0; i--)
	{
		if (span_stack_meta[i].is_activation && span_stack_meta[i].nest_level >= level)
		{
			if (is_leak)
				otel_counters.activation_leaked_at_commit++;
			stack_remove_at(i);
		}
	}
}

static void
otel_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PREPARE:
			drop_nrecs_from_level(1);
			drop_activations_from_level(1, true);
			break;
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			drop_nrecs_from_level(1);
			drop_activations_from_level(1, false);
			break;
		default:
			break;
	}
}

static void
otel_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
					  SubTransactionId parentSubid, void *arg)
{
	if (event == SUBXACT_EVENT_ABORT_SUB)
	{
		int			level = GetCurrentTransactionNestLevel();

		drop_nrecs_from_level(level);
		drop_activations_from_level(level, false);
	}
}

static void
otel_producer_shmem_exit(int code, Datum arg)
{
	otel_counters.open_at_exit += session_spans_open;
}


/* ----------------------------------------------------------------
 * Producer API
 * ---------------------------------------------------------------- */

static OtelSpanRef
start_nrec(const OtelSpanStartArgs *args, const ResolvedParent *p,
		   const OtelTraceId *trace_id, uint8 random_flag)
{
	int			idx;
	OtelNrec   *n;

	if (!args->detached && span_stack_depth >= OTEL_MAX_STACK_DEPTH)
	{
		otel_counters.start_stack_full++;
		return OTEL_SPAN_NONE;
	}
	if (nrec_free_head >= 0)
	{
		idx = nrec_free_head;
		nrec_free_head = nrecs[idx].next_free;
	}
	else if (nrecs_used < OTEL_MAX_STACK_DEPTH)
	{
		idx = nrecs_used++;
		nrecs[idx].owner_released_gen = 0;
	}
	else
	{
		otel_counters.start_stack_full++;
		return OTEL_SPAN_NONE;
	}

	n = &nrecs[idx];
	n->gen = next_gen();

	/*
	 * An unsampled span has no resource owner; transaction and
	 * subtransaction end drop it by level.  One that would have been a
	 * session span, as a recording span, gets level 0 so no transaction
	 * end drops it.  A default-owner span is dropped at the end of its
	 * (sub)transaction, a little later than a recording span would be
	 * (at the end of its statement's portal).
	 */
	if (args->owner == OTEL_OWNER_SESSION ||
		(args->owner == NULL && CurrentResourceOwner == NULL))
		n->nest_level = 0;
	else
		n->nest_level = GetCurrentTransactionNestLevel();
	n->detached = args->detached;
	n->have_span_id = false;
	memset(&n->ctx, 0, sizeof(n->ctx));
	n->ctx.trace_id = *trace_id;
	n->ctx.trace_flags = random_flag;	/* sampled bit clear */
	(void) p;

	if (!args->detached)
		stack_push(stack_entry_for_nrec(idx), false, 0);
	otel_counters.spans_unsampled++;
	return make_ref(n->gen, idx, false);
}

static void nrec_context(OtelNrec *n, OtelSpanContext *out);

/*
 * No producer call may run inside a critical section, or from inside an
 * emit hook.  cassert builds fail an Assert; other builds count the call
 * and do nothing.  A read_only call allocates nothing and changes no
 * span, so an emit hook may make it.  After a FATAL from an emit hook,
 * backend exit runs with dispatch_depth still raised; calls made then are
 * refused without the Assert.
 */
static inline bool
call_refused(bool read_only)
{
	Assert(CritSectionCount == 0);
	Assert(read_only || dispatch_depth == 0 || proc_exit_inprogress);
	if (unlikely(CritSectionCount != 0))
	{
		otel_counters.in_crit_section++;
		return true;
	}
	if (unlikely(dispatch_depth > 0) && !read_only)
	{
		otel_counters.in_emit_hook++;
		return true;
	}
	return false;
}

static OtelSpanRef
api_span_start(const OtelSpanStartArgs *args)
{
	ResolvedParent p;
	OtelTraceId trace_id;
	uint8		parent_flags = 0;
	OtelSamplerDecision decision;
	ResourceOwner owner;
	bool		session;
	int			idx;
	OtelSlot   *slot;
	OtelSpanRef ref;

	if (call_refused(false))
		return OTEL_SPAN_NONE;
	if (args == NULL || args->struct_size < sizeof(OtelSpanStartArgs) ||
		args->name == NULL)
	{
		otel_counters.start_bad_args++;
		Assert(false);			/* bad arguments to otel_span_start */
		return OTEL_SPAN_NONE;
	}
	seed_prng_if_needed();
	check_scoped_frames();

	if (!resolve_parent(args, &p))
	{
		otel_counters.start_bad_args++;
		return OTEL_SPAN_NONE;
	}

	/* The trace ID and the sampling decision. */
	switch (p.kind)
	{
		case PARENT_SLOT:
			trace_id = slots[p.idx].span.trace_id;
			parent_flags = slots[p.idx].span.trace_flags;
			decision = (parent_flags & OTEL_TRACE_FLAG_SAMPLED)
				? OTEL_SAMPLE_RECORD_AND_SAMPLE : OTEL_SAMPLE_RECORD_ONLY;
			break;
		case PARENT_NREC:
			/*
			 * An unsampled local parent.  force_sample does not apply
			 * here: it only forces a new root to record (PARENT_NONE,
			 * below), never a child of an unsampled parent --- that
			 * would record a span whose parent is never exported,
			 * producing an orphan.
			 */
			return start_nrec(args, &p, &nrecs[p.idx].ctx.trace_id,
							  nrecs[p.idx].ctx.trace_flags & OTEL_TRACE_FLAG_RANDOM);
		case PARENT_REMOTE:
			{
				trace_id = p.ctx.trace_id;
				parent_flags = p.ctx.trace_flags;
				decision = otel_run_sampler(&trace_id, false,
											otel_span_context_sampled(&p.ctx));
				break;
			}
		case PARENT_NONE:
		default:
			{
				new_trace_id(&trace_id);
				parent_flags = OTEL_TRACE_FLAG_RANDOM;
				decision = otel_run_sampler(&trace_id, true, false);
				/*
				 * force_sample applies only to a new root: an explicit
				 * per-session opt-in ("trace everything") bypasses the
				 * sampler for spans that start their own trace.  It
				 * must never override the decision for a span with a
				 * parent (PARENT_SLOT/PARENT_NREC/PARENT_REMOTE above) ---
				 * doing so would record a span under an unrecorded
				 * parent, and that parent is never exported, producing an
				 * orphan span.
				 */
				if (args->force_sample)
					decision = OTEL_SAMPLE_RECORD_AND_SAMPLE;
				break;
			}
	}

	if (decision == OTEL_SAMPLE_DROP)
		return start_nrec(args, &p, &trace_id, parent_flags & OTEL_TRACE_FLAG_RANDOM);

	/* A recording span.  Find its owner, then a slot. */
	if (args->owner == OTEL_OWNER_SESSION)
		owner = NULL;
	else if (args->owner != NULL)
		owner = args->owner;
	else
		owner = CurrentResourceOwner;
	session = owner == NULL;

	if (session && session_spans_open >= otel_max_session_spans)
	{
		otel_counters.start_no_session_slot++;
		return OTEL_SPAN_NONE;
	}
	if (!args->detached && span_stack_depth >= OTEL_MAX_STACK_DEPTH)
	{
		otel_counters.start_stack_full++;
		return OTEL_SPAN_NONE;
	}
	if (!ensure_pool())
	{
		otel_counters.start_no_slot++;
		return OTEL_SPAN_NONE;
	}
	/* These can raise ERROR on OOM; nothing to undo yet. */
	if (args->tracer != NULL && args->tracer->scope == NULL && args->tracer->name != NULL)
	{
		INJECTION_POINT("otel-api-oom-tracer-register", NULL);
		args->tracer->scope = otel_tracer_register(args->tracer->name,
												   args->tracer->version,
												   args->tracer->schema_url);
	}
	if (owner != NULL)
	{
		INJECTION_POINT("otel-api-oom-resowner-enlarge", NULL);
		ResourceOwnerEnlarge(owner);
	}
	idx = take_slot();
	if (idx < 0)
	{
		otel_counters.start_no_slot++;
		return OTEL_SPAN_NONE;
	}

	slot = &slots[idx];
	slot->gen = next_gen();
	slot->detached = args->detached;
	slot->session = session;
	slot->owner = owner;
	slot->attrs = slot->inline_attrs;
	slot->attrs_cap = OTEL_SLOT_INLINE_ATTRS;
#ifdef USE_ASSERT_CHECKING
	slot->scope_frame = args->scoped ? (void *) args : NULL;
#endif

	slot->span.struct_size = sizeof(OtelSpan);
	slot->span.kind = args->kind;
	if (args->tracer != NULL)
		slot->span.scope = args->tracer->scope;
	slot->span.trace_id = trace_id;
	new_span_id(&slot->span.span_id);
	if (p.kind == PARENT_SLOT)
	{
		slot->span.parent_span_id = slots[p.idx].span.span_id;
		if (slots[p.idx].span.tracestate)
			slot->span.tracestate = slot_strdup(slot, slots[p.idx].span.tracestate, -1, NULL,
												"otel-api-oom-tracestate");
	}
	else if (p.kind == PARENT_REMOTE)
	{
		slot->span.parent_span_id = p.ctx.span_id;
		if (p.ctx.tracestate)
			slot->span.tracestate = slot_strdup(slot, p.ctx.tracestate, -1, NULL,
												"otel-api-oom-tracestate");
	}
	slot->span.trace_flags = (parent_flags & ~OTEL_TRACE_FLAG_SAMPLED) |
		(decision == OTEL_SAMPLE_RECORD_AND_SAMPLE ? OTEL_TRACE_FLAG_SAMPLED : 0);
	slot->span.sampler_decision = decision;
	slot->span.name = slot_strdup(slot, args->name, otel_attr_value_max, NULL,
								  "otel-api-oom-name");
	if (slot->span.name == NULL)
		slot->span.name = "(out of memory)";
	slot->span.start_time = args->start_time ? args->start_time : GetCurrentTimestamp();

	ref = make_ref(slot->gen, idx, true);
	if (!args->detached)
		stack_push(stack_entry_for_slot(idx), false, 0);
	if (owner != NULL)
		ResourceOwnerRemember(owner, Int64GetDatum(ref.v), &otel_span_resowner_desc);
	else
	{
		session_spans_open++;
		/* Exit callbacks registered in _PG_init are reset in each backend. */
		if (!exit_callback_registered)
		{
			before_shmem_exit(otel_producer_shmem_exit, (Datum) 0);
			exit_callback_registered = true;
		}
	}
	otel_counters.spans_started++;
	return ref;
}

static void
api_span_end(OtelSpanRef s, TimestampTz end_time)
{
	int			pos;

	if (call_refused(false) || s.v == 0)
		return;
	check_scoped_frames();

	if (s.v < 0)
	{
		OtelNrec   *n = nrec_for_ref(s);
		int			idx;

		if (n == NULL)
			return;
		idx = n - nrecs;

		/*
		 * Search the stack regardless of .detached: a .detached entry
		 * that is currently active (otel_span_activate()) is on the
		 * stack too, and must be popped the same way.
		 */
		if ((pos = stack_find(stack_entry_for_nrec(idx))) >= 0)
		{
			if (pos != span_stack_depth - 1)
			{
				nonlifo_warning("unsampled span ended with spans still open above it", pos);
				stack_unwind_above(pos, "parent span ended first");
			}
			span_stack_depth--;
		}
		free_nrec(idx, false);
		return;
	}
	else
	{
		OtelSlot   *slot = slot_for_ref(s);
		int			idx;

		if (slot == NULL)
			return;
		idx = slot - slots;

		/* See the .detached comment in the s.v < 0 branch above. */
		if ((pos = stack_find(stack_entry_for_slot(idx))) >= 0)
		{
			if (pos != span_stack_depth - 1)
			{
				nonlifo_warning("span ended with spans still open above it", pos);
				stack_unwind_above(pos, "parent span ended first");
			}
			span_stack_depth--;
		}
		end_slot(idx, end_time, false, NULL);
	}
}

/* Find or add the attribute key; NULL if it can't be stored. */
static OtelAttribute *
slot_attr(OtelSlot *slot, const char *key)
{
	OtelAttribute *a;
	char	   *k;

	for (int i = 0; i < slot->span.n_attrs; i++)
		if (strcmp(slot->attrs[i].key, key) == 0)
			return &slot->attrs[i];

	if (!slot_grow(slot, (void **) &slot->attrs, &slot->attrs_cap,
				   slot->span.n_attrs + 1, sizeof(OtelAttribute), slot->inline_attrs,
				   "otel-api-oom-attrs") ||
		(k = slot_strdup(slot, key, -1, NULL, "otel-api-oom-attr-key")) == NULL)
	{
		slot->span.dropped_attrs++;
		otel_counters.attr_dropped++;
		return NULL;
	}
	a = &slot->attrs[slot->span.n_attrs++];
	a->key = k;
	a->type = OTEL_ATTR_INT;
	a->v.i = 0;
	return a;
}

static OtelSlot *
setter_slot(OtelSpanRef s)
{
	if (call_refused(false) || s.v <= 0)
		return NULL;
	return slot_for_ref(s);
}

static void
set_str_internal(OtelSlot *slot, const char *key, const char *val)
{
	OtelAttribute *a;
	bool		truncated;
	char	   *copy;

	if (val == NULL || (a = slot_attr(slot, key)) == NULL)
		return;
	copy = slot_strdup(slot, val, otel_attr_value_max, &truncated, "otel-api-oom-attr-value");
	if (copy == NULL)
	{
		/* Keep the key, drop the value: remove the entry again. */
		slot->span.n_attrs--;
		slot->span.dropped_attrs++;
		otel_counters.attr_dropped++;
		return;
	}
	if (truncated)
		otel_counters.attr_truncated++;
	a->type = OTEL_ATTR_STRING;
	a->v.s = copy;
}

static void
api_span_set_str(OtelSpanRef s, const char *key, const char *val)
{
	OtelSlot   *slot = setter_slot(s);

	if (slot)
		set_str_internal(slot, key, val);
}

static void
api_span_set_int(OtelSpanRef s, const char *key, int64 val)
{
	OtelSlot   *slot = setter_slot(s);
	OtelAttribute *a;

	if (slot && (a = slot_attr(slot, key)) != NULL)
	{
		a->type = OTEL_ATTR_INT;
		a->v.i = val;
	}
}

static void
api_span_set_double(OtelSpanRef s, const char *key, double val)
{
	OtelSlot   *slot = setter_slot(s);
	OtelAttribute *a;

	if (slot && (a = slot_attr(slot, key)) != NULL)
	{
		a->type = OTEL_ATTR_DOUBLE;
		a->v.d = val;
	}
}

static void
api_span_set_bool(OtelSpanRef s, const char *key, bool val)
{
	OtelSlot   *slot = setter_slot(s);
	OtelAttribute *a;

	if (slot && (a = slot_attr(slot, key)) != NULL)
	{
		a->type = OTEL_ATTR_BOOL;
		a->v.b = val;
	}
}

static void api_span_set_vprintf(OtelSpanRef s, const char *key, const char *fmt,
								 va_list ap) pg_attribute_printf(3, 0);

static void
api_span_set_vprintf(OtelSpanRef s, const char *key, const char *fmt, va_list ap)
{
	OtelSlot   *slot = setter_slot(s);
	char		buf[1024];
	va_list		ap2;
	int			len;

	if (slot == NULL)
		return;
	va_copy(ap2, ap);
	len = pg_vsnprintf(buf, sizeof(buf), fmt, ap);
	if (len >= (int) sizeof(buf) && otel_attr_value_max >= (int) sizeof(buf))
	{
		/* Longer than the buffer, and attr_value_max allows more. */
		char	   *big = otel_inject_fail("otel-api-oom-vprintf") ? NULL :
			MemoryContextAllocExtended(slot->cxt, len + 1, MCXT_ALLOC_NO_OOM);

		if (big != NULL)
		{
			pg_vsnprintf(big, len + 1, fmt, ap2);
			set_str_internal(slot, key, big);
			pfree(big);
			va_end(ap2);
			return;
		}
	}
	va_end(ap2);
	if (len >= (int) sizeof(buf))
		otel_counters.attr_truncated++;
	set_str_internal(slot, key, buf);
}

static void
api_span_set_name(OtelSpanRef s, const char *name)
{
	OtelSlot   *slot = setter_slot(s);
	char	   *copy;

	if (slot && name &&
		(copy = slot_strdup(slot, name, otel_attr_value_max, NULL, "otel-api-oom-name")) != NULL)
		slot->span.name = copy;
}

static void
api_span_set_status(OtelSpanRef s, OtelSpanStatus code, const char *description)
{
	OtelSlot   *slot = setter_slot(s);

	if (slot == NULL)
		return;
	slot->span.status = code;
	slot->span.status_description = description
		? slot_strdup(slot, description, otel_attr_value_max, NULL,
					  "otel-api-oom-status-description") : NULL;
}

static void
api_span_add_event(OtelSpanRef s, const char *name, TimestampTz ts,
				   const OtelAttribute *attrs, int n_attrs)
{
	OtelSlot   *slot = setter_slot(s);
	OtelSpanEvent *ev;
	OtelAttribute *copy = NULL;
	char	   *ename;

	if (slot == NULL || name == NULL)
		return;
	if (!slot_grow(slot, (void **) &slot->events, &slot->events_cap,
				   slot->span.n_events + 1, sizeof(OtelSpanEvent), NULL,
				   "otel-api-oom-events") ||
		(ename = slot_strdup(slot, name, otel_attr_value_max, NULL,
							 "otel-api-oom-event-name")) == NULL)
		goto dropped;
	if (n_attrs > 0)
	{
		Size		sz = sizeof(OtelAttribute) * n_attrs;

		if (slot->bytes + sz > (Size) otel_max_span_bytes ||
			otel_inject_fail("otel-api-oom-event-attrs") ||
			(copy = MemoryContextAllocExtended(slot->cxt, sz, MCXT_ALLOC_NO_OOM)) == NULL)
			goto dropped;
		slot->bytes += sz;
		for (int i = 0; i < n_attrs; i++)
		{
			copy[i] = attrs[i];
			copy[i].key = slot_strdup(slot, attrs[i].key, -1, NULL, "otel-api-oom-attr-key");
			if (attrs[i].type == OTEL_ATTR_STRING && attrs[i].v.s)
				copy[i].v.s = slot_strdup(slot, attrs[i].v.s, otel_attr_value_max, NULL,
										  "otel-api-oom-attr-value");
			if (copy[i].key == NULL ||
				(attrs[i].type == OTEL_ATTR_STRING && attrs[i].v.s && copy[i].v.s == NULL))
				goto dropped;
		}
	}
	ev = &slot->events[slot->span.n_events++];
	ev->name = ename;
	ev->time = ts ? ts : GetCurrentTimestamp();
	ev->n_attrs = n_attrs;
	ev->attrs = copy;
	return;

dropped:
	slot->span.dropped_events++;
	otel_counters.event_dropped++;
}

static void
api_span_add_link(OtelSpanRef s, const OtelSpanContext *target)
{
	OtelSlot   *slot = setter_slot(s);
	OtelSpanContext *l;

	if (slot == NULL || target == NULL || !otel_span_context_is_valid(target))
		return;
	if (!slot_grow(slot, (void **) &slot->links, &slot->links_cap,
				   slot->span.n_links + 1, sizeof(OtelSpanContext), NULL,
				   "otel-api-oom-links"))
	{
		slot->span.dropped_links++;
		otel_counters.link_dropped++;
		return;
	}
	l = &slot->links[slot->span.n_links++];
	*l = *target;
	l->tracestate = NULL;
}

static void
api_span_record_error(OtelSpanRef s, const ErrorData *edata)
{
	OtelSlot   *slot = setter_slot(s);

	if (slot && edata)
		slot_record_error(slot, edata);
}

/*
 * For PG_CATCH.  CopyErrorData is the only way to read the error being
 * handled; it allocates, so it can itself fail and replace the original
 * error with an out-of-memory one.  An OOM being handled is recorded
 * from geterrcode() alone, without copying.
 */
static void
api_span_capture_error(OtelSpanRef s)
{
	OtelSlot   *slot = setter_slot(s);
	MemoryContext old;
	ErrorData  *edata;

	if (slot == NULL)
		return;
	if (geterrcode() == ERRCODE_OUT_OF_MEMORY)
	{
		ErrorData	e = {.elevel = ERROR, .sqlerrcode = ERRCODE_OUT_OF_MEMORY};

		slot_record_error(slot, &e);
		return;
	}
	old = MemoryContextSwitchTo(slot->cxt);
	edata = CopyErrorData();
	MemoryContextSwitchTo(old);
	slot_record_error(slot, edata);
	FreeErrorData(edata);
}

static OtelSpanRef
api_span_current(void)
{
	int32		e;

	if (call_refused(true) || span_stack_depth == 0)
		return OTEL_SPAN_NONE;
	e = span_stack[span_stack_depth - 1];
	if (e >= 0)
		return make_ref(slots[e].gen, e, true);
	return make_ref(nrecs[-e - 1].gen, -e - 1, false);
}

static void
nrec_context(OtelNrec *n, OtelSpanContext *out)
{
	if (!n->have_span_id)
	{
		seed_prng_if_needed();
		new_span_id(&n->ctx.span_id);
		n->have_span_id = true;
	}
	*out = n->ctx;
}

/*
 * otel_span_context_of() without the critical-section check, for
 * otel_api's own commit-record hook, which core calls inside
 * RecordTransactionCommit's critical section.  Allocates nothing.
 */
bool
otel_span_context_of_internal(OtelSpanRef s, OtelSpanContext *out)
{
	memset(out, 0, sizeof(*out));
	if (s.v == 0)
	{
		ResolvedParent p;

		resolve_active_parent(&p);
		switch (p.kind)
		{
			case PARENT_SLOT:
				s = make_ref(slots[p.idx].gen, p.idx, true);
				break;
			case PARENT_NREC:
				nrec_context(&nrecs[p.idx], out);
				return true;
			case PARENT_REMOTE:
				*out = p.ctx;
				return true;
			case PARENT_NONE:
				return false;
		}
	}
	if (s.v > 0)
	{
		OtelSlot   *slot = slot_for_ref(s);

		if (slot == NULL)
			return false;
		out->trace_id = slot->span.trace_id;
		out->span_id = slot->span.span_id;
		out->trace_flags = slot->span.trace_flags;
		out->tracestate = slot->span.tracestate;
		return true;
	}
	else
	{
		OtelNrec   *n = nrec_for_ref(s);

		if (n == NULL)
			return false;
		nrec_context(n, out);
		return true;
	}
}

static bool
api_span_context_of(OtelSpanRef s, OtelSpanContext *out)
{
	if (call_refused(true))
	{
		memset(out, 0, sizeof(*out));
		return false;
	}
	return otel_span_context_of_internal(s, out);
}

static void
api_resource_add(const char *key, const char *value)
{
	if (call_refused(false))
		return;
	otel_resource_attr_add(key, value);
}

static void
api_span_discard(OtelSpanRef s)
{
	int			pos;

	if (call_refused(false) || s.v == 0)
		return;
	if (s.v < 0)
	{
		OtelNrec   *n = nrec_for_ref(s);

		if (n == NULL)
			return;
		if ((pos = stack_find(stack_entry_for_nrec(n - nrecs))) >= 0)
			stack_remove_at(pos);
		free_nrec(n - nrecs, false);
	}
	else
	{
		OtelSlot   *slot = slot_for_ref(s);

		if (slot == NULL)
			return;
		if ((pos = stack_find(stack_entry_for_slot(slot - slots))) >= 0)
			stack_remove_at(pos);
		release_slot(slot - slots, false);
	}
	otel_counters.spans_discarded++;
}

/*
 * Push a .detached, still-open span onto the active stack.  Refused
 * (OTEL_ACTIVATION_NONE, counted) for a stale handle, a non-.detached
 * span (already on the stack from otel_span_start()), a span that is
 * already active, or a full stack.  The encoding mirrors make_ref():
 * an activation token and the span's own handle name the same (gen,
 * idx) pair, which is enough to find it again in otel_span_deactivate()
 * and to detect a stale token the same way slot_for_ref()/nrec_for_ref()
 * do for a span handle.
 */
static OtelActivation
api_span_activate(OtelSpanRef s)
{
	int			idx;
	int32		entry;
	uint32		gen;
	bool		recording;

	if (call_refused(false) || s.v == 0)
		return OTEL_ACTIVATION_NONE;

	if (s.v > 0)
	{
		OtelSlot   *slot = slot_for_ref(s);

		if (slot == NULL)
			return OTEL_ACTIVATION_NONE;
		if (!slot->detached)
		{
			otel_counters.activate_not_detached++;
			Assert(false);		/* only a .detached span may be activated */
			return OTEL_ACTIVATION_NONE;
		}
		idx = slot - slots;
		entry = stack_entry_for_slot(idx);
		gen = slot->gen;
		recording = true;
	}
	else
	{
		OtelNrec   *n = nrec_for_ref(s);

		if (n == NULL)
			return OTEL_ACTIVATION_NONE;
		if (!n->detached)
		{
			otel_counters.activate_not_detached++;
			Assert(false);
			return OTEL_ACTIVATION_NONE;
		}
		idx = n - nrecs;
		entry = stack_entry_for_nrec(idx);
		gen = n->gen;
		recording = false;
	}

	if (stack_find(entry) >= 0)
	{
		otel_counters.activate_already_active++;
		Assert(false);			/* span is already active */
		return OTEL_ACTIVATION_NONE;
	}
	if (span_stack_depth >= OTEL_MAX_STACK_DEPTH)
	{
		otel_counters.start_stack_full++;
		return OTEL_ACTIVATION_NONE;
	}

	stack_push(entry, true, GetCurrentTransactionNestLevel());
	return (OtelActivation) {make_ref(gen, idx, recording).v};
}

/*
 * Pop an activation.  Checked exactly like otel_span_end(): entries
 * pushed above it since are unwound first (LIFO violation, WARNING,
 * Assert in cassert builds), same as ending a span out of order.  A
 * stale token (the span already ended, or this token already
 * deactivated) is a quiet no-op via slot_for_ref()/nrec_for_ref(),
 * except that a token whose span is still open but no longer on the
 * stack (a double deactivate) is its own counted misuse, since neither
 * of those helpers can tell that case apart from a fresh stale handle.
 */
static void
api_span_deactivate(OtelActivation tok)
{
	OtelSpanRef s = {tok.v};
	int32		entry;
	int			pos;

	if (call_refused(false) || tok.v == 0)
		return;

	if (s.v > 0)
	{
		OtelSlot   *slot = slot_for_ref(s);

		if (slot == NULL)
			return;				/* stale: already ended */
		entry = stack_entry_for_slot(slot - slots);
	}
	else
	{
		OtelNrec   *n = nrec_for_ref(s);

		if (n == NULL)
			return;				/* stale: already ended */
		entry = stack_entry_for_nrec(n - nrecs);
	}

	pos = stack_find(entry);
	if (pos < 0)
	{
		otel_counters.deactivate_not_active++;
		Assert(false);			/* not currently active (double deactivate?) */
		return;
	}
	if (pos != span_stack_depth - 1)
	{
		nonlifo_warning("span deactivated with spans still open above it", pos);
		stack_unwind_above(pos, "parent activation deactivated first");
	}
	span_stack_depth--;
}

const OtelProducerApi otel_producer_api_table = {
	.version = OTEL_PRODUCER_API_VERSION,
	.struct_size = sizeof(OtelProducerApi),
	.recording_possible = &otel_recording_possible_flag,
	.span_start = api_span_start,
	.span_end = api_span_end,
	.span_set_str = api_span_set_str,
	.span_set_int = api_span_set_int,
	.span_set_double = api_span_set_double,
	.span_set_bool = api_span_set_bool,
	.span_set_vprintf = api_span_set_vprintf,
	.span_set_name = api_span_set_name,
	.span_set_status = api_span_set_status,
	.span_add_event = api_span_add_event,
	.span_add_link = api_span_add_link,
	.span_capture_error = api_span_capture_error,
	.span_record_error = api_span_record_error,
	.span_current = api_span_current,
	.span_context_of = api_span_context_of,
	.resource_add = api_resource_add,
	.span_discard = api_span_discard,
	.span_activate = api_span_activate,
	.span_deactivate = api_span_deactivate,
};


/* ----------------------------------------------------------------
 * JSON log emitter (otel_api.emit_spans_to_log)
 * ---------------------------------------------------------------- */

static void
append_json_attrs(StringInfo buf, const OtelAttribute *attrs, int n)
{
	appendStringInfoChar(buf, '{');
	for (int i = 0; i < n; i++)
	{
		if (i > 0)
			appendStringInfoChar(buf, ',');
		escape_json(buf, attrs[i].key);
		appendStringInfoChar(buf, ':');
		switch (attrs[i].type)
		{
			case OTEL_ATTR_STRING:
				if (attrs[i].v.s)
					escape_json(buf, attrs[i].v.s);
				else
					appendStringInfoString(buf, "null");
				break;
			case OTEL_ATTR_INT:
				appendStringInfo(buf, INT64_FORMAT, attrs[i].v.i);
				break;
			case OTEL_ATTR_DOUBLE:
				appendStringInfo(buf, "%.17g", attrs[i].v.d);
				break;
			case OTEL_ATTR_BOOL:
				appendStringInfoString(buf, attrs[i].v.b ? "true" : "false");
				break;
		}
	}
	appendStringInfoChar(buf, '}');
}

void
otel_emit_span_as_log_line(const OtelSpan *span)
{
	StringInfoData buf;
	char		tid[OTEL_TRACE_ID_HEX_LEN + 1];
	char		sid[OTEL_SPAN_ID_HEX_LEN + 1];
	char		pid[OTEL_SPAN_ID_HEX_LEN + 1];

	otel_trace_id_to_hex(&span->trace_id, tid);
	otel_span_id_to_hex(&span->span_id, sid);
	otel_span_id_to_hex(&span->parent_span_id, pid);

	initStringInfo(&buf);
	appendStringInfo(&buf, "{\"trace_id\":\"%s\",\"span_id\":\"%s\",\"parent_span_id\":\"%s\","
					 "\"trace_flags\":\"%02x\",\"name\":",
					 tid, sid, otel_span_id_is_valid(&span->parent_span_id) ? pid : "",
					 span->trace_flags);
	escape_json(&buf, span->name);
	appendStringInfo(&buf, ",\"kind\":%d,\"status\":%d", (int) span->kind, (int) span->status);
	if (span->status_description)
	{
		appendStringInfoString(&buf, ",\"status_description\":");
		escape_json(&buf, span->status_description);
	}
	if (span->scope && span->scope->name)
	{
		appendStringInfoString(&buf, ",\"scope\":");
		escape_json(&buf, span->scope->name);
	}
	appendStringInfo(&buf, ",\"start_time\":\"%s\"", timestamptz_to_str(span->start_time));
	appendStringInfo(&buf, ",\"end_time\":\"%s\"", timestamptz_to_str(span->end_time));
	appendStringInfoString(&buf, ",\"attributes\":");
	append_json_attrs(&buf, span->attrs, span->n_attrs);
	appendStringInfoString(&buf, ",\"events\":[");
	for (int i = 0; i < span->n_events; i++)
	{
		if (i > 0)
			appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, "{\"name\":");
		escape_json(&buf, span->events[i].name);
		appendStringInfo(&buf, ",\"time\":\"%s\",\"attributes\":",
						 timestamptz_to_str(span->events[i].time));
		append_json_attrs(&buf, span->events[i].attrs, span->events[i].n_attrs);
		appendStringInfoChar(&buf, '}');
	}
	appendStringInfoString(&buf, "],\"links\":[");
	for (int i = 0; i < span->n_links; i++)
	{
		char		ltid[OTEL_TRACE_ID_HEX_LEN + 1];
		char		lsid[OTEL_SPAN_ID_HEX_LEN + 1];

		otel_trace_id_to_hex(&span->links[i].trace_id, ltid);
		otel_span_id_to_hex(&span->links[i].span_id, lsid);
		appendStringInfo(&buf, "%s{\"trace_id\":\"%s\",\"span_id\":\"%s\"}",
						 i > 0 ? "," : "", ltid, lsid);
	}
	appendStringInfo(&buf, "],\"dropped_attributes\":%u,\"dropped_events\":%u,\"dropped_links\":%u}",
					 span->dropped_attrs, span->dropped_events, span->dropped_links);

	ereport(LOG, errmsg_internal("otel-span: %s", buf.data));
	pfree(buf.data);
}


/* ----------------------------------------------------------------
 * SQL-callable
 * ---------------------------------------------------------------- */

PG_FUNCTION_INFO_V1(otel_api_counters);

/* otel_api_counters() RETURNS TABLE (name text, value bigint) */
Datum
otel_api_counters(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	static const struct
	{
		const char *name;
		size_t		off;
	}			fields[] = {
#define F(f) {#f, offsetof(OtelApiCounters, f)}
		F(spans_started), F(spans_unsampled), F(spans_emitted), F(spans_discarded),
		F(start_no_slot), F(start_no_session_slot), F(start_stack_full),
		F(in_crit_section), F(in_emit_hook), F(start_bad_args),
		F(stale_handle), F(non_lifo_end), F(unwound),
		F(leaked_at_commit), F(dropped_in_dispatch), F(open_at_exit),
		F(attr_truncated), F(attr_dropped), F(event_dropped), F(link_dropped),
		F(error_capture_failed), F(emit_hook_errors),
		F(activate_not_detached), F(activate_already_active), F(deactivate_not_active),
		F(activation_unwound), F(activation_leaked_at_commit),
#undef F
	};

	InitMaterializedSRF(fcinfo, 0);
	for (int i = 0; i < lengthof(fields); i++)
	{
		Datum		values[2];
		bool		nulls[2] = {false, false};

		values[0] = CStringGetTextDatum(fields[i].name);
		values[1] = Int64GetDatum((int64) *(const uint64 *)
								  ((const char *) &otel_counters + fields[i].off));
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	return (Datum) 0;
}


void
otel_producer_init(void)
{
	prev_emit_log_hook = emit_log_hook;
	emit_log_hook = otel_emit_log_hook;
	RegisterXactCallback(otel_xact_callback, NULL);
	RegisterSubXactCallback(otel_subxact_callback, NULL);
}
