/*-------------------------------------------------------------------------
 *
 * otel_parallel.c
 *	  Parallel-worker trace-context propagation for contrib/otel.
 *
 * The leader publishes its currently-active span's identity into a
 * per-backend slot in a fixed-size shared-memory array sized to
 * MaxBackends.  Parallel workers, on their first executor entry,
 * read their leader's slot (indexed by ParallelLeaderProcNumber)
 * and use the leader's span_id as parent_span_id.
 *
 * Why a dedicated shared-memory array rather than the otel_api.traceparent
 * GUC system?  The GUCs are intentionally scoped as the client->server
 * entry point ONLY --- they hold what the client supplied via
 * SET / SET LOCAL or the 'M' protocol header, and must not be
 * mutated by in-backend nesting state.  Earlier versions of
 * contrib/otel rode the GUC propagation machinery (RestoreGUCState)
 * via a dedicated otel.current_span_id GUC; that conflated wire-
 * supplied context with in-backend state and was removed in the
 * Phase 3 split refactor.  This file is its replacement.
 *
 * Memory layout: one slot per backend, indexed by ProcNumber.  Each
 * slot has a spinlock + boolean "set" flag + the SpanContext fields.
 * Spinlocks are uncontended in practice --- each slot has exactly
 * one writer (the backend that owns the slot) and zero or more
 * readers (parallel workers attributing to that backend), and the
 * write happens long before workers read (start_span fires at
 * ExecutorStart_hook, well before parallel workers are launched).
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/otel/otel_parallel.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/procnumber.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/elog.h"

#include "otel.h"
#include "otel_internal.h"


/*
 * Per-backend slot in the parallel-context array.
 *
 * `lock` is a spinlock guarding the rest of the struct.
 * `set` is true when the backend that owns this slot currently has
 * an active span whose identity has been published.
 *
 * trace_id / parent_span_id / trace_flags carry the leader's
 * SpanContext.  parent_span_id is named "parent" from the worker's
 * point of view --- it's what a worker should use as its own
 * parent_span_id when starting a span.  From the leader's point of
 * view this is the leader's current span_id.
 */
typedef struct OtelParallelContextSlot
{
	slock_t		lock;
	bool		set;
	char		trace_id[OTEL_TRACE_ID_LEN + 1];
	char		parent_span_id[OTEL_SPAN_ID_LEN + 1];
	char		trace_flags[OTEL_TRACE_FLAGS_LEN + 1];
} OtelParallelContextSlot;


/*
 * The shared slot array.  Pointer is set at shmem-startup time and
 * never changes thereafter.  NULL means contrib/otel was not loaded
 * in shared_preload_libraries (the shmem hooks didn't fire); the
 * parallel-context propagation just no-ops in that case --- it's
 * the same situation as if the leader simply had no active span.
 */
static OtelParallelContextSlot *otel_parallel_slots = NULL;


/* Previous hook chain pointers --- we always chain. */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;


/*
 * Size of the shared array.  Indexed by ProcNumber (0..MaxBackends-1).
 */
static inline Size
otel_parallel_shmem_size(void)
{
	return mul_size(MaxBackends, sizeof(OtelParallelContextSlot));
}


/* shmem_request hook --- runs at postmaster startup before
 * shared memory is sized. */
static void
otel_parallel_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
	RequestAddinShmemSpace(otel_parallel_shmem_size());
}


/* shmem_startup hook --- runs at postmaster startup AFTER shared
 * memory is allocated.  Locates / allocates our slot array. */
static void
otel_parallel_shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	otel_parallel_slots = (OtelParallelContextSlot *)
		ShmemInitStruct("contrib/otel parallel-context slots",
						otel_parallel_shmem_size(),
						&found);
	if (!found)
	{
		Size		i;

		/* First attach: zero everything and initialise spinlocks. */
		memset(otel_parallel_slots, 0, otel_parallel_shmem_size());
		for (i = 0; i < (Size) MaxBackends; i++)
			SpinLockInit(&otel_parallel_slots[i].lock);
	}
	LWLockRelease(AddinShmemInitLock);
}


/*
 * Register the shmem hooks.  Called from _PG_init.  Must run in the
 * postmaster (i.e. via shared_preload_libraries); ereport's WARNING
 * and falls through if invoked outside the preload context, since
 * RequestAddinShmemSpace only works pre-fork.
 */
void
otel_parallel_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(WARNING,
				(errmsg("contrib/otel parallel-context propagation requires shared_preload_libraries"),
				 errhint("Add 'otel' to shared_preload_libraries to enable parallel-worker span linkage.")));
		return;
	}
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = otel_parallel_shmem_request;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = otel_parallel_shmem_startup;
}


/*
 * otel_parallel_publish_leader_context --- the leader calls this
 * when it starts a span, to publish its identity so parallel
 * workers spawned later in this session can attribute to it.
 *
 * Caller passes the active span's trace_id / span_id / trace_flags
 * as NUL-terminated lowercase-hex strings (same representation as
 * OtelSpan and the OtelContext root state).  NULL or empty
 * trace_id is treated as "clear the slot".
 *
 * No-op if shared memory wasn't initialised (otel not preloaded).
 */
void
otel_parallel_publish_leader_context(const char *trace_id,
									 const char *span_id,
									 const char *trace_flags)
{
	OtelParallelContextSlot *slot;

	if (otel_parallel_slots == NULL)
		return;
	if (MyProcNumber < 0 || MyProcNumber >= MaxBackends)
		return;

	slot = &otel_parallel_slots[MyProcNumber];

	SpinLockAcquire(&slot->lock);
	if (trace_id == NULL || trace_id[0] == '\0' ||
		span_id == NULL || span_id[0] == '\0')
	{
		slot->set = false;
		slot->trace_id[0] = '\0';
		slot->parent_span_id[0] = '\0';
		slot->trace_flags[0] = '\0';
	}
	else
	{
		strlcpy(slot->trace_id, trace_id, sizeof(slot->trace_id));
		strlcpy(slot->parent_span_id, span_id, sizeof(slot->parent_span_id));
		strlcpy(slot->trace_flags,
				(trace_flags && trace_flags[0]) ? trace_flags : "00",
				sizeof(slot->trace_flags));
		slot->set = true;
	}
	SpinLockRelease(&slot->lock);
}


/*
 * otel_parallel_clear_leader_context --- the leader calls this when
 * it finishes a span, to mark its slot empty so any workers
 * spawned in a future query don't read a stale value.
 */
void
otel_parallel_clear_leader_context(void)
{
	OtelParallelContextSlot *slot;

	if (otel_parallel_slots == NULL)
		return;
	if (MyProcNumber < 0 || MyProcNumber >= MaxBackends)
		return;

	slot = &otel_parallel_slots[MyProcNumber];

	SpinLockAcquire(&slot->lock);
	slot->set = false;
	slot->trace_id[0] = '\0';
	slot->parent_span_id[0] = '\0';
	slot->trace_flags[0] = '\0';
	SpinLockRelease(&slot->lock);
}


/*
 * otel_parallel_get_leader_context --- workers call this when they
 * start a span, to find the leader's published SpanContext.
 *
 * Returns true and fills *out if we're a parallel worker AND the
 * leader's slot is populated.  Returns false otherwise (we're not
 * a worker, or the leader hasn't published, or the slot is empty).
 *
 * No-op if shared memory wasn't initialised.
 */
bool
otel_parallel_get_leader_context(OtelParallelContext *out)
{
	OtelParallelContextSlot *slot;
	ProcNumber	leader;

	if (out == NULL)
		return false;
	if (otel_parallel_slots == NULL)
		return false;

	leader = ParallelLeaderProcNumber;
	if (leader == INVALID_PROC_NUMBER)
		return false;					/* we are not a worker */
	if (leader < 0 || leader >= MaxBackends)
		return false;

	slot = &otel_parallel_slots[leader];

	SpinLockAcquire(&slot->lock);
	if (!slot->set)
	{
		SpinLockRelease(&slot->lock);
		return false;
	}
	out->version = OTEL_PARALLEL_CONTEXT_V1;
	strlcpy(out->trace_id, slot->trace_id, sizeof(out->trace_id));
	strlcpy(out->parent_span_id, slot->parent_span_id, sizeof(out->parent_span_id));
	strlcpy(out->trace_flags, slot->trace_flags, sizeof(out->trace_flags));
	SpinLockRelease(&slot->lock);
	return true;
}
