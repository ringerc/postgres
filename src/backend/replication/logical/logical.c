/*-------------------------------------------------------------------------
 *
 * logical.c
 *
 *	   Logical decoding (aka changeset extraction) infrastructure
 *
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/logical.c
 *
 */

#include "postgres.h"

#include <unistd.h>
#include <sys/stat.h>

#include "fmgr.h"
#include "miscadmin.h"

#include "access/transam.h"

#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/reorderbuffer.h"
#include "replication/snapbuild.h"

#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/fd.h"

#include "utils/memutils.h"
#include "utils/syscache.h"

/*
 * Set the xmin required for decoding snapshots for the specific decoding
 * slot.
 */
void
IncreaseLogicalXminForSlot(XLogRecPtr lsn, TransactionId xmin)
{
	bool	updated_xmin = false;

	Assert(MyReplicationSlot != NULL);

	SpinLockAcquire(&MyReplicationSlot->mutex);

	/*
	 * don't overwrite if we already have a newer xmin. This can
	 * happen if we restart decoding in a slot.
	 */
	if (TransactionIdPrecedesOrEquals(xmin, MyReplicationSlot->catalog_xmin))
	{
	}
	/*
	 * If the client has already confirmed up to this lsn, we directly
	 * can mark this as accepted. This can happen if we restart
	 * decoding in a slot.
	 */
	else if (lsn <= MyReplicationSlot->confirmed_flush)
	{
		MyReplicationSlot->candidate_catalog_xmin = xmin;
		MyReplicationSlot->candidate_xmin_lsn = lsn;

		/* our candidate can directly be used */
		updated_xmin = true;
	}
	/*
	 * Only increase if the previous values have been applied, otherwise we
	 * might never end up updating if the receiver acks too slowly.
	 */
	else if (MyReplicationSlot->candidate_xmin_lsn == InvalidXLogRecPtr)
	{
		MyReplicationSlot->candidate_catalog_xmin = xmin;
		MyReplicationSlot->candidate_xmin_lsn = lsn;
		elog(DEBUG1, "got new xmin %u at %X/%X", xmin,
			 (uint32) (lsn >> 32), (uint32) lsn);
	}
	SpinLockRelease(&MyReplicationSlot->mutex);

	/* candidate already valid with the current flush position, apply */
	if (updated_xmin)
		LogicalConfirmReceivedLocation(MyReplicationSlot->confirmed_flush);
}

/*
 * Mark the minimal LSN (restart_lsn) we need to read to replay all
 * transactions that have not yet committed at current_lsn. Only takes
 * effect when the client has confirmed to have received current_lsn.
 */
void
IncreaseRestartDecodingForSlot(XLogRecPtr current_lsn, XLogRecPtr restart_lsn)
{
	bool	updated_lsn = false;

	Assert(MyReplicationSlot != NULL);
	Assert(restart_lsn != InvalidXLogRecPtr);
	Assert(current_lsn != InvalidXLogRecPtr);

	SpinLockAcquire(&MyReplicationSlot->mutex);

	/* don't overwrite if have a newer restart lsn*/
	if (restart_lsn <= MyReplicationSlot->restart_decoding)
	{
	}
	/*
	 * We might have already flushed far enough to directly accept this lsn, in
	 * this case there is no need to check for existing candidate LSNs
	 */
	else if (current_lsn <= MyReplicationSlot->confirmed_flush)
	{
		MyReplicationSlot->candidate_restart_valid = current_lsn;
		MyReplicationSlot->candidate_restart_decoding = restart_lsn;

		/* our candidate can directly be used */
		updated_lsn = true;
	}
	/*
	 * Only increase if the previous values have been applied, otherwise we
	 * might never end up updating if the receiver acks too slowly. A missed
	 * value here will just cause some extra effort after reconnecting.
	 */
	if (MyReplicationSlot->candidate_restart_valid == InvalidXLogRecPtr)
	{
		MyReplicationSlot->candidate_restart_valid = current_lsn;
		MyReplicationSlot->candidate_restart_decoding = restart_lsn;

		elog(DEBUG1, "got new restart lsn %X/%X at %X/%X",
			 (uint32) (restart_lsn >> 32), (uint32) restart_lsn,
			 (uint32) (current_lsn >> 32), (uint32) current_lsn);
	}
	else
	{
		elog(DEBUG1, "failed to increase restart lsn: proposed %X/%X, after %X/%X, current candidate %X/%X, current after %X/%X, flushed up to %X/%X",
			 (uint32) (restart_lsn >> 32), (uint32) restart_lsn,
			 (uint32) (current_lsn >> 32), (uint32) current_lsn,
			 (uint32) (MyReplicationSlot->candidate_restart_decoding >> 32),
			 (uint32) MyReplicationSlot->candidate_restart_decoding,
			 (uint32) (MyReplicationSlot->candidate_restart_valid >> 32),
			 (uint32) MyReplicationSlot->candidate_restart_valid,
			 (uint32) (MyReplicationSlot->confirmed_flush >> 32),
			 (uint32) MyReplicationSlot->confirmed_flush
			);
	}
	SpinLockRelease(&MyReplicationSlot->mutex);

	/* candidates are already valid with the current flush position, apply */
	if (updated_lsn)
		LogicalConfirmReceivedLocation(MyReplicationSlot->confirmed_flush);
}

void
LogicalConfirmReceivedLocation(XLogRecPtr lsn)
{
	Assert(lsn != InvalidXLogRecPtr);

	/* Do an unlocked check for candidate_lsn first. */
	if (MyReplicationSlot->candidate_xmin_lsn != InvalidXLogRecPtr ||
		MyReplicationSlot->candidate_restart_valid != InvalidXLogRecPtr)
	{
		bool		updated_xmin = false;
		bool		updated_restart = false;

		/* use volatile pointer to prevent code rearrangement */
		volatile ReplicationSlot *slot = MyReplicationSlot;

		SpinLockAcquire(&slot->mutex);

		slot->confirmed_flush = lsn;

		/* if were past the location required for bumping xmin, do so */
		if (slot->candidate_xmin_lsn != InvalidXLogRecPtr &&
			slot->candidate_xmin_lsn <= lsn)
		{
			/*
			 * We have to write the changed xmin to disk *before* we change
			 * the in-memory value, otherwise after a crash we wouldn't know
			 * that some catalog tuples might have been removed already.
			 *
			 * Ensure that by first writing to ->xmin and only update
			 * ->effective_xmin once the new state is synced to disk. After a
			 * crash ->effective_xmin is set to ->xmin.
			 */
			if (TransactionIdIsValid(slot->candidate_catalog_xmin) &&
				slot->catalog_xmin != slot->candidate_catalog_xmin)
			{
				slot->catalog_xmin = slot->candidate_catalog_xmin;
				slot->candidate_catalog_xmin = InvalidTransactionId;
				slot->candidate_xmin_lsn = InvalidXLogRecPtr;
				updated_xmin = true;
			}
		}

		if (slot->candidate_restart_valid != InvalidXLogRecPtr &&
			slot->candidate_restart_valid <= lsn)
		{
			Assert(slot->candidate_restart_decoding != InvalidXLogRecPtr);

			slot->restart_decoding = slot->candidate_restart_decoding;
			slot->candidate_restart_decoding = InvalidXLogRecPtr;
			slot->candidate_restart_valid = InvalidXLogRecPtr;
			updated_restart = true;
		}

		SpinLockRelease(&slot->mutex);

		/* first write new xmin to disk, so we know whats up after a crash */
		if (updated_xmin || updated_restart)
		{
			/* cast away volatile, thats ok. */
			ReplicationSlotSave();
			elog(DEBUG1, "updated xmin: %u restart: %u", updated_xmin, updated_restart);
		}
		/*
		 * now the new xmin is safely on disk, we can let the global value
		 * advance
		 */
		if (updated_xmin)
		{
			SpinLockAcquire(&slot->mutex);
			slot->effective_catalog_xmin = slot->catalog_xmin;
			SpinLockRelease(&slot->mutex);

			ReplicationSlotsComputeRequiredXmin();
		}
	}
	else
	{
		volatile ReplicationSlot *slot = MyReplicationSlot;

		SpinLockAcquire(&slot->mutex);
		slot->confirmed_flush = lsn;
		SpinLockRelease(&slot->mutex);
	}
}

/*
 * Compute the oldest WAL LSN we need to be able to read to be able to continue
 * decoding all slots.
 *
 * Returns InvalidXLogRecPtr if logical decoding is disabled or there are no
 * active slots including the case where wal_level < logical.
 */
XLogRecPtr
ComputeLogicalRestartLSN(void)
{
	XLogRecPtr	result = InvalidXLogRecPtr;
	int			i;

	if (max_replication_slots <= 0)
		return InvalidXLogRecPtr;

	LWLockAcquire(ReplicationSlotCtlLock, LW_SHARED);

	for (i = 0; i < max_replication_slots; i++)
	{
		volatile ReplicationSlot *s;
		XLogRecPtr		restart_decoding;

		s = &ReplicationSlotCtl->replication_slots[i];

		/* cannot change while ReplicationSlotCtlLock is held */
		if (!s->in_use)
			continue;

		/* read once, it's ok if it increases while we're checking */
		SpinLockAcquire(&s->mutex);
		restart_decoding = s->restart_decoding;
		SpinLockRelease(&s->mutex);

		if (result == InvalidXLogRecPtr ||
			restart_decoding < result)
			result = restart_decoding;
	}

	LWLockRelease(ReplicationSlotCtlLock);

	return result;
}

/*
 * Make sure the current settings & environment are capable of doing logical
 * changeset extraction.
 */
void
CheckLogicalDecodingRequirements(void)
{
	CheckSlotRequirements();

	if (wal_level < WAL_LEVEL_LOGICAL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("changeset extraction requires wal_level >= logical")));

	if (MyDatabaseId == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("changeset extraction requires to be connected to a database")));
}

/*
 * LogicalDecodingCountDBSlots -- count number of slots referring to the given DB
 *
 * Returns true if there are any slots referencing the
 * database. *nslots will be set to the absolute number of slots in
 * the database, *nactive to ones currently active.
 */
bool
LogicalDecodingCountDBSlots(Oid dboid, int *nslots, int *nactive)
{
	int			i;

	*nslots = *nactive = 0;

	if (max_replication_slots <= 0)
		return false;

	LWLockAcquire(ReplicationSlotCtlLock, LW_SHARED);
	for (i = 0; i < max_replication_slots; i++)
	{
		volatile ReplicationSlot *s;

		s = &ReplicationSlotCtl->replication_slots[i];

		/* cannot change while ReplicationSlotCtlLock is held */
		if (!s->in_use)
			continue;

		/* not our database, don't count */
		if (s->database != dboid)
			continue;

		/* count slots with spinlock held */
		SpinLockAcquire(&s->mutex);
		(*nslots)++;
		if (s->active)
			(*nactive)++;
		SpinLockRelease(&s->mutex);
	}
	LWLockRelease(ReplicationSlotCtlLock);

	if (*nslots > 0)
		return true;
	return false;
}

static void
LoadOutputPlugin(OutputPluginCallbacks *callbacks, char *plugin)
{
	LogicalOutputPluginInit plugin_init;

	plugin_init = (LogicalOutputPluginInit)
		load_external_function(plugin, "_PG_output_plugin_init", false, NULL);

	if (plugin_init == NULL)
		elog(ERROR, "output plugins have to declare the _PG_output_plugin_init symbol");

	plugin_init(callbacks);

	if (callbacks->begin_cb == NULL)
		elog(ERROR, "output plugins have to register a begin callback");
	if (callbacks->change_cb == NULL)
		elog(ERROR, "output plugins have to register a change callback");
	if (callbacks->commit_cb == NULL)
		elog(ERROR, "output plugins have to register a commit callback");
}

/*
 * Context management functions to coordinate between the different logical
 * decoding pieces.
 */

typedef struct LogicalErrorCallbackState
{
	LogicalDecodingContext *ctx;
	const char *callback;
} LogicalErrorCallbackState;

static void
output_plugin_error_callback(void *arg)
{
	LogicalErrorCallbackState *state = (LogicalErrorCallbackState *) arg;
	/* XXX: Add the current LSN? */
	errcontext("slot \"%s\", output plugin \"%s\" during the %s callback",
			   NameStr(state->ctx->slot->name),
			   NameStr(state->ctx->slot->plugin),
			   state->callback);
}

static void
startup_slot_wrapper(LogicalDecodingContext *ctx, bool is_init)
{
	LogicalErrorCallbackState state;
	ErrorContextCallback errcallback;

	/* Push callback + info on the error context stack */
	state.ctx = ctx;
	state.callback = "pg_decode_startup";
	errcallback.callback = output_plugin_error_callback;
	errcallback.arg = (void *) &state;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/* set output state */
	ctx->accept_writes = false;

	/* do the actual work: call callback */
	ctx->callbacks.startup_cb(ctx, is_init);

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;
}

static void
shutdown_slot_wrapper(LogicalDecodingContext *ctx)
{
	LogicalErrorCallbackState state;
	ErrorContextCallback errcallback;

	/* Push callback + info on the error context stack */
	state.ctx = ctx;
	state.callback = "pg_decode_shutdown";
	errcallback.callback = output_plugin_error_callback;
	errcallback.arg = (void *) &state;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/* set output state */
	ctx->accept_writes = false;

	/* do the actual work: call callback */
	ctx->callbacks.shutdown_cb(ctx);

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;
}


/*
 * Callbacks for ReorderBuffer which add in some more information and then call
 * output_plugin.h plugins.
 */
static void
begin_txn_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn)
{
	LogicalDecodingContext *ctx = cache->private_data;
	LogicalErrorCallbackState state;
	ErrorContextCallback errcallback;

	/* Push callback + info on the error context stack */
	state.ctx = ctx;
	state.callback = "pg_decode_begin_txn";
	errcallback.callback = output_plugin_error_callback;
	errcallback.arg = (void *) &state;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/* set output state */
	ctx->accept_writes = true;
	ctx->write_xid = txn->xid;
	ctx->write_location = txn->first_lsn;

	/* do the actual work: call callback */
	ctx->callbacks.begin_cb(ctx, txn);

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;
}

static void
commit_txn_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn, XLogRecPtr commit_lsn)
{
	LogicalDecodingContext *ctx = cache->private_data;
	LogicalErrorCallbackState state;
	ErrorContextCallback errcallback;

	/* Push callback + info on the error context stack */
	state.ctx = ctx;
	state.callback = "pg_decode_commit_txn";
	errcallback.callback = output_plugin_error_callback;
	errcallback.arg = (void *) &state;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/* set output state */
	ctx->accept_writes = true;
	ctx->write_xid = txn->xid;
	ctx->write_location = txn->end_lsn;

	/* do the actual work: call callback */
	ctx->callbacks.commit_cb(ctx, txn, commit_lsn);

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;
}

static void
change_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
			   Relation relation, ReorderBufferChange *change)
{
	LogicalDecodingContext *ctx = cache->private_data;
	LogicalErrorCallbackState state;
	ErrorContextCallback errcallback;

	/* Push callback + info on the error context stack */
	state.ctx = ctx;
	state.callback = "pg_decode_change";
	errcallback.callback = output_plugin_error_callback;
	errcallback.arg = (void *) &state;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/* set output state */
	ctx->accept_writes = true;
	ctx->write_xid = txn->xid;
	ctx->write_location = txn->end_lsn;

	ctx->callbacks.change_cb(ctx, txn, relation, change);

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;
}

/*
 * Prepare a write using the context's output routine.
 */
void
OutputPluginPrepareWrite(struct LogicalDecodingContext *ctx, bool last_write)
{
	if (!ctx->accept_writes)
		elog(ERROR, "writes are only accepted in commit, begin and change callbacks");

	ctx->prepare_write(ctx, ctx->write_location, ctx->write_xid, last_write);
	ctx->prepared_write = true;
}

/*
 * Perform a write using the context's output routine.
 */
void
OutputPluginWrite(struct LogicalDecodingContext *ctx, bool last_write)
{
	if (!ctx->prepared_write)
		elog(ERROR, "OutputPluginPrepareWrite needs to be called before OutputPluginWrite");

	ctx->write(ctx, ctx->write_location, ctx->write_xid, last_write);
	ctx->prepared_write = false;
}

/*
 * Allocate a new decoding context.
 *
 * is_init denotes whether the slot is newly initialized
 * plugin sets the output plugin used when initializing the slot
 * output_plugin_options contains options passed to the output plugin
 * read_page, prepare_write, do_write are callbacks that have to be filled to
 *		perform the use-case dependent, actual, work.
 *
 * Returns a usable output plugin after calling the output plugins startup
 * function.
 */
LogicalDecodingContext *
CreateDecodingContext(bool is_init,
					  char *plugin,
					  XLogRecPtr start_lsn,
					  List *output_plugin_options,
					  XLogPageReadCB read_page,
					  LogicalOutputPluginWriterPrepareWrite prepare_write,
					  LogicalOutputPluginWriterWrite do_write)
{
	MemoryContext context;
	MemoryContext old_context;
	TransactionId xmin_horizon;
	LogicalDecodingContext *ctx;
	ReplicationSlot *slot;

	if (MyReplicationSlot == NULL)
		elog(ERROR, "need a current slot");

	if (is_init && start_lsn != InvalidXLogRecPtr)
		elog(ERROR, "Cannot INIT_LOGICAL_REPLICATION at a specified LSN");

	if (is_init && plugin == NULL)
		elog(ERROR, "Cannot INIT_LOGICAL_REPLICATION without a specified plugin");

	if (MyReplicationSlot->database == InvalidOid)
		elog(ERROR, "slots for changeset extraction need to be database specific");

	if (MyReplicationSlot->database != MyDatabaseId)
		elog(ERROR, "slot for the wrong database");

	/* shorter lines... */
	slot = MyReplicationSlot;

	context = AllocSetContextCreate(CurrentMemoryContext,
									"Changeset Extraction Context",
									ALLOCSET_DEFAULT_MINSIZE,
									ALLOCSET_DEFAULT_INITSIZE,
									ALLOCSET_DEFAULT_MAXSIZE);
	old_context = MemoryContextSwitchTo(context);
	ctx = palloc0(sizeof(LogicalDecodingContext));

	ctx->context = context;

	/* If we're initializing a new slot, there's a bit more to do */
	if (is_init)
	{
		/* register output plugin name with slot */
		strncpy(NameStr(MyReplicationSlot->plugin), plugin,
				NAMEDATALEN);
		NameStr(MyReplicationSlot->plugin)[NAMEDATALEN - 1] = '\0';

		/*
		 * Lets start with enough information if we can, so log a standby
		 * snapshot and start decoding at exactl that position.
		 */
		if (!RecoveryInProgress())
		{
			XLogRecPtr flushptr;

			/* start at current insert position*/
			slot->restart_decoding = GetXLogInsertRecPtr();

			/* make sure we have enough information to start */
			flushptr = LogStandbySnapshot();

			/* and make sure it's fsynced to disk */
			XLogFlush(flushptr);
		}
		else
			slot->restart_decoding = GetRedoRecPtr();

		/*
		 * Acquire the current global xmin value and directly set the logical
		 * xmin before releasing the lock if necessary. We do this so wal
		 * decoding is guaranteed to have all catalog rows produced by xacts
		 * with an xid > walsnd->xmin available.
		 *
		 * We can't use ReplicationSlotComputeXmin here as that acquires
		 * ProcArrayLock separately which would open a short window for the
		 * global xmin to advance above our xmin.
		 */
		LWLockAcquire(ProcArrayLock, LW_SHARED);
		slot->effective_catalog_xmin = GetOldestXmin(true, true, true);
		slot->catalog_xmin = slot->effective_catalog_xmin;

		if (!TransactionIdIsValid(ReplicationSlotCtl->catalog_xmin) ||
			NormalTransactionIdPrecedes(slot->effective_catalog_xmin,
										ReplicationSlotCtl->catalog_xmin))
			ReplicationSlotCtl->catalog_xmin = slot->effective_catalog_xmin;
		LWLockRelease(ProcArrayLock);

		Assert(slot->effective_catalog_xmin <= GetOldestXmin(true, true, false));

		xmin_horizon = slot->catalog_xmin;
	}
	else
		xmin_horizon = InvalidTransactionId;

	/* load output plugins, so we detect a wrong output plugin now. */
	LoadOutputPlugin(&ctx->callbacks, NameStr(slot->plugin));

	ctx->slot = slot;

	ctx->reader = XLogReaderAllocate(read_page, ctx);
	ctx->reader->private_data = ctx;

	ctx->reorder = ReorderBufferAllocate();
	ctx->snapshot_builder =
		AllocateSnapshotBuilder(ctx->reorder, xmin_horizon, start_lsn);

	ctx->reorder->private_data = ctx;

	/* wrap output plugin callbacks, so we can add error context information */
	ctx->reorder->begin = begin_txn_wrapper;
	ctx->reorder->apply_change = change_wrapper;
	ctx->reorder->commit = commit_txn_wrapper;

	ctx->out = makeStringInfo();
	ctx->prepare_write = prepare_write;
	ctx->write = do_write;

	ctx->output_plugin_options = output_plugin_options;

	/* call output plugin initialization callback */
	if (ctx->callbacks.startup_cb != NULL)
		startup_slot_wrapper(ctx, is_init);

	MemoryContextSwitchTo(old_context);

	return ctx;
}

/*
 * Read from the decoding slot
 */
void
DecodingContextFindStartpoint(LogicalDecodingContext *ctx)
{
	XLogRecPtr	startptr;

	/* Initialize from where to start reading WAL. */
	startptr = ctx->slot->restart_decoding;

	elog(LOG, "starting to decode from %X/%X",
		 (uint32)(ctx->slot->restart_decoding >> 32),
		 (uint32)ctx->slot->restart_decoding);

	/* Wait for a consistent starting point */
	for (;;)
	{
		XLogRecord *record;
		char	   *err = NULL;

		/*
		 * If the caller requires that interrupts be checked, the read_page
		 * callback should do so, as those will often wait.
		 */

		/* the read_page callback waits for new WAL */
		record = XLogReadRecord(ctx->reader, startptr, &err);
		if (err)
			elog(ERROR, "%s", err);

		Assert(record);

		startptr = InvalidXLogRecPtr;

		DecodeRecordIntoReorderBuffer(ctx, record);

		/* only continue till we found a consistent spot */
		if (DecodingContextReady(ctx))
			break;
	}

	ctx->slot->confirmed_flush = ctx->reader->EndRecPtr;
}

/*
 * Free a previously allocated decoding context, invoking the shutdown
 * callback if necessary.
 */
void
FreeDecodingContext(LogicalDecodingContext *ctx)
{
	if (ctx->callbacks.shutdown_cb != NULL)
		shutdown_slot_wrapper(ctx);

	ReorderBufferFree(ctx->reorder);
	FreeSnapshotBuilder(ctx->snapshot_builder);
	XLogReaderFree(ctx->reader);
	MemoryContextDelete(ctx->context);
}


/*
 * Returns true if an consistent initial decoding snapshot has been built.
 */
bool
DecodingContextReady(LogicalDecodingContext *ctx)
{
	return SnapBuildCurrentState(ctx->snapshot_builder) == SNAPBUILD_CONSISTENT;
}
