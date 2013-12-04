/*-------------------------------------------------------------------------
 *
 * snapbuild.c
 *
 *	  Infrastructure for building catalog snapshots based on the contents of
 *	  the WAL for the purpose of decoding the contents of the WAL.
 *
 * NOTES:
 *
 * We build snapshots which can *only* be used to read catalog contents by
 * reading and interpreting the WAL stream. The aim is to build a snapshot
 * that behaves the same as a freshly taken MVCC snapshot would have at the
 * time the XLogRecord was generated.
 *
 * To build the snapshots we reuse the infrastructure built for Hot
 * Standby. The in-memory snapshots we build look different than HS' because
 * we have different needs. To successfully decode data from the WAL we only
 * need to access catalog tables and (sys|rel|cat)cache, not the actual user
 * tables since the data we decode is contained in the WAL records. Also, our
 * snapshots need to be different in comparison to normal MVCC ones because in
 * contrast to those we cannot fully rely on the clog and pg_subtrans for
 * information about committed transactions because they might commit in the
 * future from the POV of the WAL entry we're currently decoding.
 *
 * As the percentage of transactions modifying the catalog normally is fairly
 * small in comparisons to ones only manipulating user data, we keep track of
 * the committed catalog modifying ones inside (xmin, xmax) instead of keeping
 * track of all running transactions like its done in a normal snapshot. Note
 * that we're generally only looking at transactions that have acquired an
 * xid. That is we keep a list of transactions between snapshot->(xmin, xmax)
 * that we consider committed, everything else is considered aborted/in
 * progress. That also allows us not to care about subtransactions before they
 * have committed which means this modules, in contrast to HS, doesn't have to
 * care about suboverflowed subtransactions and similar.
 *
 * One complexity of doing this is that to e.g. handle mixed DDL/DML
 * transactions we need Snapshots that see intermediate versions of the
 * catalog in a transaction. During normal operation this is achieved by using
 * CommandIds/cmin/cmax. The problem with that however is that for space
 * efficiency reasons only one value of that is stored
 * (c.f. combocid.c). Since ComboCids are only available in memory we log
 * additional information which allows us to get the original (cmin, cmax)
 * pair during visibility checks. Check the reorderbuffer.c's comment above
 * ResolveCminCmaxDuringDecoding() for details.
 *
 * To facilitate all this we need our own visibility routine, as the normal
 * ones are optimized for different usecases. To make sure no unexpected
 * database access bypassing our special snapshot is possible - which would
 * possibly load invalid data into caches - we temporarily overload the
 * .satisfies methods of the usual snapshots while changeset extraction.
 *
 * To replace the normal catalog snapshots with decoding ones use the
 * SetupDecodingSnapshots and RevertFromDecodingSnapshots functions.
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/snapbuild.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "miscadmin.h"

#include "access/heapam_xlog.h"
#include "access/transam.h"
#include "access/xact.h"

#include "replication/logical.h"
#include "replication/reorderbuffer.h"
#include "replication/snapbuild.h"

#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/snapshot.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"

#include "storage/block.h"		/* debugging output */
#include "storage/fd.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/standby.h"

struct SnapBuild
{
	/* how far are we along building our first full snapshot */
	SnapBuildState state;

	/* private memory context used to allocate memory for this module. */
	MemoryContext context;

	/* all transactions < than this have committed/aborted */
	TransactionId xmin;

	/* all transactions >= than this are uncommitted */
	TransactionId xmax;

	/*
	 * Don't replay commits from an LSN <= this LSN. This can be set
	 * externally but it will also be advanced (never retreat) from within
	 * snapbuild.c.
	 */
	XLogRecPtr	transactions_after;

	/*
	 * Don't start decoding WAL until the "xl_running_xacts" information
	 * indicates there are no running xids with a xid smaller than this.
	 */
	TransactionId initial_xmin_horizon;

	/*
	 * Snapshot thats valid to see all currently committed transactions that
	 * see catalog modifications.
	 */
	Snapshot	snapshot;

	/*
	 * LSN of the last location we are sure a snapshot has been serialized to.
	 */
	XLogRecPtr	last_serialized_snapshot;

	ReorderBuffer *reorder;

	/*
	 * Information about initially running transactions
	 *
	 * When we start building a snapshot there already may be transactions in
	 * progress.  Those are stored in running.xip.	We don't have enough
	 * information about those to decode their contents, so until they are
	 * finished (xcnt=0) we cannot switch to a CONSISTENT state.
	 */
	struct
	{
		/*
		 * As long as running.xcnt all XIDs < running.xmin and > running.xmax
		 * have to be checked whether they still are running.
		 */
		TransactionId xmin;
		TransactionId xmax;

		size_t		xcnt;		/* number of used xip entries */
		size_t		xcnt_space; /* allocated size of xip */
		TransactionId *xip;		/* running xacts array, xidComparator-sorted */
	}			running;

	/*
	 * Array of transactions which could have catalog changes that committed
	 * between xmin and xmax
	 */
	struct
	{
		/* number of committed transactions */
		size_t		xcnt;

		/* available space for committed transactions */
		size_t		xcnt_space;

		/*
		 * Until we reach a CONSISTENT state, we record commits of all
		 * transactions, not just the catalog changing ones. Record when that
		 * changes so we know we cannot export a snapshot safely anymore.
		 */
		bool		includes_all_transactions;

		/*
		 * Array of committed transactions that have modified the catalog.
		 *
		 * As this array is frequently modified we do *not* keep it in
		 * xidComparator order. Instead we sort the array when building &
		 * distributing a snapshot.
		 *
		 * XXX: That doesn't seem to be good reasoning anymore. Every time we
		 * add something here after becoming consistent will also require
		 * distributing a snapshot. Storing them sorted would potentially make
		 * it easier to purge as well (but more complicated wrt wraparound?).
		 */
		TransactionId *xip;
	}			committed;
};

/*
 * Starting a transaction -- which we need to do while exporting a snapshot --
 * removes knowledge about the previously used resowner, so we save it here.
 */
ResourceOwner SavedResourceOwnerDuringExport = NULL;
bool ExportInProgress = false;

/* transaction state manipulation functions */
static void SnapBuildEndTxn(SnapBuild *builder, XLogRecPtr lsn, TransactionId xid);

/* ->running manipulation */
static bool SnapBuildTxnIsRunning(SnapBuild *builder, TransactionId xid);

/* ->committed manipulation */
static void SnapBuildPurgeCommittedTxn(SnapBuild *builder);

/* snapshot building/manipulation/distribution functions */
static Snapshot SnapBuildBuildSnapshot(SnapBuild *builder, TransactionId xid);

static void SnapBuildFreeSnapshot(Snapshot snap);

static void SnapBuildSnapIncRefcount(Snapshot snap);

static void SnapBuildDistributeNewCatalogSnapshot(SnapBuild *builder, XLogRecPtr lsn);

/* xlog reading helper functions for SnapBuildProcessRecord */
static bool SnapBuildFindSnapshot(SnapBuild *builder, XLogRecPtr lsn, xl_running_xacts *running);

/* serialization functions */
static void SnapBuildSerialize(SnapBuild *builder, XLogRecPtr lsn);
static bool SnapBuildRestore(SnapBuild *builder, XLogRecPtr lsn);


/*
 * Allocate a new snapshot builder.
 */
SnapBuild *
AllocateSnapshotBuilder(ReorderBuffer *reorder,
						TransactionId xmin_horizon,
						XLogRecPtr start_lsn)
{
	MemoryContext context;
	MemoryContext oldcontext;
	SnapBuild  *builder;

	/* allocate memory in own context, to have better accountability */
	context = AllocSetContextCreate(CurrentMemoryContext,
									"snapshot builder context",
									ALLOCSET_DEFAULT_MINSIZE,
									ALLOCSET_DEFAULT_INITSIZE,
									ALLOCSET_DEFAULT_MAXSIZE);
	oldcontext = MemoryContextSwitchTo(context);

	builder = palloc0(sizeof(SnapBuild));

	builder->state = SNAPBUILD_START;
	builder->context = context;
	builder->reorder = reorder;
	/* Other struct members initialized by zeroing via palloc0 above */

	builder->committed.xcnt = 0;
	builder->committed.xcnt_space = 128;		/* arbitrary number */
	builder->committed.xip =
		palloc0(builder->committed.xcnt_space * sizeof(TransactionId));
	builder->committed.includes_all_transactions = true;
	builder->committed.xip =
		palloc0(builder->committed.xcnt_space * sizeof(TransactionId));
	builder->initial_xmin_horizon = xmin_horizon;
	builder->transactions_after = start_lsn;

	MemoryContextSwitchTo(oldcontext);

	return builder;
}

/*
 * Free a snapshot builder.
 */
void
FreeSnapshotBuilder(SnapBuild *builder)
{
	MemoryContext context = builder->context;

	/* free snapshot explicitly, that contains some error checking */
	if (builder->snapshot != NULL)
	{
		SnapBuildSnapDecRefcount(builder->snapshot);
		builder->snapshot = NULL;
	}

	/* other resources are deallocated via memory context reset */
	MemoryContextDelete(context);
}

/*
 * Free an unreferenced snapshot that has previously been built by us.
 */
static void
SnapBuildFreeSnapshot(Snapshot snap)
{
	/* make sure we don't get passed an external snapshot */
	Assert(snap->satisfies == HeapTupleSatisfiesMVCCDuringDecoding);

	/* make sure nobody modified our snapshot */
	Assert(snap->curcid == FirstCommandId);
	Assert(!snap->suboverflowed);
	Assert(!snap->takenDuringRecovery);
	Assert(!snap->regd_count);

	/* slightly more likely, so it's checked even without c-asserts */
	if (snap->copied)
		elog(ERROR, "cannot free a copied snapshot");

	if (snap->active_count)
		elog(ERROR, "cannot free an active snapshot");

	pfree(snap);
}

/*
 * In which state of snapshot building ar we?
 */
SnapBuildState
SnapBuildCurrentState(SnapBuild *builder)
{
	return builder->state;
}

/*
 * Should the contents of transaction ending at 'ptr' be decoded?
 */
bool
SnapBuildXactNeedsSkip(SnapBuild *builder, XLogRecPtr ptr)
{
	return ptr <= builder->transactions_after;
}

/*
 * Increase refcount of a snapshot.
 *
 * This is used when handing out a snapshot to some external resource or when
 * adding a Snapshot as builder->snapshot.
 */
static void
SnapBuildSnapIncRefcount(Snapshot snap)
{
	snap->active_count++;
}

/*
 * Decrease refcount of a snapshot and free if the refcount reaches zero.
 *
 * Externally visible so external resources that have been handed an IncRef'ed
 * Snapshot can free it easily.
 */
void
SnapBuildSnapDecRefcount(Snapshot snap)
{
	/* make sure we don't get passed an external snapshot */
	Assert(snap->satisfies == HeapTupleSatisfiesMVCCDuringDecoding);

	/* make sure nobody modified our snapshot */
	Assert(snap->curcid == FirstCommandId);
	Assert(!snap->suboverflowed);
	Assert(!snap->takenDuringRecovery);
	Assert(!snap->regd_count);

	Assert(snap->active_count);

	/* slightly more likely, so its checked even without casserts */
	if (snap->copied)
		elog(ERROR, "cannot free a copied snapshot");

	snap->active_count--;
	if (!snap->active_count)
		SnapBuildFreeSnapshot(snap);
}

/*
 * Build a new snapshot, based on currently committed catalog-modifying
 * transactions.
 *
 * In-progress transactions with catalog access are *not* allowed to modify
 * these snapshots; they have to copy them and fill in appropriate ->curcid
 * and ->subxip/subxcnt values.
 */
static Snapshot
SnapBuildBuildSnapshot(SnapBuild *builder, TransactionId xid)
{
	Snapshot	snapshot;
	Size		ssize;

	Assert(builder->state >= SNAPBUILD_FULL_SNAPSHOT);

	ssize = sizeof(SnapshotData)
		+ sizeof(TransactionId) * builder->committed.xcnt
		+ sizeof(TransactionId) * 1 /* toplevel xid */ ;

	snapshot = MemoryContextAllocZero(builder->context, ssize);

	snapshot->satisfies = HeapTupleSatisfiesMVCCDuringDecoding;

	/*
	 * We misuse the original meaning of SnapshotData's xip and subxip fields
	 * to make the more fitting for our needs.
	 *
	 * In the 'xip' array we store transactions that have to be treated as
	 * committed. Since we will only ever look at tuples from transactions
	 * that have modified the catalog its more efficient to store those few
	 * that exist between xmin and xmax (frequently there are none).
	 *
	 * Snapshots that are used in transactions that have modified the catalog
	 * also use the 'subxip' array to store their toplevel xid and all the
	 * subtransaction xids so we can recognize when we need to treat rows as
	 * visible that are not in xip but still need to be visible. Subxip only
	 * gets filled when the transaction is copied into the context of a
	 * catalog modifying transaction since we otherwise share a snapshot
	 * between transactions. As long as a txn hasn't modified the catalog it
	 * doesn't need to treat any uncommitted rows as visible, so there is no
	 * need for those xids.
	 *
	 * Both arrays are qsort'ed so that we can use bsearch() on them.
	 *
	 * XXX: Do we want extra fields instead of misusing existing ones instead?
	 */
	Assert(TransactionIdIsNormal(builder->xmin));
	Assert(TransactionIdIsNormal(builder->xmax));

	snapshot->xmin = builder->xmin;
	snapshot->xmax = builder->xmax;

	/* store all transactions to be treated as committed by this snapshot */
	snapshot->xip =
		(TransactionId *) ((char *) snapshot + sizeof(SnapshotData));
	snapshot->xcnt = builder->committed.xcnt;
	memcpy(snapshot->xip,
		   builder->committed.xip,
		   builder->committed.xcnt * sizeof(TransactionId));

	/* sort so we can bsearch() */
	qsort(snapshot->xip, snapshot->xcnt, sizeof(TransactionId), xidComparator);

	/*
	 * Initially, subxip is empty, i.e. it's a snapshot to be used by
	 * transactions that don't modify the catalog. Will be filled by
	 * ReorderBufferCopySnap() if necessary.
	 */
	snapshot->subxcnt = 0;
	snapshot->subxip = NULL;

	snapshot->suboverflowed = false;
	snapshot->takenDuringRecovery = false;
	snapshot->copied = false;
	snapshot->curcid = FirstCommandId;
	snapshot->active_count = 0;
	snapshot->regd_count = 0;

	return snapshot;
}

/*
 * Export a snapshot so it can be set in another session with SET TRANSACTION
 * SNAPSHOT.
 *
 * For that we need to start a transaction in the current backend as the
 * importing side checks whether the source transaction is still open to make
 * sure the xmin horizon hasn't advanced since then.
 *
 * After that we convert a locally built snapshot into the normal variant
 * understood by HeapTupleSatisfiesMVCC et al.
 */
const char *
SnapBuildExportSnapshot(SnapBuild *builder)
{
	Snapshot	snap;
	char	   *snapname;
	TransactionId xid;
	TransactionId *newxip;
	int			newxcnt = 0;

	if (builder->state != SNAPBUILD_CONSISTENT)
		elog(ERROR, "cannot export a snapshot before reaching a consistent state");

	if (!builder->committed.includes_all_transactions)
		elog(ERROR, "cannot export a snapshot, not all transactions are monitored anymore");

	/* so we don't overwrite the existing value */
	if (TransactionIdIsValid(MyPgXact->xmin))
		elog(ERROR, "cannot export a snapshot when MyPgXact->xmin already is valid");

	if (IsTransactionOrTransactionBlock())
		elog(ERROR, "cannot export a snapshot from within a transaction");

	if (SavedResourceOwnerDuringExport)
		elog(ERROR, "can only export one snapshot at a time");

	SavedResourceOwnerDuringExport = CurrentResourceOwner;
	ExportInProgress = true;

	StartTransactionCommand();

	Assert(!FirstSnapshotSet);

	/* There doesn't seem to a nice API to set these */
	XactIsoLevel = XACT_REPEATABLE_READ;
	XactReadOnly = true;

	snap = SnapBuildBuildSnapshot(builder, GetTopTransactionId());

	/*
	 * We know that snap->xmin is alive, enforced by the logical xmin
	 * mechanism. Due to that we can do this without locks, we're only
	 * changing our own value.
	 */
	MyPgXact->xmin = snap->xmin;

	/* allocate in transaction context */
	newxip = (TransactionId *)
		palloc(sizeof(TransactionId) * GetMaxSnapshotXidCount());

	/*
	 * snapbuild.c builds transactions in an "inverted" manner, which means it
	 * stores committed transactions in ->xip, not ones in progress. Build a
	 * classical snapshot by marking all non-committed transactions as
	 * in-progress. This can be expensive.
	 */
	for (xid = snap->xmin; NormalTransactionIdPrecedes(xid, snap->xmax);)
	{
		void	   *test;

		/*
		 * Check whether transaction committed using the decoding snapshot
		 * meaning of ->xip.
		 */
		test = bsearch(&xid, snap->xip, snap->xcnt,
					   sizeof(TransactionId), xidComparator);

		if (test == NULL)
		{
			if (newxcnt >= GetMaxSnapshotXidCount())
				elog(ERROR, "snapshot too large");

			newxip[newxcnt++] = xid;
		}

		TransactionIdAdvance(xid);
	}

	snap->xcnt = newxcnt;
	snap->xip = newxip;

	/*
	 * now that we've built a plain snapshot, use the normal mechanisms for
	 * exporting it
	 */
	snapname = ExportSnapshot(snap);

	ereport(LOG,
			(errmsg("exported snapbuild snapshot: \"%s\" with %u xids",
					snapname, snap->xcnt)));
	return snapname;
}

/*
 * Reset a previously SnapBuildExportSnapshot()'ed snapshot if there is
 * any. Aborts the previously started transaction and resets the resource
 * owner back to it's original value.
 */
void
SnapBuildClearExportedSnapshot()
{
	/* nothing exported, thats the usual case */
	if (!ExportInProgress)
		return;

	if (!IsTransactionState())
		elog(ERROR, "clearing exported snapshot in wrong transaction state");

	/* make sure nothing  could have ever happened */
	AbortCurrentTransaction();

	CurrentResourceOwner = SavedResourceOwnerDuringExport;
	SavedResourceOwnerDuringExport = NULL;
	ExportInProgress = false;
}

/*
 * Handle the effects of a single heap change, appropriate to the current state
 * of the snapshot builder and returns whether changes made at (xid, lsn) may
 * be decoded.
 */
bool
SnapBuildProcessChange(SnapBuild *builder, TransactionId xid, XLogRecPtr lsn)
{
	bool is_old_tx;

	/*
	 * We can't handle data in transactions if we haven't built a snapshot
	 * yet, so don't store them.
	 */
	if (builder->state < SNAPBUILD_FULL_SNAPSHOT)
		return false;

	/*
	 * No point in keeping track of changes in transactions that we don't have
	 * enough information about to decode.
	 */
	if (builder->state < SNAPBUILD_CONSISTENT &&
		SnapBuildTxnIsRunning(builder, xid))
		return false;

	/*
	 * There's a change coming that will need a snapshot to be decoded, add
	 * one if not already present.
	 */
	is_old_tx = ReorderBufferIsXidKnown(builder->reorder, xid);

	if (!is_old_tx || !ReorderBufferXidHasBaseSnapshot(builder->reorder, xid))
	{
		/* only build a new snapshot if we don't have a prebuilt one */
		if (builder->snapshot == NULL)
		{
			builder->snapshot = SnapBuildBuildSnapshot(builder, xid);
			/* inrease refcount for the snapshot builder */
			SnapBuildSnapIncRefcount(builder->snapshot);
		}

		/* increase refcount for the transaction */
		SnapBuildSnapIncRefcount(builder->snapshot);
		ReorderBufferSetBaseSnapshot(builder->reorder, xid, lsn,
									 builder->snapshot);
	}

	return true;
}

/*
 * Do CommandId/ComboCid handling after reading a xl_heap_new_cid record. This
 * implies that a transaction has done some for of write to system catalogs.
 */
void
SnapBuildProcessNewCid(SnapBuild *builder, TransactionId xid,
					   XLogRecPtr lsn, xl_heap_new_cid *xlrec)
{
	CommandId	cid;

	/*
	 * we only log new_cid's if a catalog tuple was modified, so mark
	 * the transaction as containing catalog modifications
	 */
	ReorderBufferXidSetCatalogChanges(builder->reorder, xid,lsn);

	ReorderBufferAddNewTupleCids(builder->reorder, xlrec->top_xid, lsn,
								 xlrec->target.node, xlrec->target.tid,
								 xlrec->cmin, xlrec->cmax,
								 xlrec->combocid);

	/* figure out new command id */
	if (xlrec->cmin != InvalidCommandId &&
		xlrec->cmax != InvalidCommandId)
		cid = Max(xlrec->cmin, xlrec->cmax);
	else if (xlrec->cmax != InvalidCommandId)
		cid = xlrec->cmax;
	else if (xlrec->cmin != InvalidCommandId)
		cid = xlrec->cmin;
	else
	{
		cid = InvalidCommandId;		/* silence compiler */
		elog(ERROR, "broken arrow, no cid?");
	}

	ReorderBufferAddNewCommandId(builder->reorder, xid, lsn, cid + 1);
}

/*
 * Check whether `xid` is currently 'running'. Running transactions in our
 * parlance are transactions which we didn't observe from the start so we can't
 * properly decode them. They only exist after we freshly started from an
 * < CONSISTENT snapshot.
 */
static bool
SnapBuildTxnIsRunning(SnapBuild *builder, TransactionId xid)
{
	Assert(builder->state < SNAPBUILD_CONSISTENT);
	Assert(TransactionIdIsValid(builder->running.xmin));
	Assert(TransactionIdIsValid(builder->running.xmax));

	if (builder->running.xcnt &&
		NormalTransactionIdFollows(xid, builder->running.xmin) &&
		NormalTransactionIdPrecedes(xid, builder->running.xmax))
	{
		TransactionId *search =
		bsearch(&xid, builder->running.xip, builder->running.xcnt_space,
				sizeof(TransactionId), xidComparator);

		if (search != NULL)
		{
			Assert(*search == xid);
			return true;
		}
	}

	return false;
}

/*
 * Add a new Snapshot to all transactions we're decoding that currently are
 * in-progress so they can see new catalog contents made by the transaction
 * that just committed. This is necessary because those in-progress
 * transactions will use the new catalog's contents from here on (at the very
 * least everything they do needs to be compatible with newer catalog
 * contents).
 */
static void
SnapBuildDistributeNewCatalogSnapshot(SnapBuild *builder, XLogRecPtr lsn)
{
	dlist_iter	txn_i;
	ReorderBufferTXN *txn;

	/*
	 * Iterate through all toplevel transactions. This can include
	 * subtransactions which we just don't yet know to be that, but that's
	 * fine, they will just get an unneccesary snapshot queued.
	 */
	dlist_foreach(txn_i, &builder->reorder->toplevel_by_lsn)
	{
		txn = dlist_container(ReorderBufferTXN, node, txn_i.cur);

		Assert(TransactionIdIsValid(txn->xid));

		/*
		 * If we don't have a base snapshot yet, there are no changes in this
		 * transaction which in turn implies we don't yet need a snapshot at
		 * all. We'll add add a snapshot when the first change gets queued.
		 *
		 * XXX: is that fine if only a subtransaction has a base snapshot so
		 * far?
		 */
		if (!ReorderBufferXidHasBaseSnapshot(builder->reorder, txn->xid))
			continue;

		elog(DEBUG2, "adding a new snapshot to %u at %X/%X",
			 txn->xid, (uint32) (lsn >> 32), (uint32) lsn);

		/* increase refcount for the transaction */
		SnapBuildSnapIncRefcount(builder->snapshot);
		ReorderBufferAddSnapshot(builder->reorder, txn->xid, lsn,
								 builder->snapshot);
	}
}

/*
 * Keep track of a new catalog changing transaction that has committed.
 */
static void
SnapBuildAddCommittedTxn(SnapBuild *builder, TransactionId xid)
{
	Assert(TransactionIdIsValid(xid));

	if (builder->committed.xcnt == builder->committed.xcnt_space)
	{
		builder->committed.xcnt_space = builder->committed.xcnt_space * 2 + 1;

		/* XXX: put in a limit here as a defense against bugs? */

		elog(DEBUG1, "increasing space for committed transactions to %u",
			 (uint32) builder->committed.xcnt_space);

		builder->committed.xip = repalloc(builder->committed.xip,
					builder->committed.xcnt_space * sizeof(TransactionId));
	}

	/*
	 * XXX: It might make sense to keep the array sorted here instead of doing
	 * it every time we build a new snapshot. On the other hand this gets
	 * called repeatedly when a transaction with subtransactions commits.
	 */
	builder->committed.xip[builder->committed.xcnt++] = xid;
}

/*
 * Remove knowledge about transactions we treat as committed that are smaller
 * than ->xmin. Those won't ever get checked via the ->commited array but via
 * the clog machinery, so we don't need to waste memory on them.
 */
static void
SnapBuildPurgeCommittedTxn(SnapBuild *builder)
{
	int			off;
	TransactionId *workspace;
	int			surviving_xids = 0;

	/* not ready yet */
	if (!TransactionIdIsNormal(builder->xmin))
		return;

	/* XXX: Neater algorithm? */
	workspace =
		MemoryContextAlloc(builder->context,
						   builder->committed.xcnt * sizeof(TransactionId));

	/* copy xids that still are interesting to workspace */
	for (off = 0; off < builder->committed.xcnt; off++)
	{
		if (NormalTransactionIdPrecedes(builder->committed.xip[off],
										builder->xmin))
			;					/* remove */
		else
			workspace[surviving_xids++] = builder->committed.xip[off];
	}

	/* copy workspace back to persistent state */
	memcpy(builder->committed.xip, workspace,
		   surviving_xids * sizeof(TransactionId));

	elog(DEBUG3, "purged committed transactions from %u to %u, xmin: %u, xmax: %u",
		 (uint32) builder->committed.xcnt, (uint32) surviving_xids,
		 builder->xmin, builder->xmax);
	builder->committed.xcnt = surviving_xids;

	pfree(workspace);
}

/*
 * Common logic for SnapBuildAbortTxn and SnapBuildCommitTxn dealing with
 * keeping track of the amount of running transactions.
 *
 * FIXME: make sure subtransaction handling is correct!
 */
static void
SnapBuildEndTxn(SnapBuild *builder, XLogRecPtr lsn, TransactionId xid)
{
	if (builder->state == SNAPBUILD_CONSISTENT)
		return;

	if (SnapBuildTxnIsRunning(builder, xid))
	{
		Assert(builder->running.xcnt > 0);

		if (!--builder->running.xcnt)
		{
			/*
			 * None of the originally running transaction is running anymore,
			 * so our incrementaly built snapshot now is consistent.
			 */
			ereport(LOG,
					(errmsg("lsn %X/%X: xid %u finished, found consistent point due to SnapBuildEndTxn + running",
							(uint32)(lsn >> 32), (uint32)lsn, xid)));
			builder->state = SNAPBUILD_CONSISTENT;
		}
	}
}

/*
 * Abort a transaction, throw away all state we kept.
 */
void
SnapBuildAbortTxn(SnapBuild *builder, XLogRecPtr lsn,
				  TransactionId xid,
				  int nsubxacts, TransactionId *subxacts)
{
	int			i;

	for (i = 0; i < nsubxacts; i++)
	{
		TransactionId subxid = subxacts[i];

		SnapBuildEndTxn(builder, lsn, subxid);
	}

	SnapBuildEndTxn(builder, lsn, xid);
}

/*
 * Handle everything that needs to be done when a transaction commits
 */
void
SnapBuildCommitTxn(SnapBuild *builder, XLogRecPtr lsn, TransactionId xid,
				   int nsubxacts, TransactionId *subxacts)
{
	int			nxact;

	bool		forced_timetravel = false;
	bool		sub_needs_timetravel = false;
	bool		top_needs_timetravel = false;

	TransactionId xmax = xid;

	/*
	 * If we couldn't observe every change of a transaction because it was
	 * already running at the point we started to observe we have to assume it
	 * made catalog changes.
	 *
	 * This has the positive benefit that we afterwards have enough
	 * information to build an exportable snapshot thats usable by pg_dump et
	 * al.
	 */
	if (builder->state < SNAPBUILD_CONSISTENT)
	{
		/* ensure that only commits after this are getting replayed */
		if (builder->transactions_after < lsn)
			builder->transactions_after = lsn;

		/*
		 * we could avoid treating !SnapBuildTxnIsRunning transactions as
		 * timetravel ones, but we want to be able to export a snapshot when
		 * we reached consistency.
		 */
		forced_timetravel = true;
		elog(DEBUG1, "forced to assume catalog changes for xid %u because it was running to early", xid);
	}

	for (nxact = 0; nxact < nsubxacts; nxact++)
	{
		TransactionId subxid = subxacts[nxact];

		/*
		 * make sure txn is not tracked in running txn's anymore, switch state
		 */
		SnapBuildEndTxn(builder, lsn, subxid);

		/*
		 * If we're forcing timetravel we also need accurate subtransaction
		 * status.
		 */
		if (forced_timetravel)
		{
			SnapBuildAddCommittedTxn(builder, subxid);
			if (NormalTransactionIdFollows(subxid, xmax))
				xmax = subxid;
		}

		/*
		 * add subtransaction to base snapshot, we don't distinguish to
		 * toplevel transactions there.
		 */
		else if (ReorderBufferXidHasCatalogChanges(builder->reorder, subxid))
		{
			sub_needs_timetravel = true;

			elog(DEBUG1, "found subtransaction %u:%u with catalog changes.",
				 xid, subxid);

			SnapBuildAddCommittedTxn(builder, subxid);

			if (NormalTransactionIdFollows(subxid, xmax))
				xmax = subxid;
		}
	}

	/*
	 * make sure txn is not tracked in running txn's anymore, switch state
	 */
	SnapBuildEndTxn(builder, lsn, xid);

	if (forced_timetravel)
	{
		elog(DEBUG2, "forced transaction %u to do timetravel.", xid);

		SnapBuildAddCommittedTxn(builder, xid);
	}
	/* add toplevel transaction to base snapshot */
	else if (ReorderBufferXidHasCatalogChanges(builder->reorder, xid))
	{
		elog(DEBUG2, "found top level transaction %u, with catalog changes!",
			 xid);

		top_needs_timetravel = true;
		SnapBuildAddCommittedTxn(builder, xid);
	}
	else if (sub_needs_timetravel)
	{
		/* mark toplevel txn as timetravel as well */
		SnapBuildAddCommittedTxn(builder, xid);
	}

	if (forced_timetravel || top_needs_timetravel || sub_needs_timetravel)
	{
		if (!TransactionIdIsValid(builder->xmax) ||
			TransactionIdFollowsOrEquals(xmax, builder->xmax))
		{
			builder->xmax = xmax;
			TransactionIdAdvance(builder->xmax);
		}

		if (builder->state < SNAPBUILD_FULL_SNAPSHOT)
			return;

		/* decrease the snapshot builder's refcount of the old snapshot */
		if (builder->snapshot)
			SnapBuildSnapDecRefcount(builder->snapshot);

		builder->snapshot = SnapBuildBuildSnapshot(builder, xid);

		/* we might need to execute invalidations */
		if (!ReorderBufferXidHasBaseSnapshot(builder->reorder, xid))
		{
			SnapBuildSnapIncRefcount(builder->snapshot);
			ReorderBufferSetBaseSnapshot(builder->reorder, xid, lsn,
										 builder->snapshot);
		}

		/* refcount of the snapshot builder for the new snapshot */
		SnapBuildSnapIncRefcount(builder->snapshot);

		/* add a new SnapshotNow to all currently running transactions */
		SnapBuildDistributeNewCatalogSnapshot(builder, lsn);
	}
	else
	{
		/* record that we cannot export a general snapshot anymore */
		builder->committed.includes_all_transactions = false;
	}
}


/* -----------------------------------
 * Snapshot building functions dealing with xlog records
 * -----------------------------------
 */
void
SnapBuildProcessRunningXacts(SnapBuild *builder, XLogRecPtr lsn, xl_running_xacts *running)
{
	ReorderBufferTXN *txn;

	if (builder->state < SNAPBUILD_CONSISTENT)
	{
		/* returns false if there's no point in performing cleanup just yet */
		if (!SnapBuildFindSnapshot(builder, lsn, running))
			return;
	}
	else
	{
		SnapBuildSerialize(builder, lsn);
	}

	/*
	 * update range of interesting xids. We don't increase ->xmax because once
	 * we are in a consistent state we can do that ourselves and much more
	 * efficiently so because we only need to do it for catalog transactions.
	 */
	builder->xmin = running->oldestRunningXid;

	/*
	 * xmax can be lower than xmin here because we only increase xmax when we
	 * hit a transaction with catalog changes. While odd looking, its correct
	 * and actually more efficient this way since we hit fast paths in
	 * tqual.c.
	 */

	/* Remove transactions we don't need to keep track off anymore */
	SnapBuildPurgeCommittedTxn(builder);

	elog(DEBUG3, "xmin: %u, xmax: %u, oldestrunning: %u",
		 builder->xmin, builder->xmax,
		 running->oldestRunningXid);

	/*
	 * inrease shared memory state, so vacuum can work on tuples we prevent
	 * from being pruned till now.
	 */
	IncreaseLogicalXminForSlot(lsn, running->oldestRunningXid);

	/*
	 * Also tell the slot where we can restart decoding from. We don't want to
	 * do that after every commit because changing that implies an fsync of
	 * the logical slot's state file, so we only do it every time we see a
	 * running xacts record.
	 *
	 * Do so by looking for the oldest in progress transaction (determined by
	 * the first LSN of any of its relevant records). Every transaction
	 * remembers the last location we stored the snapshot to disk before its
	 * beginning. That point is where we can restart from.
	 */

	/*
	 * Can't know about a serialized snapshot's location if we're not
	 * consistent
	 */
	if (builder->state < SNAPBUILD_CONSISTENT)
		return;

	txn = ReorderBufferGetOldestTXN(builder->reorder);

	/*
	 * oldest ongoing txn might have started when we didn't yet serialize
	 * anything because we hadn't reached a consistent state yet.
	 */
	if (txn != NULL && txn->restart_decoding_lsn != InvalidXLogRecPtr)
		IncreaseRestartDecodingForSlot(lsn, txn->restart_decoding_lsn);

	/*
	 * No in-progress transaction, can reuse the last serialized snapshot if we
	 * have one.
	 */
	else if (txn == NULL &&
			 builder->reorder->current_restart_decoding_lsn != InvalidXLogRecPtr &&
			 builder->last_serialized_snapshot != InvalidXLogRecPtr)
		IncreaseRestartDecodingForSlot(lsn, builder->last_serialized_snapshot);
}


/*
 * Build the start of a snapshot that's capable of decoding the
 * catalog. Helper function for SnapBuildProcessRunningXacts() while we're not
 * yet consistent.
 *
 * Returns true if there is a point in performing internal maintenance/cleanup
 * using the xl_running_xacts record.
 */
static bool
SnapBuildFindSnapshot(SnapBuild *builder, XLogRecPtr lsn, xl_running_xacts *running)
{
	/* ---
	 * Build catalog decoding snapshot incrementally using information about
	 * the currently running transactions. There are several ways to do that:

	 * a) There were no running transactions when the xl_running_xacts record
	 *    was inserted, jump to CONSISTENT immediately. We might find such a
	 *    state we were waiting for b) and c).

	 * b) Wait for all toplevel transactions that were running to end. We
	 *    simply track the number of in-progress toplevel transactions and
	 *    lower it whenever one commits or aborts. When that number
	 *    (builder->running.xcnt) reaches zero, we can go from FULL_SNAPSHOT
	 *    to CONSISTENT.
	 *	  NB: We need to search running.xip when seeing a transaction's end to
	 *    make sure it's a toplevel transaction and it's been one of the
	 *    intially running ones.
	 *	  Interestingly, in contrast to HS this allows us not to care about
	 *	  subtransactions - and by extension suboverflowed xl_running_xacts -
	 *	  at all.
	 *
	 * c) This (in a previous run) or another decoding slot serialized a
	 *    snapshot to disk that we can use.
	 * ---
	 */

	/*
	 * xl_running_xact record is older than what we can use, we might not have
	 * all necessary catalog rows anymore.
	 */
	if (TransactionIdIsNormal(builder->initial_xmin_horizon) &&
		NormalTransactionIdPrecedes(running->oldestRunningXid,
									builder->initial_xmin_horizon))
	{
		ereport(LOG,
				(errmsg("lsn %X/%X: skipping snapshot due to initial xmin horizon of %u vs the snapshot's %u",
						(uint32) (lsn >> 32), (uint32) lsn,
						builder->initial_xmin_horizon, running->oldestRunningXid)));
		return true;
	}

	/*
	 * a) No transaction were running, we can jump to consistent.
	 *
	 * NB: We might have already started to incrementally assemble a snapshot,
	 * so we need to be careful to deal with that.
	 */
	if (running->xcnt == 0)
	{
		if (builder->transactions_after == InvalidXLogRecPtr ||
			builder->transactions_after < lsn)
			builder->transactions_after = lsn;

		builder->xmin = running->oldestRunningXid;
		builder->xmax = running->latestCompletedXid;
		TransactionIdAdvance(builder->xmax);

		Assert(TransactionIdIsNormal(builder->xmin));
		Assert(TransactionIdIsNormal(builder->xmax));

		/* no transactions running now */
		builder->running.xcnt = 0;
		builder->running.xmin = InvalidTransactionId;
		builder->running.xmax = InvalidTransactionId;

		builder->state = SNAPBUILD_CONSISTENT;

		ereport(LOG,
				(errmsg("lsn %X/%X: found initial snapshot (xmin %u) due to running xacts with xcnt == 0",
						(uint32)(lsn >> 32), (uint32)lsn,
						builder->xmin)));

		return false;
	}
	/* c) valid on disk state */
	else if (SnapBuildRestore(builder, lsn))
	{
		/* there won't be any state to cleanup */
		return false;
	}
	/*
	 * b) first encounter of a useable xl_running_xacts record. If we had
	 * found one earlier we would either track running transactions
	 * (i.e. builder->running.xcnt != 0) or be consistent (this function
	 * wouldn't get called)..
	 */
	else if (!builder->running.xcnt)
	{
		int off;

		/*
		 * We only care about toplevel xids as those are the ones we
		 * definitely see in the wal stream. As snapbuild.c tracks committed
		 * instead of running transactions we don't need to know anything
		 * about uncommitted subtransactions.
		 */
		builder->xmin = running->oldestRunningXid;
		builder->xmax = running->latestCompletedXid;
		TransactionIdAdvance(builder->xmax);

		/* so we can safely use the faster comparisons */
		Assert(TransactionIdIsNormal(builder->xmin));
		Assert(TransactionIdIsNormal(builder->xmax));

		builder->running.xcnt = running->xcnt;
		builder->running.xcnt_space = running->xcnt;
		builder->running.xip =
			MemoryContextAlloc(builder->context,
							builder->running.xcnt * sizeof(TransactionId));
		memcpy(builder->running.xip, running->xids,
			   builder->running.xcnt * sizeof(TransactionId));

		/* sort so we can do a binary search */
		qsort(builder->running.xip, builder->running.xcnt,
			  sizeof(TransactionId), xidComparator);

		builder->running.xmin = builder->running.xip[0];
		builder->running.xmax = builder->running.xip[running->xcnt - 1];

		/* makes comparisons cheaper later */
		TransactionIdRetreat(builder->running.xmin);
		TransactionIdAdvance(builder->running.xmax);

		builder->state = SNAPBUILD_FULL_SNAPSHOT;

		ereport(LOG,
				(errmsg("lsn %X/%X: found initial snapshot (xmin %u) due to running xacts, %u xacts need to finish",
						(uint32)(lsn >> 32), (uint32)lsn,
						builder->xmin, (uint32) builder->running.xcnt)));

		/*
		 * Iterate through all xids, wait for them to finish.
		 *
		 * This isn't required for the correctness of decoding but primarily
		 * to allow isolationtester to notice that we're currently waiting for
		 * something.
		 */
		for(off = 0; off < builder->running.xcnt; off++)
		{
			XactLockTableWait(builder->running.xip[off]);
		}

		/* nothing could have built up so far */
		return false;
	}

	/*
	 * We already started to track running xacts and need to wait for all
	 * in-progress ones to finish. We fall through to the normal processing of
	 * records so incremental cleanup can be performed.
	 */
	return true;
}


/* -----------------------------------
 * Snapshot serialization support
 * -----------------------------------
 */

/*
 * We store current state of struct SnapBuild on disk in the following manner:
 *
 * struct SnapBuildOnDisk;
 * TransactionId * running.xcnt_space;
 * TransactionId * committed.xcnt; (*not xcnt_space*)
 *
 */
typedef struct SnapBuildOnDisk
{
	/* first part of this struct needs to be version independent */

	/* data not covered by checksum */
	uint32		magic;
	pg_crc32	checksum;

	/* data covered by checksum */

	/* version, in case we want to support pg_upgrade */
	uint32		version;
	/* how large is the on disk data, excluding the constant sized part */
	uint32		length;

	/* version dependent part */
	SnapBuild	builder;

	/* variable amount of TransactionIds follows */
} SnapBuildOnDisk;

#define SnapBuildOnDiskConstantSize \
	offsetof(SnapBuildOnDisk, builder)
#define SnapBuildOnDiskNotChecksummedSize \
	offsetof(SnapBuildOnDisk, version)

#define SNAPBUILD_MAGIC 0x51A1E001
#define SNAPBUILD_VERSION 1

/*
 * Store/Load a snapshot from disk, depending on the snapshot builder's state.
 *
 * Supposed to be used by external (i.e. not snapbuild.c) code that just reada
 * record that's a potential location for a serialized snapshot.
 */
void
SnapBuildSerializationPoint(SnapBuild *builder, XLogRecPtr lsn)
{
	if (builder->state < SNAPBUILD_CONSISTENT)
		SnapBuildRestore(builder, lsn);
	else
		SnapBuildSerialize(builder, lsn);
}

/*
 * Serialize the snapshot 'builder' at the location 'lsn' if it hasn't already
 * been done by another decoding process.
 */
static void
SnapBuildSerialize(SnapBuild *builder, XLogRecPtr lsn)
{
	Size		needed_length;
	SnapBuildOnDisk *ondisk;
	char	   *ondisk_c;
	int			fd;
	char		tmppath[MAXPGPATH];
	char		path[MAXPGPATH];
	int			ret;
	struct stat stat_buf;
	uint32		sz;

	Assert(lsn != InvalidXLogRecPtr);
	Assert(builder->last_serialized_snapshot == InvalidXLogRecPtr ||
		   builder->last_serialized_snapshot <= lsn);

	/*
	 * no point in serializing if we cannot continue to work immediately after
	 * restoring the snapshot
	 */
	if (builder->state < SNAPBUILD_CONSISTENT)
		return;

	/*
	 * FIXME: Timeline handling/naming.
	 */

	/*
	 * first check whether some other backend already has written the snapshot
	 * for this LSN. It's perfectly fine if there's none, so we accept ENOENT
	 * as a valid state. Everything else is an unexpected error.
	 */
	sprintf(path, "pg_llog/snapshots/%X-%X.snap",
			(uint32) (lsn >> 32), (uint32) lsn);

	ret = stat(path, &stat_buf);

	if (ret != 0 && errno != ENOENT)
		ereport(ERROR,
				(errmsg("could not stat file \"%s\": %m", path)));

	else if (ret == 0)
	{
		/*
		 * somebody else has already serialized to this point, don't overwrite
		 * but remember location, so we don't need to read old data again.
		 *
		 * To be sure it has been synced to disk after the rename() from the
		 * tempfile filename to the real filename, we just repeat the
		 * fsync. That ought to be cheap because in most scenarios it should
		 * already be safely on disk.
		 */
		fsync_fname(path, false);
		fsync_fname("pg_llog/snapshots", true);

		builder->last_serialized_snapshot = lsn;
		goto out;
	}

	/*
	 * there is an obvious race condition here between the time we stat(2) the
	 * file and us writing the file. But we rename the file into place
	 * atomically and all files created need to contain the same data anyway,
	 * so this is perfectly fine, although a bit of a resource waste. Locking
	 * seems like pointless complication.
	 */
	elog(DEBUG1, "serializing snapshot to %s", path);

	/* to make sure only we will write to this tempfile, include pid */
	sprintf(tmppath, "pg_llog/snapshots/%X-%X.snap.%u.tmp",
			(uint32) (lsn >> 32), (uint32) lsn, MyProcPid);

	/*
	 * Unlink temporary file if it already exists, needs to have been before a
	 * crash/error since we won't enter this function twice from within a
	 * single decoding slot/backend and the temporary file contains the pid of
	 * the current process.
	 */
	if (unlink(tmppath) != 0 && errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not unlink file \"%s\": %m",	path)));

	needed_length = sizeof(SnapBuildOnDisk) +
		sizeof(TransactionId) * builder->running.xcnt_space +
		sizeof(TransactionId) * builder->committed.xcnt;

	ondisk_c = MemoryContextAllocZero(builder->context, needed_length);
	ondisk = (SnapBuildOnDisk *) ondisk_c;
	ondisk->magic = SNAPBUILD_MAGIC;
	ondisk->version = SNAPBUILD_VERSION;
	ondisk->length = needed_length;
	INIT_CRC32(ondisk->checksum);
	COMP_CRC32(ondisk->checksum,
			   ((char *) ondisk) + SnapBuildOnDiskNotChecksummedSize,
			   SnapBuildOnDiskConstantSize - SnapBuildOnDiskNotChecksummedSize);
	ondisk_c += sizeof(SnapBuildOnDisk);

	memcpy(&ondisk->builder, builder, sizeof(SnapBuild));
	/* NULL-ify memory-only data */
	ondisk->builder.context = NULL;
	ondisk->builder.snapshot = NULL;
	ondisk->builder.reorder = NULL;
	ondisk->builder.running.xip = NULL;
	ondisk->builder.committed.xip = NULL;

	COMP_CRC32(ondisk->checksum,
			   &ondisk->builder,
			   sizeof(SnapBuild));

	/* copy running xacts */
	sz = sizeof(TransactionId) * builder->running.xcnt_space;
	memcpy(ondisk_c, builder->running.xip, sz);
	COMP_CRC32(ondisk->checksum, ondisk_c, sz);
	ondisk_c += sz;

	/* copy committed xacts */
	sz = sizeof(TransactionId) * builder->committed.xcnt;
	memcpy(ondisk_c, builder->committed.xip, sz);
	COMP_CRC32(ondisk->checksum, ondisk_c, sz);
	ondisk_c += sz;

	/* we have valid data now, open tempfile and write it there */
	fd = OpenTransientFile(tmppath,
						   O_CREAT | O_EXCL | O_WRONLY | PG_BINARY,
						   S_IRUSR | S_IWUSR);
	if (fd < 0)
		ereport(ERROR,
				(errmsg("could not open file \"%s\": %m", path)));

	if ((write(fd, ondisk, needed_length)) != needed_length)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m", tmppath)));
	}

	/*
	 * fsync the file before renaming so that even if we crash after this we
	 * have either a fully valid file or nothing.
	 *
	 * TODO: Do the fsync() via checkpoints/restartpoints, doing it here has
	 * some noticeable overhead since it's performed synchronously during
	 * decoding?
	 */
	if (pg_fsync(fd) != 0)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", tmppath)));
	}

	CloseTransientFile(fd);

	/*
	 * We may overwrite the work from some other backend, but that's ok, our
	 * snapshot is valid as well.
	 */
	if (rename(tmppath, path) != 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not rename file \"%s\" to \"%s\": %m",
						tmppath, path)));
	}

	/* make sure we persist */
	fsync_fname(path, false);
	fsync_fname("pg_llog/snapshots", true);

	/*
	 * now there's no way we loose the dumped state anymore, remember
	 * serialization point.
	 */
	builder->last_serialized_snapshot = lsn;

out:
	ReorderBufferSetRestartPoint(builder->reorder,
								 builder->last_serialized_snapshot);
}

/*
 * Restore a snapshot into 'builder' if previously one has been stored at the
 * location indicated by 'lsn'. Returns true if successfull, false otherwise.
 */
static bool
SnapBuildRestore(SnapBuild *builder, XLogRecPtr lsn)
{
	SnapBuildOnDisk ondisk;
	int			fd;
	char		path[MAXPGPATH];
	Size		sz;
	int			readBytes;
	pg_crc32	checksum;

	/* no point in loading a snapshot if we're already there */
	if (builder->state == SNAPBUILD_CONSISTENT)
		return false;

	sprintf(path, "pg_llog/snapshots/%X-%X.snap",
			(uint32) (lsn >> 32), (uint32) lsn);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY, 0);

	if (fd < 0 && errno == ENOENT)
		return false;
	else if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));

	/* read statically sized portion of snapshot */
	readBytes = read(fd, &ondisk, SnapBuildOnDiskConstantSize);
	if (readBytes != SnapBuildOnDiskConstantSize)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\", read %d of %d: %m",
						path, readBytes, (int) SnapBuildOnDiskConstantSize)));
	}

	if (ondisk.magic != SNAPBUILD_MAGIC)
		ereport(ERROR,
				(errmsg("snapbuild state file \"%s\" has wrong magic %u instead of %u",
						path, ondisk.magic, SNAPBUILD_MAGIC)));

	if (ondisk.version != SNAPBUILD_VERSION)
		ereport(ERROR,
				(errmsg("snapbuild state file \"%s\" has unsupported version %u instead of %u",
						path, ondisk.version, SNAPBUILD_VERSION)));

	INIT_CRC32(checksum);
	COMP_CRC32(checksum,
			   ((char *) &ondisk) + SnapBuildOnDiskNotChecksummedSize,
			   SnapBuildOnDiskConstantSize - SnapBuildOnDiskNotChecksummedSize);

	/* read SnapBuild */
	readBytes = read(fd, &ondisk.builder, sizeof(SnapBuild));
	if (readBytes != sizeof(SnapBuild))
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\", read %d of %d: %m",
						path, readBytes, (int) sizeof(SnapBuild))));
	}
	COMP_CRC32(checksum, &ondisk.builder, sizeof(SnapBuild));

	/* restore running xacts information */
	sz = sizeof(TransactionId) * ondisk.builder.running.xcnt_space;
	ondisk.builder.running.xip = MemoryContextAlloc(builder->context, sz);
	readBytes = read(fd, ondisk.builder.running.xip, sz);
	if (readBytes != sz)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\", read %d of %d: %m",
						path, readBytes, (int) sz)));
	}
	COMP_CRC32(checksum, ondisk.builder.running.xip, sz);

	/* restore committed xacts information */
	sz = sizeof(TransactionId) * ondisk.builder.committed.xcnt;
	ondisk.builder.committed.xip = MemoryContextAlloc(builder->context, sz);
	readBytes = read(fd, ondisk.builder.committed.xip, sz);
	if (readBytes != sz)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\", read %d of %d: %m",
						path, readBytes, (int) sz)));
	}
	COMP_CRC32(checksum, ondisk.builder.committed.xip, sz);

	CloseTransientFile(fd);

	/* verify checksum of what we've read */
	if (!EQ_CRC32(checksum, ondisk.checksum))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("snapbuild state file %s: checksum mismatch, is %u, should be %u",
						path, checksum, ondisk.checksum)));

	/*
	 * ok, we now have a sensible snapshot here, figure out if it has more
	 * information than we have.
	 */

	/*
	 * We are only interested in consistent snapshots for now, comparing
	 * whether one imcomplete snapshot is more "advanced" seems to be
	 * unnecessarily complex.
	 */
	if (ondisk.builder.state < SNAPBUILD_CONSISTENT)
		goto snapshot_not_interesting;

	/*
	 * Don't use a snapshot that requires an xmin that we cannot guarantee to
	 * be available.
	 */
	if (TransactionIdPrecedes(ondisk.builder.xmin, builder->initial_xmin_horizon))
		goto snapshot_not_interesting;

	/*
	 * XXX: transactions_after needs to be updated differently, to be checked
	 * here
	 */

	/* ok, we think the snapshot is sensible, copy over everything important */
	builder->xmin = ondisk.builder.xmin;
	builder->xmax = ondisk.builder.xmax;
	builder->state = ondisk.builder.state;

	builder->committed.xcnt = ondisk.builder.committed.xcnt;
	/* We only allocated/stored xcnt, not xcnt_space xids ! */
	/* don't overwrite preallocated xip, if we don't have anything here */
	if (builder->committed.xcnt > 0)
	{
		pfree(builder->committed.xip);
		builder->committed.xcnt_space = ondisk.builder.committed.xcnt;
		builder->committed.xip = ondisk.builder.committed.xip;
	}
	ondisk.builder.committed.xip = NULL;

	builder->running.xcnt = ondisk.builder.committed.xcnt;
	if (builder->running.xip)
		pfree(builder->running.xip);
	builder->running.xcnt_space = ondisk.builder.committed.xcnt_space;
	builder->running.xip = ondisk.builder.running.xip;

	/* our snapshot is not interesting anymore, build a new one */
	if (builder->snapshot != NULL)
	{
		SnapBuildSnapDecRefcount(builder->snapshot);
	}
	builder->snapshot = SnapBuildBuildSnapshot(builder, InvalidTransactionId);
	SnapBuildSnapIncRefcount(builder->snapshot);

	ReorderBufferSetRestartPoint(builder->reorder, lsn);

	Assert(builder->state == SNAPBUILD_CONSISTENT);

	ereport(LOG,
			(errmsg("lsn %X/%X: found initial snapshot (xmin %u) in snapbuild file",
					(uint32)(lsn >> 32), (uint32)lsn,
					builder->xmin)));
	return true;

snapshot_not_interesting:
	if (ondisk.builder.running.xip != NULL)
		pfree(ondisk.builder.running.xip);
	if (ondisk.builder.committed.xip != NULL)
		pfree(ondisk.builder.committed.xip);
	return false;
}

/*
 * Remove all serialized snapshots that are not required anymore because no
 * slot can need them. This doesn't actually have to run during a checkpoint,
 * but it's a convenient point to schedule this.
 *
 * NB: We run this during checkpoints even if logical decoding is disabled so
 * we cleanup old slots at some point after it got disabled.
 */
void
CheckPointSnapBuild(void)
{
	XLogRecPtr	cutoff;
	XLogRecPtr	redo;
	DIR		   *snap_dir;
	struct dirent *snap_de;
	char		path[MAXPGPATH];

	/*
	 * We start of with a minimum of the last redo pointer. No new replication
	 * slot will start before that, so that's a safe upper bound for removal.
	 */
	redo = GetRedoRecPtr();

	/* now check for the restart ptrs from existing slots */
	cutoff = ComputeLogicalRestartLSN();

	/* don't start earlier than the restart lsn */
	if (redo < cutoff)
		cutoff = redo;

	snap_dir = AllocateDir("pg_llog/snapshots");
	while ((snap_de = ReadDir(snap_dir, "pg_llog/snapshots")) != NULL)
	{
		uint32		hi;
		uint32		lo;
		XLogRecPtr	lsn;
		struct stat	statbuf;

		if (strcmp(snap_de->d_name, ".") == 0 ||
			strcmp(snap_de->d_name, "..") == 0)
			continue;

		snprintf(path, MAXPGPATH, "pg_llog/snapshots/%s", snap_de->d_name);

		if (lstat(path, &statbuf) == 0 && !S_ISREG(statbuf.st_mode))
			elog(ERROR, "only regular files expected: %s", path);

		/*
		 * temporary filenames from SnapBuildSerialize() include the LSN and
		 * everything but are postfixed by .$pid.tmp. We can just remove them
		 * the same as other files because there can be none that are currently
		 * being written that are older than cutoff.
		 */
		if (sscanf(snap_de->d_name, "%X-%X.snap", &hi, &lo) != 2)
			elog(ERROR, "could not parse filename \"%s\"",
				 snap_de->d_name);

		lsn = ((uint64) hi) << 32 | lo;

		/* check whether we still need it */
		if (lsn < cutoff || cutoff == InvalidXLogRecPtr)
		{
			elog(DEBUG1, "removing snapbuild snapshot %s", path);
			if (unlink(path) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not unlink file \"%s\": %m",
								path)));
		}
	}
	FreeDir(snap_dir);
}
