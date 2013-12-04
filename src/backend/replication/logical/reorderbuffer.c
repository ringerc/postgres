/*-------------------------------------------------------------------------
 *
 * reorderbuffer.c
 *
 * PostgreSQL logical replay buffer management
 *
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/replication/reorderbuffer.c
 *
 * NOTES
 *	  This module gets handed individual pieces of transactions in the order
 *	  they are written to the WAL and is responsible to reassemble them into
 *	  toplevel transaction sized pieces. When a transaction is completely
 *	  reassembled - signalled by reading the transaction commit record - it
 *	  will then call the output plugin (c.f. ReorderBufferCommit()) with the
 *	  individual changes. The output plugins rely on snapshots built by
 *	  snapbuild.c which hands them to us.
 *
 *	  Transactions and subtransactions/savepoints in postgres are not
 *	  immediately linked to each other from outside the performing
 *	  backend. Only at commit/abort (or special xact_assignment records) they
 *	  are linked together. Which means that we will have to splice together a
 *	  toplevel transaction from its subtransactions. To do that efficiently we
 *	  build a binary heap indexed by the smallest current lsn of the individual
 *	  subtransactions' changestreams. As the individual streams are inherently
 *	  ordered by LSN - since that is where we build them from - the transaction
 *	  can easily be reassembled by always using the subtransaction with the
 *	  smallest current LSN from the heap.
 *
 *	  In order to cope with large transactions - which can be several times as
 *	  big as the available memory - this module supports spooling the contents
 *	  of a large transactions to disk. When the transaction is replayed the
 *	  contents of individual (sub-)transactions will be read from disk in
 *	  chunks.
 *
 *	  This module also has to deal with reassembling toast records from the
 *	  individual chunks stored in WAL. When a new (or initial) version of a
 *	  tuple is stored in WAL it will always be preceded by the toast chunks
 *	  emitted for the columns stored out of line. Within a single toplevel
 *	  transaction there will be no other data carrying records between a row's
 *	  toast chunks and the row data itself. See ReorderBufferToast* for
 *	  details.
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "miscadmin.h"

#include "access/transam.h"
#include "access/xact.h"
#include "access/rewriteheap.h"

#include "catalog/catalog.h"

#include "common/relpath.h"

#include "lib/binaryheap.h"

#include "replication/reorderbuffer.h"
#include "replication/snapbuild.h" /* just for SnapBuildSnapDecRefcount */
#include "replication/logical.h"

#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/sinval.h"

#include "utils/builtins.h"
#include "utils/combocid.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/relfilenodemap.h"
#include "utils/resowner.h"
#include "utils/tqual.h"

/*
 * For efficiency and simplicity reasons we want to keep Snapshots, CommandIds
 * and ComboCids in the same list with the user visible INSERT/UPDATE/DELETE
 * changes. We don't want to leak those internal values to external users
 * though (they would just use switch()...default:) because that would make it
 * harder to add to new user visible values.
 *
 * This needs to be synchronized with ReorderBufferChangeType! Adjust the
 * StaticAssertExpr's in ReorderBufferAllocate if you add anything!
 */
typedef enum
{
	REORDER_BUFFER_CHANGE_INTERNAL_INSERT,
	REORDER_BUFFER_CHANGE_INTERNAL_UPDATE,
	REORDER_BUFFER_CHANGE_INTERNAL_DELETE,
	REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT,
	REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID,
	REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID
} ReorderBufferChangeTypeInternal;

/* entry for a hash table we use to map from xid to our transaction state */
typedef struct ReorderBufferTXNByIdEnt
{
	TransactionId xid;
	ReorderBufferTXN *txn;
} ReorderBufferTXNByIdEnt;

/* data structures for (relfilenode, ctid) => (cmin, cmax) mapping */
typedef struct ReorderBufferTupleCidKey
{
	RelFileNode relnode;
	ItemPointerData tid;
} ReorderBufferTupleCidKey;

typedef struct ReorderBufferTupleCidEnt
{
	ReorderBufferTupleCidKey key;
	CommandId	cmin;
	CommandId	cmax;
	CommandId	combocid;		/* just for debugging */
} ReorderBufferTupleCidEnt;

/* k-way in-order change iteration support structures */
typedef struct ReorderBufferIterTXNEntry
{
	XLogRecPtr	lsn;
	ReorderBufferChange *change;
	ReorderBufferTXN *txn;
	int			fd;
	XLogSegNo	segno;
} ReorderBufferIterTXNEntry;

typedef struct ReorderBufferIterTXNState
{
	binaryheap *heap;
	Size		nr_txns;
	dlist_head	old_change;
	ReorderBufferIterTXNEntry entries[FLEXIBLE_ARRAY_MEMBER];
} ReorderBufferIterTXNState;

/* toast datastructures */
typedef struct ReorderBufferToastEnt
{
	Oid			chunk_id;		/* toast_table.chunk_id */
	int32		last_chunk_seq; /* toast_table.chunk_seq of the last chunk we
								 * have seen */
	Size		num_chunks;		/* number of chunks we've already seen */
	Size		size;			/* combined size of chunks seen */
	dlist_head	chunks;			/* linked list of chunks */
	struct varlena *reconstructed;		/* reconstructed varlena now pointed
										 * to in main tup */
} ReorderBufferToastEnt;


/* number of changes kept in memory, per transaction */
const Size	max_memtries = 4096;

/* Size of the slab caches used for frequently allocated objects */
const Size	max_cached_changes = 4096 * 2;
const Size	max_cached_tuplebufs = 4096 * 2;		/* ~8MB */
const Size	max_cached_transactions = 512;


/* ---------------------------------------
 * primary reorderbuffer support routines
 * ---------------------------------------
 */
static ReorderBufferTXN *ReorderBufferGetTXN(ReorderBuffer *rb);
static void ReorderBufferReturnTXN(ReorderBuffer *rb, ReorderBufferTXN *txn);
static ReorderBufferTXN *ReorderBufferTXNByXid(ReorderBuffer *rb,
					  TransactionId xid, bool create, bool *is_new,
					  XLogRecPtr lsn, bool create_as_top);

static void AssertTXNLsnOrder(ReorderBuffer *rb);

/* ---------------------------------------
 * support functions for lsn-order iterating over the ->changes of a
 * transaction and its subtransactions
 *
 * used for iteration over the k-way heap merge of a transaction and its
 * subtransactions
 * ---------------------------------------
 */
static ReorderBufferIterTXNState *ReorderBufferIterTXNInit(ReorderBuffer *rb, ReorderBufferTXN *txn);
static ReorderBufferChange *
			ReorderBufferIterTXNNext(ReorderBuffer *rb, ReorderBufferIterTXNState *state);
static void ReorderBufferIterTXNFinish(ReorderBuffer *rb,
						   ReorderBufferIterTXNState *state);
static void ReorderBufferExecuteInvalidations(ReorderBuffer *rb, ReorderBufferTXN *txn);

/*
 * ---------------------------------------
 * Disk serialization support functions
 * ---------------------------------------
 */
static void ReorderBufferCheckSerializeTXN(ReorderBuffer *rb, ReorderBufferTXN *txn);
static void ReorderBufferSerializeTXN(ReorderBuffer *rb, ReorderBufferTXN *txn);
static void ReorderBufferSerializeChange(ReorderBuffer *rb, ReorderBufferTXN *txn,
							 int fd, ReorderBufferChange *change);
static Size ReorderBufferRestoreChanges(ReorderBuffer *rb, ReorderBufferTXN *txn,
							int *fd, XLogSegNo *segno);
static void ReorderBufferRestoreChange(ReorderBuffer *rb, ReorderBufferTXN *txn,
						   char *change);
static void ReorderBufferRestoreCleanup(ReorderBuffer *rb, ReorderBufferTXN *txn);

static void ReorderBufferFreeSnap(ReorderBuffer *rb, Snapshot snap);
static Snapshot ReorderBufferCopySnap(ReorderBuffer *rb, Snapshot orig_snap,
					  ReorderBufferTXN *txn, CommandId cid);

/* ---------------------------------------
 * toast reassembly support
 * ---------------------------------------
 */
/* Size of an EXTERNAL datum that contains a standard TOAST pointer */
#define TOAST_POINTER_SIZE (VARHDRSZ_EXTERNAL + sizeof(struct varatt_external))

/* Size of an indirect datum that contains a standard TOAST pointer */
#define INDIRECT_POINTER_SIZE (VARHDRSZ_EXTERNAL + sizeof(struct varatt_indirect))

static void ReorderBufferToastInitHash(ReorderBuffer *rb, ReorderBufferTXN *txn);
static void ReorderBufferToastReset(ReorderBuffer *rb, ReorderBufferTXN *txn);
static void ReorderBufferToastReplace(ReorderBuffer *rb, ReorderBufferTXN *txn,
						  Relation relation, ReorderBufferChange *change);
static void ReorderBufferToastAppendChunk(ReorderBuffer *rb, ReorderBufferTXN *txn,
							  Relation relation, ReorderBufferChange *change);


/*
 * Allocate a new ReorderBuffer
 */
ReorderBuffer *
ReorderBufferAllocate(void)
{
	ReorderBuffer *buffer;
	HASHCTL		hash_ctl;
	MemoryContext new_ctx;

	StaticAssertExpr((int) REORDER_BUFFER_CHANGE_INTERNAL_INSERT == (int) REORDER_BUFFER_CHANGE_INSERT, "out of sync enums");
	StaticAssertExpr((int) REORDER_BUFFER_CHANGE_INTERNAL_UPDATE == (int) REORDER_BUFFER_CHANGE_UPDATE, "out of sync enums");
	StaticAssertExpr((int) REORDER_BUFFER_CHANGE_INTERNAL_DELETE == (int) REORDER_BUFFER_CHANGE_DELETE, "out of sync enums");

	new_ctx = AllocSetContextCreate(TopMemoryContext,
									"ReorderBuffer",
									ALLOCSET_DEFAULT_MINSIZE,
									ALLOCSET_DEFAULT_INITSIZE,
									ALLOCSET_DEFAULT_MAXSIZE);

	buffer =
		(ReorderBuffer *) MemoryContextAlloc(new_ctx, sizeof(ReorderBuffer));

	memset(&hash_ctl, 0, sizeof(hash_ctl));

	buffer->context = new_ctx;

	hash_ctl.keysize = sizeof(TransactionId);
	hash_ctl.entrysize = sizeof(ReorderBufferTXNByIdEnt);
	hash_ctl.hash = tag_hash;
	hash_ctl.hcxt = buffer->context;

	buffer->by_txn = hash_create("ReorderBufferByXid", 1000, &hash_ctl,
								 HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	buffer->by_txn_last_xid = InvalidTransactionId;
	buffer->by_txn_last_txn = NULL;

	buffer->nr_cached_transactions = 0;
	buffer->nr_cached_changes = 0;
	buffer->nr_cached_tuplebufs = 0;

	buffer->outbuf = NULL;
	buffer->outbufsize = 0;

	buffer->current_restart_decoding_lsn = InvalidXLogRecPtr;

	dlist_init(&buffer->toplevel_by_lsn);
	dlist_init(&buffer->cached_transactions);
	dlist_init(&buffer->cached_changes);
	slist_init(&buffer->cached_tuplebufs);

	return buffer;
}

/*
 * Free a ReorderBuffer
 */
void
ReorderBufferFree(ReorderBuffer *rb)
{
	MemoryContext context = rb->context;

	/*
	 * We free separately allocated data by entirely scrapping oure personal
	 * memory context.
	 */
	MemoryContextDelete(context);
}

/*
 * Get a unused, possibly preallocated, ReorderBufferTXN.
 */
static ReorderBufferTXN *
ReorderBufferGetTXN(ReorderBuffer *rb)
{
	ReorderBufferTXN *txn;

	if (rb->nr_cached_transactions > 0)
	{
		rb->nr_cached_transactions--;
		txn = (ReorderBufferTXN *)
			dlist_container(ReorderBufferTXN, node,
							dlist_pop_head_node(&rb->cached_transactions));
	}
	else
	{
		txn = (ReorderBufferTXN *)
			MemoryContextAlloc(rb->context, sizeof(ReorderBufferTXN));
	}

	memset(txn, 0, sizeof(ReorderBufferTXN));

	dlist_init(&txn->changes);
	dlist_init(&txn->tuplecids);
	dlist_init(&txn->subtxns);

	return txn;
}

/*
 * Free an ReorderBufferTXN. Deallocation might be delayed for efficiency
 * purposes.
 */
void
ReorderBufferReturnTXN(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
	/* clean the lookup cache if we were cached (quite likely) */
	if (rb->by_txn_last_xid == txn->xid)
	{
		rb->by_txn_last_xid = InvalidTransactionId;
		rb->by_txn_last_txn = NULL;
	}

	if (txn->tuplecid_hash != NULL)
	{
		hash_destroy(txn->tuplecid_hash);
		txn->tuplecid_hash = NULL;
	}

	if (txn->invalidations)
	{
		pfree(txn->invalidations);
		txn->invalidations = NULL;
	}

	if (rb->nr_cached_transactions < max_cached_transactions)
	{
		rb->nr_cached_transactions++;
		dlist_push_head(&rb->cached_transactions, &txn->node);
		VALGRIND_MAKE_MEM_UNDEFINED(txn, sizeof(ReorderBufferTXN));
		VALGRIND_MAKE_MEM_DEFINED(&txn->node, sizeof(txn->node));
	}
	else
	{
		pfree(txn);
	}
}

/*
 * Get a unused, possibly preallocated, ReorderBufferChange.
 */
ReorderBufferChange *
ReorderBufferGetChange(ReorderBuffer *rb)
{
	ReorderBufferChange *change;

	if (rb->nr_cached_changes)
	{
		rb->nr_cached_changes--;
		change = (ReorderBufferChange *)
			dlist_container(ReorderBufferChange, node,
							dlist_pop_head_node(&rb->cached_changes));
	}
	else
	{
		change = (ReorderBufferChange *)
			MemoryContextAlloc(rb->context, sizeof(ReorderBufferChange));
	}

	memset(change, 0, sizeof(ReorderBufferChange));
	return change;
}

/*
 * Free an ReorderBufferChange. Deallocation might be delayed for efficiency
 * purposes.
 */
void
ReorderBufferReturnChange(ReorderBuffer *rb, ReorderBufferChange *change)
{
	switch ((ReorderBufferChangeTypeInternal) change->action_internal)
	{
		case REORDER_BUFFER_CHANGE_INTERNAL_INSERT:
		case REORDER_BUFFER_CHANGE_INTERNAL_UPDATE:
		case REORDER_BUFFER_CHANGE_INTERNAL_DELETE:
			if (change->tp.newtuple)
			{
				ReorderBufferReturnTupleBuf(rb, change->tp.newtuple);
				change->tp.newtuple = NULL;
			}

			if (change->tp.oldtuple)
			{
				ReorderBufferReturnTupleBuf(rb, change->tp.oldtuple);
				change->tp.oldtuple = NULL;
			}
			break;
		case REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT:
			if (change->snapshot)
			{
				ReorderBufferFreeSnap(rb, change->snapshot);
				change->snapshot = NULL;
			}
			break;
		case REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID:
			break;
		case REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID:
			break;
	}

	if (rb->nr_cached_changes < max_cached_changes)
	{
		rb->nr_cached_changes++;
		dlist_push_head(&rb->cached_changes, &change->node);
		VALGRIND_MAKE_MEM_UNDEFINED(change, sizeof(ReorderBufferChange));
		VALGRIND_MAKE_MEM_DEFINED(&change->node, sizeof(change->node));
	}
	else
	{
		pfree(change);
	}
}


/*
 * Get a unused, possibly preallocated, ReorderBufferTupleBuf
 */
ReorderBufferTupleBuf *
ReorderBufferGetTupleBuf(ReorderBuffer *rb)
{
	ReorderBufferTupleBuf *tuple;

	if (rb->nr_cached_tuplebufs)
	{
		rb->nr_cached_tuplebufs--;
		tuple = slist_container(ReorderBufferTupleBuf, node,
								slist_pop_head_node(&rb->cached_tuplebufs));
#ifdef USE_ASSERT_CHECKING
		memset(tuple, 0xdeadbeef, sizeof(ReorderBufferTupleBuf));
#endif
	}
	else
	{
		tuple = (ReorderBufferTupleBuf *)
			MemoryContextAlloc(rb->context, sizeof(ReorderBufferTupleBuf));
	}

	return tuple;
}

/*
 * Free an ReorderBufferTupleBuf. Deallocation might be delayed for efficiency
 * purposes.
 */
void
ReorderBufferReturnTupleBuf(ReorderBuffer *rb, ReorderBufferTupleBuf *tuple)
{
	if (rb->nr_cached_tuplebufs < max_cached_tuplebufs)
	{
		rb->nr_cached_tuplebufs++;
		slist_push_head(&rb->cached_tuplebufs, &tuple->node);
		VALGRIND_MAKE_MEM_UNDEFINED(tuple, sizeof(ReorderBufferTupleBuf));
		VALGRIND_MAKE_MEM_DEFINED(&tuple->node, sizeof(tuple->node));
	}
	else
	{
		pfree(tuple);
	}
}

/*
 * Return the ReorderBufferTXN from the given buffer, specified by Xid.
 * If create is true, and a transaction doesn't already exist, create it
 * (with the given LSN, and as top transaction if that's specified);
 * when this happens, is_new is set to true.
 */
static ReorderBufferTXN *
ReorderBufferTXNByXid(ReorderBuffer *rb, TransactionId xid, bool create,
					  bool *is_new, XLogRecPtr lsn, bool create_as_top)
{
	ReorderBufferTXN *txn;
	ReorderBufferTXNByIdEnt *ent;
	bool		found;

	Assert(TransactionIdIsValid(xid));
	Assert(!create || lsn != InvalidXLogRecPtr);

	/*
	 * Check the one-entry lookup cache first
	 */
	if (TransactionIdIsValid(rb->by_txn_last_xid) &&
		rb->by_txn_last_xid == xid)
	{
		txn = rb->by_txn_last_txn;

		if (txn != NULL)
		{
			/* found it, and it's valid */
			if (is_new)
				*is_new = false;
			return txn;
		}

		/*
		 * cached as non-existant, and asked not to create? Then nothing else
		 * to do.
		 */
		if (!create)
			return NULL;
		/* otherwise fall through to create it */
	}

	/*
	 * If the cache wasn't hit or it yielded an "does-not-exist" and we want
	 * to create an entry.
	 */

	/* search the lookup table */
	ent = (ReorderBufferTXNByIdEnt *)
		hash_search(rb->by_txn,
					(void *) &xid,
					create ? HASH_ENTER : HASH_FIND,
					&found);
	if (found)
		txn = ent->txn;
	else if (create)
	{
		/* initialize the new entry, if creation was requested */
		Assert(ent != NULL);

		ent->txn = ReorderBufferGetTXN(rb);
		ent->txn->xid = xid;
		txn = ent->txn;
		txn->first_lsn = lsn;
		txn->restart_decoding_lsn = rb->current_restart_decoding_lsn;

		if (create_as_top)
		{
			dlist_push_tail(&rb->toplevel_by_lsn, &txn->node);
			AssertTXNLsnOrder(rb);
		}
	}
	else
		txn = NULL;				/* not found and not asked to create */

	/* update cache */
	rb->by_txn_last_xid = xid;
	rb->by_txn_last_txn = txn;

	if (is_new)
		*is_new = !found;

	Assert(!create || !!txn);
	return txn;
}

/*
 * Queue a change into a transaction so it can be replayed upon commit.
 */
void
ReorderBufferQueueChange(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn,
					   ReorderBufferChange *change)
{
	ReorderBufferTXN *txn;

	txn = ReorderBufferTXNByXid(rb, xid, true, NULL, lsn, true);

	change->lsn = lsn;
	Assert(InvalidXLogRecPtr != lsn);
	dlist_push_tail(&txn->changes, &change->node);
	txn->nentries++;
	txn->nentries_mem++;

	ReorderBufferCheckSerializeTXN(rb, txn);
}

static void
AssertTXNLsnOrder(ReorderBuffer *rb)
{
#ifdef USE_ASSERT_CHECKING
	dlist_iter	iter;
	XLogRecPtr	prev_first_lsn = InvalidXLogRecPtr;

	dlist_foreach(iter, &rb->toplevel_by_lsn)
	{
		ReorderBufferTXN *cur_txn;

		cur_txn = dlist_container(ReorderBufferTXN, node, iter.cur);
		Assert(cur_txn->first_lsn != InvalidXLogRecPtr);

		if (cur_txn->end_lsn != InvalidXLogRecPtr)
			Assert(cur_txn->first_lsn <= cur_txn->end_lsn);

		if (prev_first_lsn != InvalidXLogRecPtr)
			Assert(prev_first_lsn < cur_txn->first_lsn);

		Assert(!cur_txn->is_known_as_subxact);
		prev_first_lsn = cur_txn->first_lsn;
	}
#endif
}

ReorderBufferTXN *
ReorderBufferGetOldestTXN(ReorderBuffer *rb)
{
	ReorderBufferTXN *txn;

	if (dlist_is_empty(&rb->toplevel_by_lsn))
		return NULL;

	AssertTXNLsnOrder(rb);

	txn = dlist_head_element(ReorderBufferTXN, node, &rb->toplevel_by_lsn);

	Assert(!txn->is_known_as_subxact);
	Assert(txn->first_lsn != InvalidXLogRecPtr);
	return txn;
}

void
ReorderBufferSetRestartPoint(ReorderBuffer *rb, XLogRecPtr ptr)
{
	rb->current_restart_decoding_lsn = ptr;
}

void
ReorderBufferAssignChild(ReorderBuffer *rb, TransactionId xid,
						 TransactionId subxid, XLogRecPtr lsn)
{
	ReorderBufferTXN *txn;
	ReorderBufferTXN *subtxn;
	bool		new_top;
	bool		new_sub;

	txn = ReorderBufferTXNByXid(rb, xid, true, &new_top, lsn, true);
	subtxn = ReorderBufferTXNByXid(rb, subxid, true, &new_sub, lsn, false);

	if (new_sub)
	{
		/*
		 * we assign subtransactions to top level transaction even if we don't
		 * have data for it yet, assignment records frequently reference xids
		 * that have not yet produced any records. Knowing those aren't top
		 * level xids allows us to make processing cheaper in some places.
		 */
		dlist_push_tail(&txn->subtxns, &subtxn->node);
		txn->nsubtxns++;
	}
	else if (!subtxn->is_known_as_subxact)
	{
		subtxn->is_known_as_subxact = true;
		Assert(subtxn->nsubtxns == 0);

		/* remove from lsn order list of top-level transactions */
		dlist_delete(&subtxn->node);

		/* add to toplevel transaction */
		dlist_push_tail(&txn->subtxns, &subtxn->node);
		txn->nsubtxns++;
	}
	else if (new_top)
	{
		elog(ERROR, "existing subxact assigned to unknown toplevel xact");
	}
}

/*
 * Associate a subtransaction with its toplevel transaction at commit
 * time. There may be no further changes added after this.
 */
void
ReorderBufferCommitChild(ReorderBuffer *rb, TransactionId xid,
						 TransactionId subxid, XLogRecPtr commit_lsn,
						 XLogRecPtr end_lsn)
{
	ReorderBufferTXN *txn;
	ReorderBufferTXN *subtxn;

	subtxn = ReorderBufferTXNByXid(rb, subxid, false, NULL,
								   InvalidXLogRecPtr, false);

	/*
	 * No need to do anything if that subtxn didn't contain any changes
	 */
	if (!subtxn)
		return;

	txn = ReorderBufferTXNByXid(rb, xid, false, NULL, commit_lsn, true);

	if (txn == NULL)
		elog(ERROR, "subxact logged without previous toplevel record");

	subtxn->final_lsn = commit_lsn;
	subtxn->end_lsn = end_lsn;

	if (!subtxn->is_known_as_subxact)
	{
		subtxn->is_known_as_subxact = true;
		Assert(subtxn->nsubtxns == 0);

		/* remove from lsn order list of top-level transactions */
		dlist_delete(&subtxn->node);

		/* add to subtransaction list */
		dlist_push_tail(&txn->subtxns, &subtxn->node);
		txn->nsubtxns++;
	}
}


/*
 * Support for efficiently iterating over a transaction's and its
 * subtransactions' changes.
 *
 * We do by doing a k-way merge between transactions/subtransactions. For that
 * we model the current heads of the different transactions as a binary heap
 * so we easily know which (sub-)transaction has the change with the smallest
 * lsn next.
 *
 * We assume the changes in individual transactions are already sorted by LSN.
 */

/*
 * Binary heap comparison function.
 */
static int
ReorderBufferIterCompare(Datum a, Datum b, void *arg)
{
	ReorderBufferIterTXNState *state = (ReorderBufferIterTXNState *) arg;
	XLogRecPtr	pos_a = state->entries[DatumGetInt32(a)].lsn;
	XLogRecPtr	pos_b = state->entries[DatumGetInt32(b)].lsn;

	if (pos_a < pos_b)
		return 1;
	else if (pos_a == pos_b)
		return 0;
	return -1;
}

/*
 * Allocate & initialize an iterator which iterates in lsn order over a
 * transaction and all its subtransactions.
 */
static ReorderBufferIterTXNState *
ReorderBufferIterTXNInit(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
	Size		nr_txns = 0;
	ReorderBufferIterTXNState *state;
	dlist_iter	cur_txn_i;
	int32		off;

	/*
	 * Calculate the size of our heap: one element for every transaction that
	 * contains changes.  (Besides the transactions already in the reorder
	 * buffer, we count the one we were directly passed.)
	 */
	if (txn->nentries > 0)
		nr_txns++;

	dlist_foreach(cur_txn_i, &txn->subtxns)
	{
		ReorderBufferTXN *cur_txn;

		cur_txn = dlist_container(ReorderBufferTXN, node, cur_txn_i.cur);

		if (cur_txn->nentries > 0)
			nr_txns++;
	}

	/*
	 * XXX: Add fastpath for the rather common nr_txns=1 case, no need to
	 * allocate/build a heap in that case.
	 */

	/* allocate iteration state */
	state = (ReorderBufferIterTXNState *)
		MemoryContextAllocZero(rb->context,
							   sizeof(ReorderBufferIterTXNState) +
							   sizeof(ReorderBufferIterTXNEntry) * nr_txns);

	state->nr_txns = nr_txns;
	dlist_init(&state->old_change);

	for (off = 0; off < state->nr_txns; off++)
	{
		state->entries[off].fd = -1;
		state->entries[off].segno = 0;
	}

	/* allocate heap */
	state->heap = binaryheap_allocate(state->nr_txns,
									  ReorderBufferIterCompare,
									  state);

	/*
	 * Now insert items into the binary heap, unordered.  (We will run a heap
	 * assembly step at the end; this is more efficient.)
	 */

	off = 0;

	/* add toplevel transaction if it contains changes */
	if (txn->nentries > 0)
	{
		ReorderBufferChange *cur_change;

		if (txn->nentries != txn->nentries_mem)
			ReorderBufferRestoreChanges(rb, txn, &state->entries[off].fd,
										&state->entries[off].segno);

		cur_change = dlist_head_element(ReorderBufferChange, node,
										&txn->changes);

		state->entries[off].lsn = cur_change->lsn;
		state->entries[off].change = cur_change;
		state->entries[off].txn = txn;

		binaryheap_add_unordered(state->heap, Int32GetDatum(off++));
	}

	/* add subtransactions if they contain changes */
	dlist_foreach(cur_txn_i, &txn->subtxns)
	{
		ReorderBufferTXN *cur_txn;

		cur_txn = dlist_container(ReorderBufferTXN, node, cur_txn_i.cur);

		if (cur_txn->nentries > 0)
		{
			ReorderBufferChange *cur_change;

			if (txn->nentries != txn->nentries_mem)
				ReorderBufferRestoreChanges(rb, cur_txn,
											&state->entries[off].fd,
											&state->entries[off].segno);

			cur_change = dlist_head_element(ReorderBufferChange, node,
											&cur_txn->changes);

			state->entries[off].lsn = cur_change->lsn;
			state->entries[off].change = cur_change;
			state->entries[off].txn = cur_txn;

			binaryheap_add_unordered(state->heap, Int32GetDatum(off++));
		}
	}

	/* assemble a valid binary heap */
	binaryheap_build(state->heap);

	return state;
}

/*
 * FIXME: better comment and/or name
 */
static void
ReorderBufferRestoreCleanup(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
	XLogSegNo	first;
	XLogSegNo	cur;
	XLogSegNo	last;

	Assert(txn->first_lsn != InvalidXLogRecPtr);
	Assert(txn->final_lsn != InvalidXLogRecPtr);

	XLByteToSeg(txn->first_lsn, first);
	XLByteToSeg(txn->final_lsn, last);

	/* iterate over all possible filenames, and delete them */
	for (cur = first; cur <= last; cur++)
	{
		char		path[MAXPGPATH];
		XLogRecPtr	recptr;

		XLogSegNoOffsetToRecPtr(cur, 0, recptr);

		sprintf(path, "pg_llog/%s/xid-%u-lsn-%X-%X.snap",
				NameStr(MyLogicalDecodingSlot->name), txn->xid,
				(uint32) (recptr >> 32), (uint32) recptr);
		if (unlink(path) != 0 && errno != ENOENT)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not unlink file \"%s\": %m", path)));
	}
}

/*
 * Return the next change when iterating over a transaction and its
 * subtransaction.
 *
 * Returns NULL when no further changes exist.
 */
static ReorderBufferChange *
ReorderBufferIterTXNNext(ReorderBuffer *rb, ReorderBufferIterTXNState *state)
{
	ReorderBufferChange *change;
	ReorderBufferIterTXNEntry *entry;
	int32		off;

	/* nothing there anymore */
	if (state->heap->bh_size == 0)
		return NULL;

	off = DatumGetInt32(binaryheap_first(state->heap));
	entry = &state->entries[off];

	/* free memory we might have "leaked" in the previous *Next call */
	if (!dlist_is_empty(&state->old_change))
	{
		change = dlist_container(ReorderBufferChange, node,
								 dlist_pop_head_node(&state->old_change));
		ReorderBufferReturnChange(rb, change);
		Assert(dlist_is_empty(&state->old_change));
	}

	change = entry->change;

	/*
	 * update heap with information about which transaction has the next
	 * relevant change in LSN order
	 */

	/* there are in-memory changes */
	if (dlist_has_next(&entry->txn->changes, &entry->change->node))
	{
		dlist_node *next = dlist_next_node(&entry->txn->changes, &change->node);
		ReorderBufferChange *next_change =
		dlist_container(ReorderBufferChange, node, next);

		/* txn stays the same */
		state->entries[off].lsn = next_change->lsn;
		state->entries[off].change = next_change;

		binaryheap_replace_first(state->heap, Int32GetDatum(off));
		return change;
	}

	/* try to load changes from disk */
	if (entry->txn->nentries != entry->txn->nentries_mem)
	{
		/*
		 * Ugly: restoring changes will reuse *Change records, thus delete the
		 * current one from the per-tx list and only free in the next call.
		 */
		dlist_delete(&change->node);
		dlist_push_tail(&state->old_change, &change->node);

		if (ReorderBufferRestoreChanges(rb, entry->txn, &entry->fd,
										&state->entries[off].segno))
		{
			/* successfully restored changes from disk */
			ReorderBufferChange *next_change =
			dlist_head_element(ReorderBufferChange, node,
							   &entry->txn->changes);

			elog(DEBUG2, "restored %u/%u changes from disk",
				 (uint32) entry->txn->nentries_mem,
				 (uint32) entry->txn->nentries);

			Assert(entry->txn->nentries_mem);
			/* txn stays the same */
			state->entries[off].lsn = next_change->lsn;
			state->entries[off].change = next_change;
			binaryheap_replace_first(state->heap, Int32GetDatum(off));

			return change;
		}
	}

	/* ok, no changes there anymore, remove */
	binaryheap_remove_first(state->heap);

	return change;
}

/*
 * Deallocate the iterator
 */
static void
ReorderBufferIterTXNFinish(ReorderBuffer *rb,
						   ReorderBufferIterTXNState *state)
{
	int32		off;

	for (off = 0; off < state->nr_txns; off++)
	{
		if (state->entries[off].fd != -1)
			CloseTransientFile(state->entries[off].fd);
	}

	/* free memory we might have "leaked" in the last *Next call */
	if (!dlist_is_empty(&state->old_change))
	{
		ReorderBufferChange *change;

		change = dlist_container(ReorderBufferChange, node,
								 dlist_pop_head_node(&state->old_change));
		ReorderBufferReturnChange(rb, change);
		Assert(dlist_is_empty(&state->old_change));
	}

	binaryheap_free(state->heap);
	pfree(state);
}

/*
 * Cleanup the contents of a transaction, usually after the transaction
 * committed or aborted.
 */
static void
ReorderBufferCleanupTXN(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
	bool		found;
	dlist_mutable_iter iter;

	/* cleanup subtransactions & their changes */
	dlist_foreach_modify(iter, &txn->subtxns)
	{
		ReorderBufferTXN *subtxn;

		subtxn = dlist_container(ReorderBufferTXN, node, iter.cur);

		/*
		 * Subtransactions are always associated to the toplevel TXN, even if
		 * they originally were happening inside another subtxn, so we won't
		 * ever recurse more than one level deep here.
		 */
		Assert(subtxn->is_known_as_subxact);
		Assert(subtxn->nsubtxns == 0);

		ReorderBufferCleanupTXN(rb, subtxn);
	}

	/* cleanup changes in the toplevel txn */
	dlist_foreach_modify(iter, &txn->changes)
	{
		ReorderBufferChange *change;

		change = dlist_container(ReorderBufferChange, node, iter.cur);

		ReorderBufferReturnChange(rb, change);
	}

	/*
	 * Cleanup the tuplecids we stored for decoding catalog snapshot
	 * access. They are always stored in the toplevel transaction.
	 */
	dlist_foreach_modify(iter, &txn->tuplecids)
	{
		ReorderBufferChange *change;

		change = dlist_container(ReorderBufferChange, node, iter.cur);
		Assert(change->action_internal == REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID);
		ReorderBufferReturnChange(rb, change);
	}

	if (txn->base_snapshot != NULL)
	{
		SnapBuildSnapDecRefcount(txn->base_snapshot);
		txn->base_snapshot = NULL;
	}

	/* delete from list of known subxacts */
	if (txn->is_known_as_subxact)
	{
		/* FIXME: adjust nsubxacts count of parent */
		dlist_delete(&txn->node);
	}
	/* delete from LSN ordered list of toplevel TXNs */
	else
	{
		dlist_delete(&txn->node);
	}

	/* now remove reference from buffer */
	hash_search(rb->by_txn,
				(void *) &txn->xid,
				HASH_REMOVE,
				&found);
	Assert(found);

	/* remove entries spilled to disk */
	if (txn->nentries != txn->nentries_mem)
		ReorderBufferRestoreCleanup(rb, txn);

	/* deallocate */
	ReorderBufferReturnTXN(rb, txn);
}

/*
 * Build a hash with a (relfilenode, ctid) -> (cmin, cmax) mapping for use by
 * tqual.c's HeapTupleSatisfiesMVCCDuringDecoding.
 */
static void
ReorderBufferBuildTupleCidHash(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
	dlist_iter	iter;
	HASHCTL		hash_ctl;

	if (!txn->has_catalog_changes || dlist_is_empty(&txn->tuplecids))
		return;

	memset(&hash_ctl, 0, sizeof(hash_ctl));

	hash_ctl.keysize = sizeof(ReorderBufferTupleCidKey);
	hash_ctl.entrysize = sizeof(ReorderBufferTupleCidEnt);
	hash_ctl.hash = tag_hash;
	hash_ctl.hcxt = rb->context;

	/*
	 * create the hash with the exact number of to-be-stored tuplecids from
	 * the start
	 */
	txn->tuplecid_hash =
		hash_create("ReorderBufferTupleCid", txn->ntuplecids, &hash_ctl,
					HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	dlist_foreach(iter, &txn->tuplecids)
	{
		ReorderBufferTupleCidKey key;
		ReorderBufferTupleCidEnt *ent;
		bool		found;
		ReorderBufferChange *change;

		change = dlist_container(ReorderBufferChange, node, iter.cur);

		Assert(change->action_internal == REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID);

		/* be careful about padding */
		memset(&key, 0, sizeof(ReorderBufferTupleCidKey));

		key.relnode = change->tuplecid.node;

		ItemPointerCopy(&change->tuplecid.tid,
						&key.tid);

		ent = (ReorderBufferTupleCidEnt *)
			hash_search(txn->tuplecid_hash,
						(void *) &key,
						HASH_ENTER | HASH_FIND,
						&found);
		if (!found)
		{
			ent->cmin = change->tuplecid.cmin;
			ent->cmax = change->tuplecid.cmax;
			ent->combocid = change->tuplecid.combocid;
		}
		else
		{
			Assert(ent->cmin == change->tuplecid.cmin);
			Assert(ent->cmax == InvalidCommandId ||
				   ent->cmax == change->tuplecid.cmax);

			/*
			 * if the tuple got valid in this transaction and now got deleted
			 * we already have a valid cmin stored. The cmax will be
			 * InvalidCommandId though.
			 */
			ent->cmax = change->tuplecid.cmax;
		}
	}
}

/*
 * Copy a provided snapshot so we can modify it privately. This is needed so
 * that catalog modifying transactions can look into intermediate catalog
 * states.
 */
static Snapshot
ReorderBufferCopySnap(ReorderBuffer *rb, Snapshot orig_snap,
					  ReorderBufferTXN *txn, CommandId cid)
{
	Snapshot	snap;
	dlist_iter	iter;
	int			i = 0;
	Size		size;

	size = sizeof(SnapshotData) +
		sizeof(TransactionId) * orig_snap->xcnt +
		sizeof(TransactionId) * (txn->nsubtxns + 1);

	snap = MemoryContextAllocZero(rb->context, size);
	memcpy(snap, orig_snap, sizeof(SnapshotData));

	snap->copied = true;
	snap->active_count = 0;
	snap->regd_count = 0;
	snap->xip = (TransactionId *) (snap + 1);

	memcpy(snap->xip, orig_snap->xip, sizeof(TransactionId) * snap->xcnt);

	/*
	 * ->subxip contains all txids that belong to our transaction which we
	 * need to check via cmin/cmax. Thats why we store the toplevel
	 * transaction in there as well.
	 */
	snap->subxip = snap->xip + snap->xcnt;
	snap->subxip[i++] = txn->xid;

	/*
	 * XXX: ->nsubxcnt can be out of date when subtransactions abort, count
	 * manually.
	 */
	snap->subxcnt = 1;

	dlist_foreach(iter, &txn->subtxns)
	{
		ReorderBufferTXN *sub_txn;

		sub_txn = dlist_container(ReorderBufferTXN, node, iter.cur);
		snap->subxip[i++] = sub_txn->xid;
		snap->subxcnt++;
	}

	/* sort so we can bsearch() later */
	qsort(snap->subxip, snap->subxcnt, sizeof(TransactionId), xidComparator);

	/* store the specified current CommandId */
	snap->curcid = cid;

	return snap;
}

/*
 * Free a previously ReorderBufferCopySnap'ed snapshot
 */
static void
ReorderBufferFreeSnap(ReorderBuffer *rb, Snapshot snap)
{
	if (snap->copied)
		pfree(snap);
	else
		SnapBuildSnapDecRefcount(snap);
}

/*
 * Commit a transaction and replay all actions that previously have been
 * ReorderBufferQueueChange'd in the toplevel TX or any of the subtransactions
 * assigned via ReorderBufferCommitChild.
 */
void
ReorderBufferCommit(ReorderBuffer *rb, TransactionId xid,
					XLogRecPtr commit_lsn, XLogRecPtr end_lsn,
					TimestampTz commit_time)
{
	ReorderBufferTXN *txn;
	ReorderBufferIterTXNState *iterstate = NULL;
	ReorderBufferChange *change;

	volatile CommandId	command_id = FirstCommandId;
	volatile Snapshot	snapshot_now;
	volatile bool		tx_started = false;
	volatile bool		subtx_started = false;

	txn = ReorderBufferTXNByXid(rb, xid, false, NULL, InvalidXLogRecPtr,
								false);

	/* empty transaction */
	if (txn == NULL)
		return;

	txn->final_lsn = commit_lsn;
	txn->end_lsn = end_lsn;
	txn->commit_time = commit_time;

	/* serialize the last bunch of changes if we need start earlier anyway */
	if (txn->nentries_mem != txn->nentries)
		ReorderBufferSerializeTXN(rb, txn);

	/*
	 * If this transaction didn't have any real changes in our database, it's
	 * OK not to have a snapshot.
	 */
	if (txn->base_snapshot == NULL)
	{
		Assert(txn->ninvalidations == 0);
		ReorderBufferCleanupTXN(rb, txn);
		return;
	}

	snapshot_now = txn->base_snapshot;

	ReorderBufferBuildTupleCidHash(rb, txn);

	/* setup initial snapshot */
	SetupDecodingSnapshots(snapshot_now, txn->tuplecid_hash);

	PG_TRY();
	{
		tx_started = false;

		/*
		 * Decoding needs access to syscaches et al., which in turn use
		 * heavyweight locks and such. Thus we need to have enough state around
		 * to keep track of those. The easiest way is to simply use a
		 * transaction internally. That also allows us to easily enforce that
		 * nothing writes to the database by checking for xid assignments.
		 *
		 * When we're called via the SQL SRF there's already a transaction
		 * started, so start an explicit subtransaction there.
		 */
		if (IsTransactionOrTransactionBlock())
		{
			BeginInternalSubTransaction("replay");
			subtx_started = true;

			if (GetTopTransactionIdIfAny() != InvalidTransactionId)
				elog(ERROR, "cannot replay using sub, already allocated xid %u",
					 GetTopTransactionIdIfAny());
		}
		else
		{
			StartTransactionCommand();
			tx_started = true;

			if (GetTopTransactionIdIfAny() != InvalidTransactionId)
				elog(ERROR, "cannot replay using top, already allocated xid %u",
					 GetTopTransactionIdIfAny());

		}

		rb->begin(rb, txn);

		iterstate = ReorderBufferIterTXNInit(rb, txn);
		while ((change = ReorderBufferIterTXNNext(rb, iterstate)))
		{
			Relation	relation = NULL;
			Oid			reloid;

			switch ((ReorderBufferChangeTypeInternal) change->action_internal)
			{
				case REORDER_BUFFER_CHANGE_INTERNAL_INSERT:
				case REORDER_BUFFER_CHANGE_INTERNAL_UPDATE:
				case REORDER_BUFFER_CHANGE_INTERNAL_DELETE:
					Assert(snapshot_now);

					reloid = RelidByRelfilenode(change->tp.relnode.spcNode,
												change->tp.relnode.relNode);

					/*
					 * catalog tuple without data, while catalog has been
					 * rewritten
					 */
					if (reloid == InvalidOid &&
						change->tp.newtuple == NULL &&
						change->tp.oldtuple == NULL)
						continue;
					else if (reloid == InvalidOid)
						elog(ERROR, "could not lookup relation %s",
							 relpathperm(change->tp.relnode, MAIN_FORKNUM));

					relation = RelationIdGetRelation(reloid);

					if (relation == NULL)
						elog(ERROR, "could open relation descriptor %s",
							 relpathperm(change->tp.relnode, MAIN_FORKNUM));

					if (RelationIsLogicallyLogged(relation))
					{
						/* user-triggered change */
						if (relation->rd_rel->relkind == RELKIND_SEQUENCE)
						{
						}
						else if (!IsToastRelation(relation))
						{
							ReorderBufferToastReplace(rb, txn, relation, change);
							rb->apply_change(rb, txn, relation, change);
							ReorderBufferToastReset(rb, txn);
						}
						/* we're not interested in toast deletions */
						else if (change->action == REORDER_BUFFER_CHANGE_INSERT)
						{
							/*
							 * need to reassemble change in memory, ensure it
							 * doesn't get reused till we're done.
							 */
							dlist_delete(&change->node);
							ReorderBufferToastAppendChunk(rb, txn, relation,
														  change);
						}

					}
					RelationClose(relation);
					break;
				case REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT:
					/* XXX: we could skip snapshots in non toplevel txns */

					/* get rid of the old */
					RevertFromDecodingSnapshots(false);

					if (snapshot_now->copied)
					{
						ReorderBufferFreeSnap(rb, snapshot_now);
						snapshot_now =
							ReorderBufferCopySnap(rb, change->snapshot,
												  txn, command_id);
					}

					/*
					 * restored from disk, we need to be careful not to double
					 * free. We could introduce refcounting for that, but for
					 * now this seems infrequent enough not to care.
					 */
					else if (change->snapshot->copied)
					{
						snapshot_now =
							ReorderBufferCopySnap(rb, change->snapshot,
												  txn, command_id);
					}
					else
					{
						snapshot_now = change->snapshot;
					}


					/* and start with the new one */
					SetupDecodingSnapshots(snapshot_now, txn->tuplecid_hash);
					break;

				case REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID:
					Assert(change->command_id != InvalidCommandId);

					if (command_id < change->command_id)
					{
						command_id = change->command_id;

						if (!snapshot_now->copied)
						{
							/* we don't use the global one anymore */
							snapshot_now = ReorderBufferCopySnap(rb, snapshot_now,
																 txn, command_id);
						}

						snapshot_now->curcid = command_id;

						RevertFromDecodingSnapshots(false);
						SetupDecodingSnapshots(snapshot_now, txn->tuplecid_hash);

						/*
						 * everytime the CommandId is incremented, we could see
						 * new catalog contents
						 */
						ReorderBufferExecuteInvalidations(rb, txn);
					}

					break;

				case REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID:
					elog(ERROR, "tuplecid value in normal queue");
					break;
			}
		}

		ReorderBufferIterTXNFinish(rb, iterstate);

		/* call commit callback */
		rb->commit(rb, txn, commit_lsn);

		/* make sure nothing has written anything */
		if (GetTopTransactionIdIfAny() != InvalidTransactionId)
			elog(ERROR, "cannot write during replay, allocated xid %u",
				 GetTopTransactionIdIfAny());

		/* make sure there's no cache pollution */
		ReorderBufferExecuteInvalidations(rb, txn);

		/* cleanup */
		RevertFromDecodingSnapshots(false);

		/*
		 * Abort subtransaction or aborting transaction as a whole has the
		 * right semantics. We want all locks acquired in here to be released,
		 * not reassinged to the parent and we do not want any database access
		 * have persistent effects.
		 */
		if (subtx_started)
			RollbackAndReleaseCurrentSubTransaction();
		else if (tx_started)
			AbortCurrentTransaction();

		if (snapshot_now->copied)
			ReorderBufferFreeSnap(rb, snapshot_now);

		ReorderBufferCleanupTXN(rb, txn);
	}
	PG_CATCH();
	{
		/* TODO: Encapsulate cleanup from the PG_TRY and PG_CATCH blocks */
		if (iterstate)
			ReorderBufferIterTXNFinish(rb, iterstate);

		ReorderBufferExecuteInvalidations(rb, txn);

		RevertFromDecodingSnapshots(true);

		if (subtx_started)
			RollbackAndReleaseCurrentSubTransaction();
		else if (tx_started)
			AbortCurrentTransaction();

		if (snapshot_now->copied)
			ReorderBufferFreeSnap(rb, snapshot_now);

		/*
		 * don't do a ReorderBufferCleanupTXN here, with the vague idea of
		 * allowing to retry decoding.
		 */
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * Abort a transaction that possibly has previous changes. Needs to be done
 * independently for toplevel and subtransactions.
 */
void
ReorderBufferAbort(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn)
{
	ReorderBufferTXN *txn;

	txn = ReorderBufferTXNByXid(rb, xid, false, NULL, InvalidXLogRecPtr,
								false);

	/* no changes in this commit */
	if (txn == NULL)
		return;

	txn->final_lsn = lsn;

	ReorderBufferCleanupTXN(rb, txn);
}

/*
 * Abort all transactions that aren't actually running anymore because the
 * server restarted.
 */
void
ReorderBufferAbortOld(ReorderBuffer *rb, TransactionId oldestRunningXid)
{
	dlist_mutable_iter it;

	dlist_foreach_modify(it, &rb->toplevel_by_lsn)
	{
		ReorderBufferTXN * txn;

		txn = dlist_container(ReorderBufferTXN, node, it.cur);

		/*
		 * Iterate through all (potential) toplevel TXNs and abort all that
		 * are older than what possibly can be running. Once we've found the
		 * first that is alive we stop, there might be some that acquired an
		 * xid earlier but started writing later, but it's unlikely and they
		 * will cleaned up in a later call to ReorderBufferAbortOld().
		 */
		if (TransactionIdPrecedes(txn->xid, oldestRunningXid))
		{
			elog(DEBUG1, "aborting old transaction %u", txn->xid);
			ReorderBufferCleanupTXN(rb, txn);
		}
		else
			return;
	}
}

/*
 * Check whether a transaction is already known in this module
 */
bool
ReorderBufferIsXidKnown(ReorderBuffer *rb, TransactionId xid)
{
	ReorderBufferTXN *txn;

	txn = ReorderBufferTXNByXid(rb, xid, false, NULL, InvalidXLogRecPtr,
								false);
	return txn != NULL;
}

/*
 * Add a new snapshot to this transaction that is only used after lsn 'lsn'.
 */
void
ReorderBufferAddSnapshot(ReorderBuffer *rb, TransactionId xid,
						 XLogRecPtr lsn, Snapshot snap)
{
	ReorderBufferChange *change = ReorderBufferGetChange(rb);

	change->snapshot = snap;
	change->action_internal = REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT;

	ReorderBufferQueueChange(rb, xid, lsn, change);
}

/*
 * Setup the base snapshot of a transaction. That is the snapshot that is used
 * to decode all changes until either this transaction modifies the catalog or
 * another catalog modifying transaction commits.
 */
void
ReorderBufferSetBaseSnapshot(ReorderBuffer *rb, TransactionId xid,
							 XLogRecPtr lsn, Snapshot snap)
{
	ReorderBufferTXN *txn;
	bool		is_new;

	txn = ReorderBufferTXNByXid(rb, xid, true, &is_new, lsn, true);
	Assert(txn->base_snapshot == NULL);

	txn->base_snapshot = snap;
}

/*
 * Access the catalog with this CommandId at this point in the changestream.
 *
 * May only be called for command ids > 1
 */
void
ReorderBufferAddNewCommandId(ReorderBuffer *rb, TransactionId xid,
							 XLogRecPtr lsn, CommandId cid)
{
	ReorderBufferChange *change = ReorderBufferGetChange(rb);

	change->command_id = cid;
	change->action_internal = REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID;

	ReorderBufferQueueChange(rb, xid, lsn, change);
}


/*
 * Add new (relfilenode, tid) -> (cmin, cmax) mappings.
 */
void
ReorderBufferAddNewTupleCids(ReorderBuffer *rb, TransactionId xid,
							 XLogRecPtr lsn, RelFileNode node,
							 ItemPointerData tid, CommandId cmin,
							 CommandId cmax, CommandId combocid)
{
	ReorderBufferChange *change = ReorderBufferGetChange(rb);
	ReorderBufferTXN *txn;

	txn = ReorderBufferTXNByXid(rb, xid, true, NULL, lsn, true);

	change->tuplecid.node = node;
	change->tuplecid.tid = tid;
	change->tuplecid.cmin = cmin;
	change->tuplecid.cmax = cmax;
	change->tuplecid.combocid = combocid;
	change->lsn = lsn;
	change->action_internal = REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID;

	dlist_push_tail(&txn->tuplecids, &change->node);
	txn->ntuplecids++;
}

/*
 * Setup the invalidation of the toplevel transaction.
 *
 * This needs to be done before ReorderBufferCommit is called!
 */
void
ReorderBufferAddInvalidations(ReorderBuffer *rb, TransactionId xid,
							  XLogRecPtr lsn, Size nmsgs,
							  SharedInvalidationMessage *msgs)
{
	ReorderBufferTXN *txn;

	txn = ReorderBufferTXNByXid(rb, xid, true, NULL, lsn, true);

	if (txn->ninvalidations != 0)
		elog(ERROR, "only ever add one set of invalidations");

	Assert(nmsgs > 0);

	txn->ninvalidations = nmsgs;
	txn->invalidations = (SharedInvalidationMessage *)
		MemoryContextAlloc(rb->context,
						   sizeof(SharedInvalidationMessage) * nmsgs);
	memcpy(txn->invalidations, msgs,
		   sizeof(SharedInvalidationMessage) * nmsgs);
}

/*
 * Apply all invalidations we know. Possibly we only need parts at this point
 * in the changestream but we don't know which those are.
 */
static void
ReorderBufferExecuteInvalidations(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
	int			i;

	for (i = 0; i < txn->ninvalidations; i++)
		LocalExecuteInvalidationMessage(&txn->invalidations[i]);
}

/*
 * Mark a transaction as containing catalog changes
 */
void
ReorderBufferXidSetCatalogChanges(ReorderBuffer *rb, TransactionId xid,
								  XLogRecPtr lsn)
{
	ReorderBufferTXN *txn;

	txn = ReorderBufferTXNByXid(rb, xid, true, NULL, lsn, true);

	txn->has_catalog_changes = true;
}

/*
 * Query whether a transaction is already *known* to contain catalog
 * changes. This can be wrong until directly before the commit!
 */
bool
ReorderBufferXidHasCatalogChanges(ReorderBuffer *rb, TransactionId xid)
{
	ReorderBufferTXN *txn;

	txn = ReorderBufferTXNByXid(rb, xid, false, NULL, InvalidXLogRecPtr,
								false);
	if (txn == NULL)
		return false;

	return txn->has_catalog_changes;
}

/*
 * Have we already added the first snapshot?
 */
bool
ReorderBufferXidHasBaseSnapshot(ReorderBuffer *rb, TransactionId xid)
{
	ReorderBufferTXN *txn;

	txn = ReorderBufferTXNByXid(rb, xid, false, NULL, InvalidXLogRecPtr,
								false);

	/* transaction isn't known yet, ergo no snapshot */
	if (txn == NULL)
		return false;

	return txn->base_snapshot != NULL;
}

static void
ReorderBufferSerializeReserve(ReorderBuffer *rb, Size sz)
{
	if (!rb->outbufsize)
	{
		rb->outbuf = MemoryContextAlloc(rb->context, sz);
		rb->outbufsize = sz;
	}
	else if (rb->outbufsize < sz)
	{
		rb->outbuf = repalloc(rb->outbuf, sz);
		rb->outbufsize = sz;
	}
}

typedef struct ReorderBufferDiskChange
{
	Size		size;
	ReorderBufferChange change;
	/* data follows */
} ReorderBufferDiskChange;

/*
 * Persistency support
 */
static void
ReorderBufferSerializeChange(ReorderBuffer *rb, ReorderBufferTXN *txn,
							 int fd, ReorderBufferChange *change)
{
	ReorderBufferDiskChange *ondisk;
	Size		sz = sizeof(ReorderBufferDiskChange);

	ReorderBufferSerializeReserve(rb, sz);

	ondisk = (ReorderBufferDiskChange *) rb->outbuf;
	memcpy(&ondisk->change, change, sizeof(ReorderBufferChange));

	switch ((ReorderBufferChangeTypeInternal) change->action_internal)
	{
		case REORDER_BUFFER_CHANGE_INTERNAL_INSERT:
			/* fall through */
		case REORDER_BUFFER_CHANGE_INTERNAL_UPDATE:
			/* fall through */
		case REORDER_BUFFER_CHANGE_INTERNAL_DELETE:
			{
				char	   *data;
				Size		oldlen = 0;
				Size		newlen = 0;

				if (change->tp.oldtuple)
					oldlen = offsetof(ReorderBufferTupleBuf, data)
						+ change->tp.oldtuple->tuple.t_len
						- offsetof(HeapTupleHeaderData, t_bits);

				if (change->tp.newtuple)
					newlen = offsetof(ReorderBufferTupleBuf, data)
						+ change->tp.newtuple->tuple.t_len
						- offsetof(HeapTupleHeaderData, t_bits);

				sz += oldlen;
				sz += newlen;

				/* make sure we have enough space */
				ReorderBufferSerializeReserve(rb, sz);

				data = ((char *) rb->outbuf) + sizeof(ReorderBufferDiskChange);
				/* might have been reallocated above */
				ondisk = (ReorderBufferDiskChange *) rb->outbuf;

				if (oldlen)
				{
					memcpy(data, change->tp.oldtuple, oldlen);
					data += oldlen;
					Assert(&change->tp.oldtuple->header == change->tp.oldtuple->tuple.t_data);
				}

				if (newlen)
				{
					memcpy(data, change->tp.newtuple, newlen);
					data += newlen;
					Assert(&change->tp.newtuple->header == change->tp.newtuple->tuple.t_data);
				}
				break;
			}
		case REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT:
			{
				char	   *data;

				sz += sizeof(SnapshotData) +
					sizeof(TransactionId) * change->snapshot->xcnt +
					sizeof(TransactionId) * change->snapshot->subxcnt
					;

				/* make sure we have enough space */
				ReorderBufferSerializeReserve(rb, sz);
				data = ((char *) rb->outbuf) + sizeof(ReorderBufferDiskChange);
				/* might have been reallocated above */
				ondisk = (ReorderBufferDiskChange *) rb->outbuf;

				memcpy(data, change->snapshot, sizeof(SnapshotData));
				data += sizeof(SnapshotData);

				if (change->snapshot->xcnt)
				{
					memcpy(data, change->snapshot->xip,
						   sizeof(TransactionId) + change->snapshot->xcnt);
					data += sizeof(TransactionId) + change->snapshot->xcnt;
				}

				if (change->snapshot->subxcnt)
				{
					memcpy(data, change->snapshot->subxip,
						   sizeof(TransactionId) + change->snapshot->subxcnt);
					data += sizeof(TransactionId) + change->snapshot->subxcnt;
				}
				break;
			}
		case REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID:
			/* ReorderBufferChange contains everything important */
			break;
		case REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID:
			/* ReorderBufferChange contains everything important */
			break;
	}

	ondisk->size = sz;

	if (write(fd, rb->outbuf, ondisk->size) != ondisk->size)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to xid data file \"%u\": %m",
						txn->xid)));
	}

	Assert(ondisk->change.action_internal == change->action_internal);
}

/*
 * Check whether we should spill data to disk.
 */
static void
ReorderBufferCheckSerializeTXN(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
	/* FIXME subtxn handling? */
	if (txn->nentries_mem >= max_memtries)
	{
		ReorderBufferSerializeTXN(rb, txn);
		Assert(txn->nentries_mem == 0);
	}
}

/*
 * Spill data of a large transaction (and its subtransactions) to disk.
 */
static void
ReorderBufferSerializeTXN(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
	dlist_iter	subtxn_i;
	dlist_mutable_iter change_i;
	int			fd = -1;
	XLogSegNo	curOpenSegNo = 0;
	Size		spilled = 0;
	char		path[MAXPGPATH];

	elog(DEBUG2, "spill %u changes in tx %u to disk",
		 (uint32) txn->nentries_mem, txn->xid);

	/* do the same to all child TXs */
	dlist_foreach(subtxn_i, &txn->subtxns)
	{
		ReorderBufferTXN *subtxn;

		subtxn = dlist_container(ReorderBufferTXN, node, subtxn_i.cur);
		ReorderBufferSerializeTXN(rb, subtxn);
	}

	/* serialize changestream */
	dlist_foreach_modify(change_i, &txn->changes)
	{
		ReorderBufferChange *change;

		change = dlist_container(ReorderBufferChange, node, change_i.cur);

		/*
		 * store in segment in which it belongs by start lsn, don't split over
		 * multiple segments tho
		 */
		if (fd == -1 || XLByteInSeg(change->lsn, curOpenSegNo))
		{
			XLogRecPtr	recptr;

			if (fd != -1)
				CloseTransientFile(fd);

			XLByteToSeg(change->lsn, curOpenSegNo);
			XLogSegNoOffsetToRecPtr(curOpenSegNo, 0, recptr);

			/*
			 * No need to care about TLIs here, only used during a single run,
			 * so each LSN only maps to a specific WAL record.
			 */
			sprintf(path, "pg_llog/%s/xid-%u-lsn-%X-%X.snap",
					NameStr(MyLogicalDecodingSlot->name), txn->xid,
					(uint32) (recptr >> 32), (uint32) recptr);

			/* open segment, create it if necessary */
			fd = OpenTransientFile(path,
								   O_CREAT | O_WRONLY | O_APPEND | PG_BINARY,
								   S_IRUSR | S_IWUSR);

			if (fd < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open reorderbuffer file %s for writing: %m",
								path)));
		}

		ReorderBufferSerializeChange(rb, txn, fd, change);
		dlist_delete(&change->node);
		ReorderBufferReturnChange(rb, change);

		spilled++;
	}

	Assert(spilled == txn->nentries_mem);
	Assert(dlist_is_empty(&txn->changes));
	txn->nentries_mem = 0;

	if (fd != -1)
		CloseTransientFile(fd);

	/* issue write barrier */
	/* serialize main transaction state */
}

/*
 * Restore a number of changes spilled to disk back into memory.
 */
static Size
ReorderBufferRestoreChanges(ReorderBuffer *rb, ReorderBufferTXN *txn,
							int *fd, XLogSegNo *segno)
{
	Size		restored = 0;
	XLogSegNo	last_segno;
	dlist_mutable_iter cleanup_iter;

	Assert(txn->first_lsn != InvalidXLogRecPtr);
	Assert(txn->final_lsn != InvalidXLogRecPtr);

	/* free current entries, so we have memory for more */
	dlist_foreach_modify(cleanup_iter, &txn->changes)
	{
		ReorderBufferChange *cleanup =
		dlist_container(ReorderBufferChange, node, cleanup_iter.cur);

		dlist_delete(&cleanup->node);
		ReorderBufferReturnChange(rb, cleanup);
	}
	txn->nentries_mem = 0;
	Assert(dlist_is_empty(&txn->changes));

	XLByteToSeg(txn->final_lsn, last_segno);

	while (restored < max_memtries && *segno <= last_segno)
	{
		int			readBytes;
		ReorderBufferDiskChange *ondisk;

		if (*fd == -1)
		{
			XLogRecPtr	recptr;
			char		path[MAXPGPATH];

			/* first time in */
			if (*segno == 0)
			{
				XLByteToSeg(txn->first_lsn, *segno);
			}

			Assert(*segno != 0 || dlist_is_empty(&txn->changes));
			XLogSegNoOffsetToRecPtr(*segno, 0, recptr);

			/*
			 * No need to care about TLIs here, only used during a single run,
			 * so each LSN only maps to a specific WAL record.
			 */
			sprintf(path, "pg_llog/%s/xid-%u-lsn-%X-%X.snap",
					NameStr(MyLogicalDecodingSlot->name), txn->xid,
					(uint32) (recptr >> 32), (uint32) recptr);

			*fd = OpenTransientFile(path, O_RDONLY | PG_BINARY, 0);
			if (*fd < 0 && errno == ENOENT)
			{
				*fd = -1;
				(*segno)++;
				continue;
			}
			else if (*fd < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open reorderbuffer file %s for reading: %m",
								path)));

		}

		ReorderBufferSerializeReserve(rb, sizeof(ReorderBufferDiskChange));


		/*
		 * read the statically sized part of a change which has information
		 * about the total size. If we couldn't read a record, we're at the
		 * end of this file.
		 */

		readBytes = read(*fd, rb->outbuf, sizeof(ReorderBufferDiskChange));

		/* eof */
		if (readBytes == 0)
		{
			CloseTransientFile(*fd);
			*fd = -1;
			(*segno)++;
			continue;
		}
		else if (readBytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from reorderbuffer spill file: %m")));
		else if (readBytes != sizeof(ReorderBufferDiskChange))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("incomplete read from reorderbuffer spill file: read %d instead of %u",
							readBytes,
							(uint32) sizeof(ReorderBufferDiskChange))));

		ondisk = (ReorderBufferDiskChange *) rb->outbuf;

		ReorderBufferSerializeReserve(rb,
									  sizeof(ReorderBufferDiskChange) + ondisk->size);
		ondisk = (ReorderBufferDiskChange *) rb->outbuf;

		readBytes = read(*fd, rb->outbuf + sizeof(ReorderBufferDiskChange),
						 ondisk->size - sizeof(ReorderBufferDiskChange));

		if (readBytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from reorderbuffer spill file: %m")));
		else if (readBytes != ondisk->size - sizeof(ReorderBufferDiskChange))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("incomplete read from reorderbuffer spill file: read %d instead of %u",
							readBytes,
							(uint32) (ondisk->size - sizeof(ReorderBufferDiskChange)))));

		/*
		 * ok, read a full change from disk, now restore it into proper
		 * in-memory format
		 */
		ReorderBufferRestoreChange(rb, txn, rb->outbuf);
		restored++;
	}

	return restored;
}

/*
 * Convert change from its on-disk format to in-memory format and queue it onto
 * the TXN's ->changes list.
 */
static void
ReorderBufferRestoreChange(ReorderBuffer *rb, ReorderBufferTXN *txn,
						   char *data)
{
	ReorderBufferDiskChange *ondisk;
	ReorderBufferChange *change;

	ondisk = (ReorderBufferDiskChange *) data;

	change = ReorderBufferGetChange(rb);

	/* copy static part */
	memcpy(change, &ondisk->change, sizeof(ReorderBufferChange));

	data += sizeof(ReorderBufferDiskChange);

	/* restore individual stuff */
	switch ((ReorderBufferChangeTypeInternal) change->action_internal)
	{
		case REORDER_BUFFER_CHANGE_INTERNAL_INSERT:
			/* fall through */
		case REORDER_BUFFER_CHANGE_INTERNAL_UPDATE:
			/* fall through */
		case REORDER_BUFFER_CHANGE_INTERNAL_DELETE:
			if (change->tp.newtuple)
			{
				Size		len = offsetof(ReorderBufferTupleBuf, data)
				+((ReorderBufferTupleBuf *) data)->tuple.t_len
				- offsetof(HeapTupleHeaderData, t_bits);

				change->tp.newtuple = ReorderBufferGetTupleBuf(rb);
				memcpy(change->tp.newtuple, data, len);
				change->tp.newtuple->tuple.t_data = &change->tp.newtuple->header;

				data += len;
			}

			if (change->tp.oldtuple)
			{
				Size		len = offsetof(ReorderBufferTupleBuf, data)
				+((ReorderBufferTupleBuf *) data)->tuple.t_len
				- offsetof(HeapTupleHeaderData, t_bits);

				change->tp.oldtuple = ReorderBufferGetTupleBuf(rb);
				memcpy(change->tp.oldtuple, data, len);
				change->tp.oldtuple->tuple.t_data = &change->tp.oldtuple->header;
				data += len;
			}
			break;
		case REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT:
			{
				Snapshot	oldsnap = (Snapshot) data;
				Size		size = sizeof(SnapshotData) +
				sizeof(TransactionId) * oldsnap->xcnt +
				sizeof(TransactionId) * (oldsnap->subxcnt + 0)
						   ;

				Assert(change->snapshot != NULL);

				change->snapshot = MemoryContextAllocZero(rb->context, size);

				memcpy(change->snapshot, data, size);
				change->snapshot->xip = (TransactionId *)
					(((char *) change->snapshot) + sizeof(SnapshotData));
				change->snapshot->subxip =
					change->snapshot->xip + change->snapshot->xcnt + 0;
				change->snapshot->copied = true;
				break;
			}
			/* nothing needs to be done */
		case REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID:
		case REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID:
			break;
	}

	dlist_push_tail(&txn->changes, &change->node);
	txn->nentries_mem++;
}

/*
 * Delete all data spilled to disk after we've restarted/crashed. It will be
 * recreated when the respective slots are reused.
 */
void
ReorderBufferStartup(void)
{
	DIR		   *logical_dir;
	struct dirent *logical_de;

	DIR		   *spill_dir;
	struct dirent *spill_de;

	logical_dir = AllocateDir("pg_llog");
	while ((logical_de = ReadDir(logical_dir, "pg_llog")) != NULL)
	{
		char		path[MAXPGPATH];

		if (strcmp(logical_de->d_name, ".") == 0 ||
			strcmp(logical_de->d_name, "..") == 0)
			continue;

		/* one of our own directories */
		if (strcmp(logical_de->d_name, "snapshots") == 0)
			continue;

		/*
		 * ok, has to be a surviving logical slot, iterate and delete
		 * everythign starting with xid-*
		 */
		sprintf(path, "pg_llog/%s", logical_de->d_name);

		spill_dir = AllocateDir(path);
		while ((spill_de = ReadDir(spill_dir, "pg_llog")) != NULL)
		{
			if (strcmp(spill_de->d_name, ".") == 0 ||
				strcmp(spill_de->d_name, "..") == 0)
				continue;

			if (strncmp(spill_de->d_name, "xid", 3) == 0)
			{
				sprintf(path, "pg_llog/%s/%s", logical_de->d_name,
						spill_de->d_name);

				if (unlink(path) != 0)
					ereport(PANIC,
							(errcode_for_file_access(),
						  errmsg("could not remove xid data file \"%s\": %m",
								 path)));
			}
			/* XXX: WARN? */
		}
		FreeDir(spill_dir);
	}
	FreeDir(logical_dir);
}

/*
 * toast support
 */

/*
 * copied stuff from tuptoaster.c. Perhaps there should be toast_internal.h?
 */
#define VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr)	\
do { \
	varattrib_1b_e *attre = (varattrib_1b_e *) (attr); \
	Assert(VARATT_IS_EXTERNAL(attre)); \
	Assert(VARSIZE_EXTERNAL(attre) == sizeof(toast_pointer) + VARHDRSZ_EXTERNAL); \
	memcpy(&(toast_pointer), VARDATA_EXTERNAL(attre), sizeof(toast_pointer)); \
} while (0)

#define VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer) \
	((toast_pointer).va_extsize < (toast_pointer).va_rawsize - VARHDRSZ)

/*
 * Initialize per tuple toast reconstruction support.
 */
static void
ReorderBufferToastInitHash(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
	HASHCTL		hash_ctl;

	Assert(txn->toast_hash == NULL);

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(ReorderBufferToastEnt);
	hash_ctl.hash = tag_hash;
	hash_ctl.hcxt = rb->context;
	txn->toast_hash = hash_create("ReorderBufferToastHash", 5, &hash_ctl,
								  HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}

/*
 * Per toast-chunk handling for toast reconstruction
 *
 * Appends a toast chunk so we can reconstruct it when the tuple "owning" the
 * toasted Datum comes along.
 */
static void
ReorderBufferToastAppendChunk(ReorderBuffer *rb, ReorderBufferTXN *txn,
							  Relation relation, ReorderBufferChange *change)
{
	ReorderBufferToastEnt *ent;
	bool		found;
	int32		chunksize;
	bool		isnull;
	Pointer		chunk;
	TupleDesc	desc = RelationGetDescr(relation);
	Oid			chunk_id;
	Oid			chunk_seq;

	if (txn->toast_hash == NULL)
		ReorderBufferToastInitHash(rb, txn);

	Assert(IsToastRelation(relation));

	chunk_id = DatumGetObjectId(fastgetattr(&change->tp.newtuple->tuple, 1, desc, &isnull));
	Assert(!isnull);
	chunk_seq = DatumGetInt32(fastgetattr(&change->tp.newtuple->tuple, 2, desc, &isnull));
	Assert(!isnull);

	ent = (ReorderBufferToastEnt *)
		hash_search(txn->toast_hash,
					(void *) &chunk_id,
					HASH_ENTER,
					&found);

	if (!found)
	{
		Assert(ent->chunk_id == chunk_id);
		ent->num_chunks = 0;
		ent->last_chunk_seq = 0;
		ent->size = 0;
		ent->reconstructed = NULL;
		dlist_init(&ent->chunks);

		if (chunk_seq != 0)
			elog(ERROR, "got sequence entry %d for toast chunk %u instead of seq 0",
				 chunk_seq, chunk_id);
	}
	else if (found && chunk_seq != ent->last_chunk_seq + 1)
		elog(ERROR, "got sequence entry %d for toast chunk %u instead of seq %d",
			 chunk_seq, chunk_id, ent->last_chunk_seq + 1);

	chunk = DatumGetPointer(fastgetattr(&change->tp.newtuple->tuple, 3, desc, &isnull));
	Assert(!isnull);

	/* calculate size so we can allocate the right size at once later */
	if (!VARATT_IS_EXTENDED(chunk))
		chunksize = VARSIZE(chunk) - VARHDRSZ;
	else if (VARATT_IS_SHORT(chunk))
		/* could happen due to heap_form_tuple doing its thing */
		chunksize = VARSIZE_SHORT(chunk) - VARHDRSZ_SHORT;
	else
		elog(ERROR, "unexpected type of toast chunk");

	ent->size += chunksize;
	ent->last_chunk_seq = chunk_seq;
	ent->num_chunks++;
	dlist_push_tail(&ent->chunks, &change->node);
}

/*
 * Rejigger change->newtuple to point to in-memory toast tuples instead to
 * on-disk toast tuples that may not longer exist (think DROP TABLE or VACUUM).
 *
 * We cannot replace unchanged toast tuples though, so those will still point
 * to on-disk toast data.
 */
static void
ReorderBufferToastReplace(ReorderBuffer *rb, ReorderBufferTXN *txn,
						  Relation relation, ReorderBufferChange *change)
{
	TupleDesc	desc;
	int			natt;
	Datum	   *attrs;
	bool	   *isnull;
	bool	   *free;
	HeapTuple	newtup;
	Relation	toast_rel;
	TupleDesc	toast_desc;
	MemoryContext oldcontext;

	/* no toast tuples changed */
	if (txn->toast_hash == NULL)
		return;

	oldcontext = MemoryContextSwitchTo(rb->context);

	/* we should only have toast tuples in an INSERT or UPDATE */
	Assert(change->tp.newtuple);

	desc = RelationGetDescr(relation);

	toast_rel = RelationIdGetRelation(relation->rd_rel->reltoastrelid);
	toast_desc = RelationGetDescr(toast_rel);

	/* should we allocate from stack instead? */
	attrs = palloc0(sizeof(Datum) * desc->natts);
	isnull = palloc0(sizeof(bool) * desc->natts);
	free = palloc0(sizeof(bool) * desc->natts);

	heap_deform_tuple(&change->tp.newtuple->tuple, desc,
					  attrs, isnull);

	for (natt = 0; natt < desc->natts; natt++)
	{
		Form_pg_attribute attr = desc->attrs[natt];
		ReorderBufferToastEnt *ent;
		struct varlena *varlena;

		/* va_rawsize is the size of the original datum -- including header */
		struct varatt_external toast_pointer;
		struct varatt_indirect redirect_pointer;
		struct varlena *new_datum = NULL;
		struct varlena *reconstructed;
		dlist_iter	it;
		Size		data_done = 0;

		/* system columns aren't toasted */
		if (attr->attnum < 0)
			continue;

		if (attr->attisdropped)
			continue;

		/* not a varlena datatype */
		if (attr->attlen != -1)
			continue;

		/* no data */
		if (isnull[natt])
			continue;

		/* ok, we know we have a toast datum */
		varlena = (struct varlena *) DatumGetPointer(attrs[natt]);

		/* no need to do anything if the tuple isn't external */
		if (!VARATT_IS_EXTERNAL(varlena))
			continue;

		VARATT_EXTERNAL_GET_POINTER(toast_pointer, varlena);

		/*
		 * check whether the toast tuple changed, replace if so.
		 */
		ent = (ReorderBufferToastEnt *)
			hash_search(txn->toast_hash,
						(void *) &toast_pointer.va_valueid,
						HASH_FIND,
						NULL);
		if (ent == NULL)
			continue;

		new_datum =
			(struct varlena *) palloc0(INDIRECT_POINTER_SIZE);

		free[natt] = true;

		reconstructed = palloc0(toast_pointer.va_rawsize);

		ent->reconstructed = reconstructed;

		/* stitch toast tuple back together from its parts */
		dlist_foreach(it, &ent->chunks)
		{
			bool		isnull;
			ReorderBufferTupleBuf *tup =
			dlist_container(ReorderBufferChange, node, it.cur)->tp.newtuple;
			Pointer		chunk =
			DatumGetPointer(fastgetattr(&tup->tuple, 3, toast_desc, &isnull));

			Assert(!isnull);
			Assert(!VARATT_IS_EXTERNAL(chunk));
			Assert(!VARATT_IS_SHORT(chunk));

			memcpy(VARDATA(reconstructed) + data_done,
				   VARDATA(chunk),
				   VARSIZE(chunk) - VARHDRSZ);
			data_done += VARSIZE(chunk) - VARHDRSZ;
		}
		Assert(data_done == toast_pointer.va_extsize);

		/* make sure its marked as compressed or not */
		if (VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer))
			SET_VARSIZE_COMPRESSED(reconstructed, data_done + VARHDRSZ);
		else
			SET_VARSIZE(reconstructed, data_done + VARHDRSZ);

		memset(&redirect_pointer, 0, sizeof(redirect_pointer));
		redirect_pointer.pointer = reconstructed;

		SET_VARTAG_EXTERNAL(new_datum, VARTAG_INDIRECT);
		memcpy(VARDATA_EXTERNAL(new_datum), &redirect_pointer,
			   sizeof(redirect_pointer));

		attrs[natt] = PointerGetDatum(new_datum);
	}

	/*
	 * Build tuple in separate memory & copy tuple back into the tuplebuf
	 * passed to the output plugin. We can't directly heap_fill_tuple() into
	 * the tuplebuf because attrs[] will point back into the current content.
	 */
	newtup = heap_form_tuple(desc, attrs, isnull);
	Assert(change->tp.newtuple->tuple.t_len <= MaxHeapTupleSize);
	Assert(&change->tp.newtuple->header == change->tp.newtuple->tuple.t_data);

	memcpy(change->tp.newtuple->tuple.t_data,
		   newtup->t_data,
		   newtup->t_len);
	change->tp.newtuple->tuple.t_len = newtup->t_len;

	/*
	 * free resources we won't further need, more persistent stuff will be
	 * free'd in ReorderBufferToastReset().
	 */
	RelationClose(toast_rel);
	pfree(newtup);
	for (natt = 0; natt < desc->natts; natt++)
	{
		if (free[natt])
			pfree(DatumGetPointer(attrs[natt]));
	}
	pfree(attrs);
	pfree(free);
	pfree(isnull);

	MemoryContextSwitchTo(oldcontext);
}

/*
 * Free all resources allocated for toast reconstruction.
 */
static void
ReorderBufferToastReset(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
	HASH_SEQ_STATUS hstat;
	ReorderBufferToastEnt *ent;

	if (txn->toast_hash == NULL)
		return;

	/* sequentially walk over the hash and free everything */
	hash_seq_init(&hstat, txn->toast_hash);
	while ((ent = (ReorderBufferToastEnt *) hash_seq_search(&hstat)) != NULL)
	{
		dlist_mutable_iter it;

		if (ent->reconstructed != NULL)
			pfree(ent->reconstructed);

		dlist_foreach_modify(it, &ent->chunks)
		{
			ReorderBufferChange *change =
			dlist_container(ReorderBufferChange, node, it.cur);

			dlist_delete(&change->node);
			ReorderBufferReturnChange(rb, change);
		}
	}

	hash_destroy(txn->toast_hash);
	txn->toast_hash = NULL;
}


/*
 * Visibility support routines
 */

/*-------------------------------------------------------------------------
 * Lookup actual cmin/cmax values when using decoding snapshot. We can't
 * always rely on stored cmin/cmax values because of two scenarios:
 *
 * * A tuple got changed multiple times during a single transaction and thus
 *	 has got a combocid. Combocid's are only valid for the duration of a
 *	 single transaction.
 * * A tuple with a cmin but no cmax (and thus no combocid) got
 *	 deleted/updated in another transaction than the one which created it
 *	 which we are looking at right now. As only one of cmin, cmax or combocid
 *	 is actually stored in the heap we don't have access to the the value we
 *	 need anymore.
 *
 * To resolve those problems we have a per-transaction hash of (cmin,
 * cmax) tuples keyed by (relfilenode, ctid) which contains the actual
 * (cmin, cmax) values. That also takes care of combocids by simply
 * not caring about them at all. As we have the real cmin/cmax values
 * combocids aren't interesting.
 *
 * As we only care about catalog tuples here the overhead of this
 * hashtable should be acceptable.
 *
 * Heap rewrites complicate this a bit, check rewriteheap.c for
 * details.
 * -------------------------------------------------------------------------
 */


#include "storage/fd.h"

/* struct for qsort()ing mapping files by lsn somewhat efficiently */
typedef struct RewriteMappingFile
{
	XLogRecPtr	lsn;
	char		fname[MAXPGPATH];
} RewriteMappingFile;

#if NOT_USED
static void
DisplayMapping(HTAB *tuplecid_data)
{
	HASH_SEQ_STATUS hstat;
	ReorderBufferTupleCidEnt *ent;

	hash_seq_init(&hstat, tuplecid_data);
	while ((ent = (ReorderBufferTupleCidEnt *) hash_seq_search(&hstat)) != NULL)
	{
		elog(DEBUG3, "mapping: node: %u/%u/%u tid: %u/%u cmin: %u, cmax: %u",
			 ent->key.relnode.dbNode,
			 ent->key.relnode.spcNode,
			 ent->key.relnode.relNode,
			 BlockIdGetBlockNumber(&ent->key.tid.ip_blkid),
			 ent->key.tid.ip_posid,
			 ent->cmin,
			 ent->cmax
			);
	}
}
#endif

/*
 * Apply a single mapping file to tuplecid_data.
 *
 * The mapping file has to have been verified to be a) committed b) for our
 * transaction c) applied in LSN order.
 */
static void
ApplyLogicalMappingFile(HTAB *tuplecid_data, Oid relid, const char *fname)
{
	char		path[MAXPGPATH];
	int			fd;
	int			readBytes;
	LogicalRewriteMappingData map;

	sprintf(path, "pg_llog/mappings/%s", fname);
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY, 0);
	if (fd < 0)
		ereport(ERROR, (errmsg("could not open mappingfile %s: %m", path)));

	while (true)
	{
		ReorderBufferTupleCidKey key;
		ReorderBufferTupleCidEnt *ent;
		ReorderBufferTupleCidEnt *new_ent;
		bool found;

		/* be careful about padding */
		memset(&key, 0, sizeof(ReorderBufferTupleCidKey));

		/* read all mappings till the end of the file */
		readBytes = read(fd, &map, sizeof(LogicalRewriteMappingData));

		if (readBytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read logical rewrite mapping \"%s\": %m",
							path)));
		else if (readBytes == 0) /* EOF */
			break;
		else if (readBytes != sizeof(LogicalRewriteMappingData))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read logical rewrite mapping %s, read %d instead of %d",
							path, readBytes,
							(int32) sizeof(LogicalRewriteMappingData))));

		key.relnode = map.old_node;
		ItemPointerCopy(&map.old_tid,
						&key.tid);


		ent = (ReorderBufferTupleCidEnt *)
			hash_search(tuplecid_data,
						(void *) &key,
						HASH_FIND,
						NULL);

		/* no existing mapping, no need to update */
		if (!ent)
			continue;

		key.relnode = map.new_node;
		ItemPointerCopy(&map.new_tid,
						&key.tid);

		new_ent = (ReorderBufferTupleCidEnt *)
			hash_search(tuplecid_data,
						(void *) &key,
						HASH_ENTER,
						&found);

		if (found)
		{
			/*
			 * Make sure the existing mapping makes sense. We sometime update
			 * old records that did not yet have a cmax (e.g. pg_class' own
			 * entry while rewriting it) during rewrites, so allow that.
			 */
			Assert(ent->cmin == InvalidCommandId || ent->cmin == new_ent->cmin);
			Assert(ent->cmax == InvalidCommandId || ent->cmax == new_ent->cmax);
		}
		else
		{
			/* update mapping */
			new_ent->cmin = ent->cmin;
			new_ent->cmax = ent->cmax;
			new_ent->combocid = ent->combocid;
		}
	}
}


/*
 * check whether the transaciont id 'xid' in in the pre-sorted array 'xip'.
 */
static bool
TransactionIdInArray(TransactionId xid, TransactionId *xip, Size num)
{
	return bsearch(&xid, xip, num,
				   sizeof(TransactionId), xidComparator) != NULL;
}

/*
 * qsort() comparator for sorting RewriteMappingFiles in LSN order.
 */
static int
file_sort_by_lsn(const void *a_p, const void *b_p)
{
	RewriteMappingFile *a = *(RewriteMappingFile **)a_p;
	RewriteMappingFile *b = *(RewriteMappingFile **)b_p;

	if (a->lsn < b->lsn)
		return -1;
	else if (a->lsn > b->lsn)
		return 1;
	return 0;
}

/*
 * Apply any existing logical remapping files if there are any targeted at our
 * transaction for relid.
 */
static void
UpdateLogicalMappings(HTAB *tuplecid_data, Oid relid, Snapshot snapshot)
{
	DIR		   *mapping_dir;
	struct dirent *mapping_de;
	List	   *files = NIL;
	ListCell   *file;
	RewriteMappingFile **files_a;
	size_t		off;
	Oid			dboid = IsSharedRelation(relid) ? InvalidOid : MyDatabaseId;

	mapping_dir = AllocateDir("pg_llog/mappings");
	while ((mapping_de = ReadDir(mapping_dir, "pg_llog/mappings")) != NULL)
	{
		Oid				f_dboid;
		Oid				f_relid;
		TransactionId	f_mapped_xid;
		TransactionId	f_create_xid;
		XLogRecPtr		f_lsn;
		RewriteMappingFile *f;

		if (strcmp(mapping_de->d_name, ".") == 0 ||
			strcmp(mapping_de->d_name, "..") == 0)
			continue;

		/* XXX: should we warn about such files? */
		if (strncmp(mapping_de->d_name, "map-", 4) != 0)
			continue;

		if (sscanf(mapping_de->d_name, LOGICAL_REWRITE_FORMAT,
				   &f_dboid, &f_relid, &f_lsn,
				   &f_mapped_xid, &f_create_xid) != 5)
			elog(ERROR, "could not parse fname %s", mapping_de->d_name);

		/* mapping for another database */
		if (f_dboid != dboid)
			continue;

		/* mapping for another relation */
		if (f_relid != relid)
			continue;

		/* did the creating transaction abort? */
		if (!TransactionIdDidCommit(f_create_xid))
			continue;

		/* not for our transaction */
		if (!TransactionIdInArray(f_mapped_xid, snapshot->subxip, snapshot->subxcnt))
			continue;

		/* ok, relevant, queue for apply */
		f = palloc(sizeof(RewriteMappingFile));
		f->lsn = f_lsn;
		strcpy(f->fname, mapping_de->d_name);
		files = lappend(files, f);
	}
	FreeDir(mapping_dir);

	/* build array we can easily sort */
	files_a = palloc(list_length(files) * sizeof(RewriteMappingFile *));
	off = 0;
	foreach(file, files)
	{
		files_a[off++] = lfirst(file);
	}

	/* sort files so we apply them in LSN order */
	qsort(files_a, list_length(files), sizeof(RewriteMappingFile *),
		  file_sort_by_lsn);

	for(off = 0; off < list_length(files); off++)
	{
		RewriteMappingFile *f = files_a[off];
		elog(DEBUG1, "applying mapping: %s in %u", f->fname,
			snapshot->subxip[0]);
		ApplyLogicalMappingFile(tuplecid_data, relid, f->fname);
		pfree(f);
	}
}

extern bool
ResolveCminCmaxDuringDecoding(HTAB *tuplecid_data,
							  Snapshot snapshot,
							  HeapTuple htup, Buffer buffer,
							  CommandId *cmin, CommandId *cmax)
{
	ReorderBufferTupleCidKey key;
	ReorderBufferTupleCidEnt *ent;
	ForkNumber	forkno;
	BlockNumber blockno;
	bool updated_mapping = false;

	/* be careful about padding */
	memset(&key, 0, sizeof(key));

	Assert(!BufferIsLocal(buffer));

	/*
	 * get relfilenode from the buffer, no convenient way to access it other
	 * than that.
	 */
	BufferGetTag(buffer, &key.relnode, &forkno, &blockno);

	/* tuples can only be in the main fork */
	Assert(forkno == MAIN_FORKNUM);
	Assert(blockno == ItemPointerGetBlockNumber(&htup->t_self));

	ItemPointerCopy(&htup->t_self,
					&key.tid);

restart:
	ent = (ReorderBufferTupleCidEnt *)
		hash_search(tuplecid_data,
					(void *) &key,
					HASH_FIND,
					NULL);

	/*
	 * failed to find a mapping, check whether the table was rewritten and
	 * apply mapping if so, but only do that once - there can be no new
	 * mappings while we are in here since we have to hold a lock.
	 */
	if (ent == NULL && !updated_mapping)
	{
		UpdateLogicalMappings(tuplecid_data, htup->t_tableOid, snapshot);
		/* now check but don't update for a mapping again */
		updated_mapping = true;
		goto restart;
	}
	else if (ent == NULL)
		return false;

	if (cmin)
		*cmin = ent->cmin;
	if (cmax)
		*cmax = ent->cmax;
	return true;
}
