/*-------------------------------------------------------------------------
 * slot.h
 *	   replication slot management
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef SLOT_H
#define SLOT_H

#include "access/xlog.h"
#include "access/xlogreader.h"
#include "replication/output_plugin.h"
#include "storage/shmem.h"
#include "storage/spin.h"


/*
 * Shared memory state of a single replication slot.
 */
typedef struct ReplicationSlot
{
	/* lock, on same cacheline as effective_xmin */
	slock_t		mutex;

	/* on-disk xmin, updated first */
	TransactionId catalog_xmin;
	TransactionId data_xmin;

	/* in-memory xmin, updated after syncing to disk */
	TransactionId effective_catalog_xmin;
	TransactionId effective_data_xmin;

	/* is this slot defined */
	bool		in_use;

	/* is somebody streaming out changes for this slot */
	bool		active;

	/* ----
	 * For logical decoding, this contains the point where, after a shutdown,
	 * crash, whatever where do we have to restart decoding from to
	 * a) find a valid & ready snapshot
	 * b) the complete content for all in-progress xacts
	 *
	 * For streaming replication, this contains the oldest LSN (in any
	 * timeline) the standb might ask for.
	 * ----
	 */
	XLogRecPtr	restart_decoding;

	/*
	 * Last location we know the client has confirmed to have safely received
	 * data to. No earlier data can be decoded after a restart/crash.
	 */
	XLogRecPtr	confirmed_flush;

	/* ----
	 * When the client has confirmed flushes >= candidate_xmin_after we can
	 * a) advance the pegged xmin
	 * b) advance restart_decoding_from so we have to read/keep less WAL
	 * ----
	 */
	TransactionId candidate_catalog_xmin;
	XLogRecPtr	candidate_xmin_lsn;
	XLogRecPtr	candidate_restart_valid;
	XLogRecPtr	candidate_restart_decoding;

	/* database the slot is active on */
	Oid			database;

	/* slot identifier */
	NameData	name;

	/* plugin name */
	NameData	plugin;
} ReplicationSlot;

/*
 * Shared memory control area for all of replication slots.
 */
typedef struct ReplicationSlotCtlData
{
	/*
	 * Xmin across all replication slots.
	 *
	 * Protected by ProcArrayLock.
	 */
	TransactionId catalog_xmin;
	TransactionId data_xmin;

	/*
	 * Oldest required LSN across all slots.
	 */
	XLogRecPtr oldest_lsn;

	ReplicationSlot replication_slots[FLEXIBLE_ARRAY_MEMBER];
} ReplicationSlotCtlData;

/*
 * Pointers to shared memory
 */
extern ReplicationSlotCtlData *ReplicationSlotCtl;
extern ReplicationSlot *MyReplicationSlot;

/* GUCs */
extern PGDLLIMPORT int max_replication_slots;

/* shmem initialization functions */
extern Size ReplicationSlotsShmemSize(void);
extern void ReplicationSlotsShmemInit(void);

/* management of individual slots */
extern void ReplicationSlotCreate(const char *name, bool db_specific);
extern void ReplicationSlotDrop(const char *name);
extern void ReplicationSlotAcquire(const char *name);
extern void ReplicationSlotRelease(void);
extern void ReplicationSlotSave(void);

/* misc stuff */
extern void ReplicationSlotsComputeRequiredXmin(void);
extern void ReplicationSlotsComputeRequiredLSN(void);
extern void StartupReplicationSlots(XLogRecPtr checkPointRedo);
extern void CheckSlotRequirements(void);

/* SQL callable functions */
extern Datum pg_get_replication_slots(PG_FUNCTION_ARGS);

#endif /* SLOT_H */
