/*-------------------------------------------------------------------------
 *
 * slot.c
 *	   Replication slot management.
 *
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/replication/slot.c
 *
 * NOTES
 *
 *	  Replication slots are used to keep state about replicas of the cluster
 *	  they are allocated on, primarily to avoid removing resources that are
 *	  still required on replicas, but also to keep information for monitoring
 *	  purposes.
 *	  They need to be permanent (to allow restarts), crash-safe and
 *	  allocatable on standbys (to support cascading setups). Thus they need to
 *	  be stored outside the catalog as writing to the catalog in a crashsafe
 *	  manner isn't possible on standbys.
 *    Such slots are used both for streaming replication and changeset
 *    extraction. For the latter sometimes slot specific data needs to be
 *    serialized to disk to avoid exhausting memory.
 *    For both, changeset extraction and streaming replication we need to
 *    prevent that still required WAL will be removed/recycled and that rows
 *    we still need get vacuumed away.
 *
 *    To allow slots to be created on standbys and to allow additional data to
 *    be stored per slot, each replication slot gets its own directory inside
 *    the $PGDATA/pg_replslot directory. Inside that directory the /state file
 *    will contain the slot's own data. Additional data can be stored
 *    alongside that file if required.
 *
 *    While the server is running it would be inefficient always read the
 *    state files from disk, instead the data is loaded into memory at startup
 *    and most of the time only that data is accessed. Only when it is
 *    required that a certain value needs to be the same after a restart
 *    individual slots are serialized to disk.
 *
 *    Since the individual resources that need to be managed can be described
 *    by a simple number (xmin horizon, minimal required LSN), we compute the
 *    minimum value across all slots and store it in a separate struct, so we
 *    don't have to access all slots when accessing the global minimum.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>
#include <sys/stat.h>

#include "access/transam.h"

#include "fmgr.h"
#include "miscadmin.h"

#include "replication/slot.h"

#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/fd.h"

#include "utils/memutils.h"
#include "utils/syscache.h"

/*
 * Replication slot on-disk data structure.
 */
typedef struct ReplicationSlotOnDisk
{
	/* first part of this struct needs to be version independent */

	/* data not covered by checksum */
	uint32		magic;
	pg_crc32	checksum;

	/* data covered by checksum */
	uint32		version;
	uint32		length;

	/* data with potentially evolving format */
	ReplicationSlot slot;
} ReplicationSlotOnDisk;

/* size of the part of the slot that is version independent */
#define ReplicationSlotOnDiskConstantSize \
	offsetof(ReplicationSlotOnDisk, slot)
/* size of the slots that is not version indepenent */
#define ReplicationSlotOnDiskDynamicSize \
	sizeof(ReplicationSlotOnDisk) - ReplicationSlotOnDiskConstantSize

#define SLOT_MAGIC		0x1051CA1		/* format identifier */
#define SLOT_VERSION	1				/* version for new files */

/* Control array for replication slot management */
ReplicationSlotCtlData *ReplicationSlotCtl = NULL;

/* My backend's replication slot in the shared memory array */
ReplicationSlot *MyReplicationSlot = NULL;

/* GUCs */
int			max_replication_slots = 0;	/* the maximum number of replication slots */

/* persistency functions */
static void RestoreSlot(const char *name);
static void CreateSlot(ReplicationSlot *slot);
static void SaveSlotGuts(ReplicationSlot *slot, const char *path);
static void DeleteSlot(ReplicationSlot *slot);
static void SaveSlot(ReplicationSlot *slot);

/* misc helper functions */
static void KillSlot(int code, Datum arg);


/*
 * Report shared-memory space needed by ReplicationSlotShmemInit.
 */
Size
ReplicationSlotsShmemSize(void)
{
	Size		size = 0;

	if (max_replication_slots == 0)
		return size;

	size = offsetof(ReplicationSlotCtlData, replication_slots);
	size = add_size(size,
					mul_size(max_replication_slots, sizeof(ReplicationSlot)));

	return size;
}

/*
 * Allocate and initialize walsender-related shared memory.
 */
void
ReplicationSlotsShmemInit(void)
{
	bool		found;

	if (max_replication_slots == 0)
		return;

	ReplicationSlotCtl = (ReplicationSlotCtlData *)
		ShmemInitStruct("ReplicationSlot Ctl", ReplicationSlotsShmemSize(),
						&found);

	if (!found)
	{
		int			i;

		/* First time through, so initialize */
		MemSet(ReplicationSlotCtl, 0, ReplicationSlotsShmemSize());

		ReplicationSlotCtl->catalog_xmin = InvalidTransactionId;
		ReplicationSlotCtl->data_xmin = InvalidTransactionId;
		ReplicationSlotCtl->oldest_lsn = InvalidXLogRecPtr;

		for (i = 0; i < max_replication_slots; i++)
		{
			ReplicationSlot *slot =
			&ReplicationSlotCtl->replication_slots[i];

			/* everything is zero initialized by the memset above */

			SpinLockInit(&slot->mutex);
		}
	}
}

/*
 * Create a new replication slot and mark it as used by this backend.
 *
 * name: Name of the slot
 * db_specific: changeset extraction is db specific, if the slot is going to
 * 		be used for that pass true, otherwise false.
 */
void
ReplicationSlotCreate(const char *name, bool db_specific)
{
	ReplicationSlot *slot;
	bool		name_in_use;
	int			i;

	if (MyReplicationSlot != NULL)
		elog(ERROR, "cannot create a new slot while another slot has been acquired");

	/*
	 * Prevent concurrent creation of slots, so we can safely prevent
	 * duplicate names.
	 */
	LWLockAcquire(ReplicationSlotCtlLock, LW_EXCLUSIVE);

	/* FIXME: apply sanity checking to slot name */

	/* First, make sure the requested name is not in use. */
	name_in_use = false;
	for (i = 0; i < max_replication_slots && !name_in_use; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

		SpinLockAcquire(&s->mutex);
		if (s->in_use && strcmp(name, NameStr(s->name)) == 0)
			name_in_use = true;
		SpinLockRelease(&s->mutex);
	}

	if (name_in_use)
	{
		LWLockRelease(ReplicationSlotCtlLock);
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("there already is a replication slot named \"%s\"", name)));
	}

	/* Find the first available (not in_use (=> not active)) slot. */
	slot = NULL;
	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

		SpinLockAcquire(&s->mutex);
		if (!s->in_use)
		{
			Assert(!s->active);
			/* NOT releasing the lock yet */
			slot = s;
			break;
		}
		SpinLockRelease(&s->mutex);
	}

	if (!slot)
	{
		LWLockRelease(ReplicationSlotCtlLock);
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("could not find free replication slot"),
				 errhint("Free one or increase max_replication_slots.")));
	}

	MyReplicationSlot = slot;

	slot->in_use = true;
	slot->active = true;
	if (db_specific)
		slot->database = MyDatabaseId;
	else
		slot->database = InvalidOid;

	/* XXX: do we want to use truncate identifier instead? */
	strncpy(NameStr(slot->name), name, NAMEDATALEN);
	NameStr(slot->name)[NAMEDATALEN - 1] = '\0';

	memset(NameStr(slot->plugin), 0, NAMEDATALEN);

	/* Arrange to clean up at exit/error */
	before_shmem_exit(KillSlot, 0);

	/* release slot so it can be examined by others */
	SpinLockRelease(&slot->mutex);

	/*
	 * Now create the on-disk data structures for this replication slot,
	 * thereby guaranteeing it's persistency.
	 */
	CreateSlot(slot);
	LWLockRelease(ReplicationSlotCtlLock);
}

/*
 * Find an previously created slot and mark it as used by this backend.
 */
void
ReplicationSlotAcquire(const char *name)
{
	ReplicationSlot *slot;
	int			i;

	Assert(!MyReplicationSlot);

	for (i = 0; i < max_replication_slots; i++)
	{
		slot = &ReplicationSlotCtl->replication_slots[i];

		SpinLockAcquire(&slot->mutex);
		if (slot->in_use && strcmp(name, NameStr(slot->name)) == 0)
		{
			MyReplicationSlot = slot;
			/* NOT releasing the lock yet */
			break;
		}
		SpinLockRelease(&slot->mutex);
	}

	if (!MyReplicationSlot)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("could not find replication slot \"%s\"", name)));

	slot = MyReplicationSlot;

	if (slot->active)
	{
		SpinLockRelease(&slot->mutex);
		MyReplicationSlot = NULL;
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("slot \"%s\" already active", name)));
	}

	slot->active = true;
	/* now that we've marked it as active, we release our lock */
	SpinLockRelease(&slot->mutex);

	/*
	 * Don't let the user switch the database if the slot is database
	 * specific...
	 */
	if (slot->database != InvalidOid && slot->database != MyDatabaseId)
	{
		SpinLockAcquire(&slot->mutex);
		slot->active = false;
		MyReplicationSlot = NULL;
		SpinLockRelease(&slot->mutex);

		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 (errmsg("START_REPLICATION needs to be run in the same database as CREATE_REPLICATION_SLOT"))));
	}

	/* Arrange to clean up at exit */
	before_shmem_exit(KillSlot, 0);

	SaveSlot(slot);
}

/*
 * Release a replication slot, this or another backend can ReAcquire it
 * later. Resources this slot requires will be preserved.
 */
void
ReplicationSlotRelease(void)
{
	ReplicationSlot *slot;

	slot = MyReplicationSlot;

	Assert(slot != NULL && slot->active);

	SpinLockAcquire(&slot->mutex);
	slot->active = false;
	SpinLockRelease(&slot->mutex);

	MyReplicationSlot = NULL;

	/* might not have been set when we've been a plain slot */
	MyPgXact->vacuumFlags &= ~PROC_IN_LOGICAL_DECODING;

	/*
	 * XXX: There's not actually any need to save the slot to disk, it will be
	 * marked inactive after a crash/restart anyway. But we'd need to
	 * serialize all data at shutdowns or checkpoints...
	 */
	SaveSlot(slot);

	cancel_before_shmem_exit(KillSlot, 0);
}

/*
 * Permanently drop replication slot identified by the passed in name.
 */
void
ReplicationSlotDrop(const char *name)
{
	ReplicationSlot *slot = NULL;
	int			i;

	for (i = 0; i < max_replication_slots; i++)
	{
		slot = &ReplicationSlotCtl->replication_slots[i];

		SpinLockAcquire(&slot->mutex);
		if (slot->in_use && strcmp(name, NameStr(slot->name)) == 0)
		{
			/* NOT releasing the spinlock yet */
			break;
		}
		SpinLockRelease(&slot->mutex);
		slot = NULL;
	}

	if (!slot)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("could not find replication slot \"%s\"", name)));

	if (slot->active)
	{
		SpinLockRelease(&slot->mutex);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("could not drop active replication slot \"%s\"", name)));
	}

	/*
	 * Mark it as as active, so nobody can claim this slot while we are
	 * working on it. We don't want to hold the spinlock while doing stuff
	 * like fsyncing the state file to disk.
	 */
	slot->active = true;

	SpinLockRelease(&slot->mutex);

	/*
	 * Start critical section, we can't to be interrupted while on-disk/memory
	 * state aren't coherent.
	 */
	START_CRIT_SECTION();

	DeleteSlot(slot);

	/* ok, everything gone, after a crash we now wouldn't restore this slot */
	SpinLockAcquire(&slot->mutex);
	slot->active = false;
	slot->in_use = false;
	SpinLockRelease(&slot->mutex);

	END_CRIT_SECTION();

	/* slot is dead and doesn't nail the xmin anymore, recompute horizon */
	ReplicationSlotsComputeRequiredXmin();
}

/*
 * Serialize the currently acquired slot's state from memory to disk, thereby
 * guaranteeing the current state will survive a crash.
 */
void
ReplicationSlotSave()
{
	char		path[MAXPGPATH];

	Assert(MyReplicationSlot != NULL);

	sprintf(path, "pg_replslot/%s", NameStr(MyReplicationSlot->name));
	SaveSlotGuts(MyReplicationSlot, path);
}

/*
 * Compute the xmin between all of the decoding slots and store it in
 * ReplicationSlotCtlData.
 */
void
ReplicationSlotsComputeRequiredXmin(void)
{
	int			i;
	TransactionId catalog_xmin = InvalidTransactionId;
	TransactionId data_xmin = InvalidTransactionId;
	ReplicationSlot *slot;

	Assert(ReplicationSlotCtl != NULL);

	/* XXX: can we guarantee that LW_SHARED is ok? */
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

	for (i = 0; i < max_replication_slots; i++)
	{
		slot = &ReplicationSlotCtl->replication_slots[i];

		SpinLockAcquire(&slot->mutex);

		if (!slot->in_use)
		{
			SpinLockRelease(&slot->mutex);
			continue;
		}

		/* check the catalog xmin */
		if (TransactionIdIsValid(slot->effective_catalog_xmin) &&
			(!TransactionIdIsValid(catalog_xmin) ||
			 TransactionIdPrecedes(slot->effective_catalog_xmin, catalog_xmin))
			)
		{
			catalog_xmin = slot->effective_catalog_xmin;
		}

		/* check the data xmin */
		if (TransactionIdIsValid(slot->effective_data_xmin) &&
			(!TransactionIdIsValid(data_xmin) ||
			 TransactionIdPrecedes(slot->effective_data_xmin, data_xmin))
			)
		{
			data_xmin = slot->effective_data_xmin;
		}

		SpinLockRelease(&slot->mutex);
	}

	ReplicationSlotCtl->catalog_xmin = catalog_xmin;
	ReplicationSlotCtl->data_xmin = data_xmin;
	LWLockRelease(ProcArrayLock);

	elog(DEBUG1, "computed new global slot: catalog: %u, data: %u",
		 catalog_xmin, data_xmin);
}

/*
 * Compute the xmin between all of the decoding slots and store it in
 * ReplicationSlotCtlData.
 */
void
ReplicationSlotsComputeRequiredLSN(void)
{
	int			i;
	XLogRecPtr  min_required = InvalidXLogRecPtr;
	ReplicationSlot *slot;

	/* FIXME: think about locking */
	Assert(ReplicationSlotCtl != NULL);

	for (i = 0; i < max_replication_slots; i++)
	{
		slot = &ReplicationSlotCtl->replication_slots[i];

		SpinLockAcquire(&slot->mutex);
		if (slot->in_use &&
			slot->restart_decoding != InvalidXLogRecPtr &&
			(min_required == InvalidXLogRecPtr ||
			 slot->restart_decoding < min_required)
			)
		{
			min_required = slot->restart_decoding;
		}
		SpinLockRelease(&slot->mutex);
	}
	ReplicationSlotCtl->oldest_lsn = min_required;
}

/*
 * Check whether the server's configuration supports using replication
 * slots. Usage or creating a replication slot might still fail, even if this
 * function doesn't error out.
 */
void
CheckSlotRequirements(void)
{
	if (max_replication_slots == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 (errmsg("replication slot usage requires max_replication_slots > 0"))));

	if (wal_level < WAL_LEVEL_ARCHIVE)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("replication slot usage requires wal_level >= archive")));
}

/*
 * on_shmem_exit handler marking the current slot as inactive so it
 * can be reused after we've exited.
 */
static void
KillSlot(int code, Datum arg)
{
	/* XXX: LOCK? */
	if (MyReplicationSlot && MyReplicationSlot->active)
	{
		MyReplicationSlot->active = false;
		/* might not have been set when we've been a plain slot */
		MyPgXact->vacuumFlags &= ~PROC_IN_LOGICAL_DECODING;
	}
	MyReplicationSlot = NULL;
}

/*
 * Load all replication slots from disk into memory at server startup. This
 * needs to be run before we start crash recovery.
 */
void
StartupReplicationSlots(XLogRecPtr checkPointRedo)
{
	DIR		   *replication_dir;
	struct dirent *replication_de;

	ereport(DEBUG1,
			(errmsg("starting up replication slots from LSN %X/%X",
					(uint32) (checkPointRedo >> 32), (uint32) checkPointRedo)));

	/* restore all slots by iterating over all on-disk entries */
	replication_dir = AllocateDir("pg_replslot");
	while ((replication_de = ReadDir(replication_dir, "pg_replslot")) != NULL)
	{
		if (strcmp(replication_de->d_name, ".") == 0 ||
			strcmp(replication_de->d_name, "..") == 0)
			continue;

		/* one of our own directories, ignore */
		if (strcmp(replication_de->d_name, "snapshots") == 0 ||
			strcmp(replication_de->d_name, "mappings") == 0)
			continue;

		/* FIXME: check for !directory and skip if. */

		/* we crashed while a slot was being setup or deleted, clean up */
		if (strcmp(replication_de->d_name, "new") == 0 ||
			strcmp(replication_de->d_name, "old") == 0)
		{
			char		path[MAXPGPATH];

			sprintf(path, "pg_replslot/%s", replication_de->d_name);

			if (!rmtree(path, true))
			{
				FreeDir(replication_dir);
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not remove directory \"%s\": %m",
								path)));
			}
			continue;
		}

		RestoreSlot(replication_de->d_name);
	}
	FreeDir(replication_dir);

	/* currently no slots exist, we're done. */
	if (max_replication_slots <= 0)
		return;

	/* Now that we have recovered all the data, compute replication xmin */
	ReplicationSlotsComputeRequiredXmin();
	ReplicationSlotsComputeRequiredLSN();
}

/* ----
 * Manipulation of ondisk state of replication slots
 *
 * NB: none of the routines below should take any notice whether a slot is the
 * current one or not, that's all handled a layer above.
 * ----
 */
static void
CreateSlot(ReplicationSlot *slot)
{
	char		tmppath[MAXPGPATH];
	char		path[MAXPGPATH];

	START_CRIT_SECTION();

	sprintf(tmppath, "pg_replslot/new");
	sprintf(path, "pg_replslot/%s", NameStr(slot->name));

	if (mkdir(tmppath, S_IRWXU) < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not create directory \"%s\": %m",
						tmppath)));

	fsync_fname(tmppath, true);

	SaveSlotGuts(slot, tmppath);

	if (rename(tmppath, path) != 0)
	{
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not rename file \"%s\" to \"%s\": %m",
						tmppath, path)));
	}

	fsync_fname(path, true);

	END_CRIT_SECTION();
}

/*
 * Serialize the passed to disk.
 */
static void
SaveSlot(ReplicationSlot *slot)
{
	char		path[MAXPGPATH];

	sprintf(path, "pg_replslot/%s", NameStr(slot->name));
	SaveSlotGuts(slot, path);
}

/*
 * Shared functionality between saving and creating a replication slot.
 */
static void
SaveSlotGuts(ReplicationSlot *slot, const char *dir)
{
	char		tmppath[MAXPGPATH];
	char		path[MAXPGPATH];
	int			fd;
	ReplicationSlotOnDisk cp;

	/* silence valgrind :( */
	memset(&cp, 0, sizeof(ReplicationSlotOnDisk));

	sprintf(tmppath, "%s/state.tmp", dir);
	sprintf(path, "%s/state", dir);

	START_CRIT_SECTION();

	fd = OpenTransientFile(tmppath,
						   O_CREAT | O_EXCL | O_WRONLY | PG_BINARY,
						   S_IRUSR | S_IWUSR);
	if (fd < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m",
						tmppath)));

	cp.magic = SLOT_MAGIC;
	INIT_CRC32(cp.checksum);
	cp.version = 1;
	cp.length = ReplicationSlotOnDiskDynamicSize;

	SpinLockAcquire(&slot->mutex);

	cp.slot.catalog_xmin = slot->catalog_xmin;
	cp.slot.data_xmin = slot->data_xmin;
	cp.slot.effective_catalog_xmin = slot->effective_catalog_xmin;
	cp.slot.effective_data_xmin = slot->effective_data_xmin;

	strcpy(NameStr(cp.slot.name), NameStr(slot->name));
	strcpy(NameStr(cp.slot.plugin), NameStr(slot->plugin));

	cp.slot.database = slot->database;
	cp.slot.confirmed_flush = slot->confirmed_flush;
	cp.slot.restart_decoding = slot->restart_decoding;
	cp.slot.candidate_catalog_xmin = InvalidTransactionId;
	cp.slot.candidate_xmin_lsn = InvalidXLogRecPtr;
	cp.slot.candidate_restart_decoding = InvalidXLogRecPtr;
	cp.slot.candidate_restart_valid = InvalidXLogRecPtr;
	cp.slot.in_use = slot->in_use;
	cp.slot.active = false;

	SpinLockRelease(&slot->mutex);

	COMP_CRC32(cp.checksum,
			   (char *)(&cp) + ReplicationSlotOnDiskConstantSize,
			   ReplicationSlotOnDiskDynamicSize);

	if ((write(fd, &cp, sizeof(cp))) != sizeof(cp))
	{
		CloseTransientFile(fd);
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m",
						tmppath)));
	}

	/* fsync the file */
	if (pg_fsync(fd) != 0)
	{
		CloseTransientFile(fd);
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m",
						tmppath)));
	}

	CloseTransientFile(fd);

	/* rename to permanent file, fsync file and directory */
	if (rename(tmppath, path) != 0)
	{
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not rename \"%s\" to \"%s\": %m",
						tmppath, path)));
	}

	fsync_fname((char *) dir, true);
	fsync_fname(path, false);

	END_CRIT_SECTION();
}

/*
 * Delete a single slot from disk, not touching the in-memory state.
 */
static void
DeleteSlot(ReplicationSlot *slot)
{
	char		path[MAXPGPATH];
	char		tmppath[] = "pg_replslot/old";

	START_CRIT_SECTION();

	sprintf(path, "pg_replslot/%s", NameStr(slot->name));

	if (rename(path, tmppath) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not rename \"%s\" to \"%s\": %m",
						path, tmppath)));

	/* make sure no partial state is visible after a crash */
	fsync_fname(tmppath, true);
	fsync_fname("pg_replslot", true);

	if (!rmtree(tmppath, true))
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove directory \"%s\": %m",
						tmppath)));
	}

	END_CRIT_SECTION();
}

/*
 * Load a single slot from disk into memory.
 */
static void
RestoreSlot(const char *name)
{
	ReplicationSlotOnDisk cp;
	int			i;
	char		path[MAXPGPATH];
	int			fd;
	bool		restored = false;
	int			readBytes;
	pg_crc32	checksum;

	/* delete temp file if it exists */
	sprintf(path, "pg_replslot/%s/state.tmp", name);
	if (unlink(path) < 0 && errno != ENOENT)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not unlink file \"%s\": %m", path)));

	sprintf(path, "pg_replslot/%s/state", name);

	elog(DEBUG1, "restoring replication slot from \"%s\"", path);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY, 0);

	/*
	 * We do not need to handle this as we are rename()ing the directory into
	 * place only after we fsync()ed the state file.
	 */
	if (fd < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));

	/* read part of statefile that's guaranteed to be version independent */
	readBytes = read(fd, &cp, ReplicationSlotOnDiskConstantSize);
	if (readBytes != ReplicationSlotOnDiskConstantSize)
	{
		int			saved_errno = errno;

		CloseTransientFile(fd);
		errno = saved_errno;
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\", read %d of %u: %m",
						path, readBytes,
						(uint32) ReplicationSlotOnDiskConstantSize)));
	}

	/* verify magic */
	if (cp.magic != SLOT_MAGIC)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("replication slot file \"%s\" has wrong magic %u instead of %u",
						path, cp.magic, SLOT_MAGIC)));

	/* verify version */
	if (cp.version != SLOT_VERSION)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("replication slot file \"%s\" has unsupported version %u",
						path, cp.version)));

	/* boundary check on length */
	if (cp.length != ReplicationSlotOnDiskDynamicSize)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("replication slot file \"%s\" has corrupted length %u",
						path, cp.length)));

	/* Now that we know the size, read the entire file */
	readBytes = read(fd,
					 (char *)&cp + ReplicationSlotOnDiskConstantSize,
					 cp.length);
	if (readBytes != cp.length)
	{
		int			saved_errno = errno;

		CloseTransientFile(fd);
		errno = saved_errno;
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\", read %d of %u: %m",
						path, readBytes, cp.length)));
	}

	CloseTransientFile(fd);

	/* now verify the CRC32 */
	INIT_CRC32(checksum);
	COMP_CRC32(checksum,
			   (char *)&cp + ReplicationSlotOnDiskConstantSize,
			   ReplicationSlotOnDiskDynamicSize);

	if (!EQ_CRC32(checksum, cp.checksum))
		ereport(PANIC,
				(errmsg("replication slot file %s: checksum mismatch, is %u, should be %u",
						path, checksum, cp.checksum)));

	/* nothing can be active yet, don't lock anything */
	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *slot;

		slot = &ReplicationSlotCtl->replication_slots[i];

		if (slot->in_use)
			continue;

		slot->catalog_xmin = cp.slot.catalog_xmin;
		slot->data_xmin = cp.slot.data_xmin;
		/*
		 * after a crash, always use xmin, not effective_xmin, the
		 * slot obviously survived
		 */
		slot->effective_catalog_xmin = cp.slot.catalog_xmin;
		slot->effective_data_xmin = cp.slot.data_xmin;
		strcpy(NameStr(slot->name), NameStr(cp.slot.name));
		strcpy(NameStr(slot->plugin), NameStr(cp.slot.plugin));
		slot->database = cp.slot.database;
		slot->restart_decoding = cp.slot.restart_decoding;
		slot->confirmed_flush = cp.slot.confirmed_flush;
		/* ignore previous values, they are transient */
		slot->candidate_catalog_xmin = InvalidTransactionId;
		slot->candidate_xmin_lsn = InvalidXLogRecPtr;
		slot->candidate_restart_decoding = InvalidXLogRecPtr;
		slot->candidate_restart_valid = InvalidXLogRecPtr;
		slot->in_use = true;
		slot->active = false;
		restored = true;
		break;
	}

	if (!restored)
		ereport(PANIC,
				(errmsg("too many replication slots active before shutdown"),
				 errhint("Increase max_replication_slots and try again.")));
}
