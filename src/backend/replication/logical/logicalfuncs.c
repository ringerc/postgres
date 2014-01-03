/*-------------------------------------------------------------------------
 *
 * logicalfuncs.c
 *
 *	   Support functions for using changeset extraction
 *
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logicalfuncs.c
 *
 */

#include "postgres.h"

#include <unistd.h>

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "catalog/pg_type.h"

#include "nodes/makefuncs.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/logicalfuncs.h"

#include "storage/fd.h"

/* private date for writing out data */
typedef struct DecodingOutputState {
	Tuplestorestate *tupstore;
	TupleDesc tupdesc;
} DecodingOutputState;

Datum		create_decoding_replication_slot(PG_FUNCTION_ARGS);
Datum		create_physical_replication_slot(PG_FUNCTION_ARGS);
Datum		drop_replication_slot(PG_FUNCTION_ARGS);
Datum		decoding_slot_get_changes(PG_FUNCTION_ARGS);
Datum		decoding_slot_get_binary_changes(PG_FUNCTION_ARGS);
Datum		decoding_slot_peek_changes(PG_FUNCTION_ARGS);
Datum		decoding_slot_peek_binary_changes(PG_FUNCTION_ARGS);

static void
LogicalOutputPrepareWrite(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid,
						  bool last_write)
{
	resetStringInfo(ctx->out);
}

static void
LogicalOutputWrite(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid,
				   bool last_write)
{
	Datum		values[3];
	bool		nulls[3];
	char		buf[60];
	DecodingOutputState *p;

	/* SQL Datums can only be of a limited length... */
	if (ctx->out->len > MaxAllocSize - VARHDRSZ)
		elog(ERROR, "too much output for sql interface");

	p = (DecodingOutputState *) ctx->output_writer_private;

	sprintf(buf, "%X/%X", (uint32) (lsn >> 32), (uint32) lsn);

	memset(nulls, 0, sizeof(nulls));
	values[0] = CStringGetTextDatum(buf);
	values[1] = TransactionIdGetDatum(xid);
	/*
	 * XXX: maybe we ought to assert ctx->out is in database encoding when
	 * we're writing textual output.
	 */
	/* ick, but cstring_to_text_with_len works for bytea perfectly fine */
	values[2] = PointerGetDatum(
		cstring_to_text_with_len(ctx->out->data, ctx->out->len));

	tuplestore_putvalues(p->tupstore, p->tupdesc, values, nulls);
}

/* FIXME: duplicate code with pg_xlogdump, similar to walsender.c */
static void
XLogRead(char *buf, TimeLineID tli, XLogRecPtr startptr, Size count)
{
	char	   *p;
	XLogRecPtr	recptr;
	Size		nbytes;

	static int	sendFile = -1;
	static XLogSegNo sendSegNo = 0;
	static uint32 sendOff = 0;

	p = buf;
	recptr = startptr;
	nbytes = count;

	while (nbytes > 0)
	{
		uint32		startoff;
		int			segbytes;
		int			readbytes;

		startoff = recptr % XLogSegSize;

		if (sendFile < 0 || !XLByteInSeg(recptr, sendSegNo))
		{
			char		path[MAXPGPATH];

			/* Switch to another logfile segment */
			if (sendFile >= 0)
				close(sendFile);

			XLByteToSeg(recptr, sendSegNo);

			XLogFilePath(path, tli, sendSegNo);

			sendFile = BasicOpenFile(path, O_RDONLY | PG_BINARY, 0);

			if (sendFile < 0)
			{
				if (errno == ENOENT)
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("requested WAL segment %s has already been removed",
									path)));
				else
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not open file \"%s\": %m",
									path)));
			}
			sendOff = 0;
		}

		/* Need to seek in the file? */
		if (sendOff != startoff)
		{
			if (lseek(sendFile, (off_t) startoff, SEEK_SET) < 0)
			{
				char		path[MAXPGPATH];

				XLogFilePath(path, tli, sendSegNo);

				ereport(ERROR,
						(errcode_for_file_access(),
				  errmsg("could not seek in log segment %s to offset %u: %m",
						 path, startoff)));
			}
			sendOff = startoff;
		}

		/* How many bytes are within this segment? */
		if (nbytes > (XLogSegSize - startoff))
			segbytes = XLogSegSize - startoff;
		else
			segbytes = nbytes;

		readbytes = read(sendFile, p, segbytes);
		if (readbytes <= 0)
		{
			char		path[MAXPGPATH];

			XLogFilePath(path, tli, sendSegNo);

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from log segment %s, offset %u, length %lu: %m",
							path, sendOff, (unsigned long) segbytes)));
		}

		/* Update state for read */
		recptr += readbytes;

		sendOff += readbytes;
		nbytes -= readbytes;
		p += readbytes;
	}
}

int
logical_read_local_xlog_page(XLogReaderState *state, XLogRecPtr targetPagePtr,
	int reqLen, XLogRecPtr targetRecPtr, char *cur_page, TimeLineID *pageTLI)
{
	XLogRecPtr	flushptr,
				loc;
	int			count;

	loc = targetPagePtr + reqLen;
	while (1)
	{
		/*
		 * FIXME: we're going to have to do something more intelligent about
		 * timelines on standby's. Use readTimeLineHistory() and
		 * tliOfPointInHistory() to get the proper LSN?
		 */
		if (!RecoveryInProgress())
		{
			*pageTLI = ThisTimeLineID;
			flushptr = GetFlushRecPtr();
		}
		else
			flushptr = GetXLogReplayRecPtr(pageTLI);

		if (loc <= flushptr)
			break;

		/*
		 * XXX: It'd be way nicer to be able to use the walsender waiting logic
		 * here, but that's not available in all environments.
		 */
		CHECK_FOR_INTERRUPTS();
		pg_usleep(1000L);
	}

	/* more than one block available */
	if (targetPagePtr + XLOG_BLCKSZ <= flushptr)
		count = XLOG_BLCKSZ;
	/* not enough data there */
	else if (targetPagePtr + reqLen > flushptr)
		return -1;
	/* part of the page available */
	else
		count = flushptr - targetPagePtr;

	/* XXX: more sensible/efficient implementation */
	XLogRead(cur_page, *pageTLI, targetPagePtr, XLOG_BLCKSZ);

	return count;
}

static void
check_permissions(void)
{
	if (!superuser() && !has_rolreplication(GetUserId()))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser or replication role to use changeset extraction"))));
}

Datum
create_decoding_replication_slot(PG_FUNCTION_ARGS)
{
	Name		name = PG_GETARG_NAME(0);
	Name		plugin = PG_GETARG_NAME(1);

	char		xpos[MAXFNAMELEN];

	TupleDesc	tupdesc;
	HeapTuple	tuple;
	Datum		result;
	Datum		values[2];
	bool		nulls[2];

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	check_permissions();

	CheckLogicalDecodingRequirements();
	Assert(!MyReplicationSlot);

	/*
	 * Acquire a logical decoding slot, this will check for conflicting
	 * names.
	 */
	ReplicationSlotCreate(NameStr(*name), true);

	/* make sure we don't end up with an unreleased slot */
	PG_TRY();
	{
		LogicalDecodingContext *ctx = NULL;

		/*
		 * Create logical decoding context, to build the initial snapshot.
		 */
		ctx = CreateDecodingContext(
			true, NameStr(*plugin), InvalidXLogRecPtr, NIL,
			logical_read_local_xlog_page, NULL, NULL);

		/* build initial snapshot, might take a while */
		DecodingContextFindStartpoint(ctx);

		/* Extract the values we want */
		snprintf(xpos, sizeof(xpos), "%X/%X",
				 (uint32) (MyReplicationSlot->confirmed_flush >> 32),
				 (uint32) MyReplicationSlot->confirmed_flush);

		/* don't need the decoding context anymore */
		FreeDecodingContext(ctx);
	}
	PG_CATCH();
	{
		ReplicationSlotRelease();
		ReplicationSlotDrop(NameStr(*name));
		PG_RE_THROW();
	}
	PG_END_TRY();

	values[0] = CStringGetTextDatum(NameStr(MyReplicationSlot->name));
	values[1] = CStringGetTextDatum(xpos);

	memset(nulls, 0, sizeof(nulls));

	tuple = heap_form_tuple(tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	ReplicationSlotRelease();

	PG_RETURN_DATUM(result);
}

Datum
create_physical_replication_slot(PG_FUNCTION_ARGS)
{
	Name		name = PG_GETARG_NAME(0);
	Datum		values[2];
	bool		nulls[2];
	TupleDesc	tupdesc;
	HeapTuple	tuple;
	Datum		result;

	check_permissions();

	CheckSlotRequirements();

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/*
	 * Acquire a logical decoding slot, this will check for conflicting
	 * names.
	 */
	ReplicationSlotCreate(NameStr(*name), false);

	values[0] = CStringGetTextDatum(NameStr(MyReplicationSlot->name));

	nulls[0] = false;
	nulls[0] = true;

	tuple = heap_form_tuple(tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	ReplicationSlotRelease();

	PG_RETURN_DATUM(result);
}

static Datum
decoding_slot_get_changes_guts(FunctionCallInfo fcinfo, bool confirm)
{
	Name		name = PG_GETARG_NAME(0);

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	XLogRecPtr	now;
	XLogRecPtr	startptr;
	XLogRecPtr	rp;

	LogicalDecodingContext *ctx;

	ResourceOwner old_resowner = CurrentResourceOwner;
	ArrayType  *arr;
	Size		ndim;
	List	   *options = NIL;
	DecodingOutputState *p;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* state to write output to */
	p = palloc(sizeof(DecodingOutputState));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &p->tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	check_permissions();

	CheckLogicalDecodingRequirements();

	arr = PG_GETARG_ARRAYTYPE_P(2);
	ndim = ARR_NDIM(arr);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	if (ndim > 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("start_logical_replication only accept one dimension of arguments")));
	}
	else if (array_contains_nulls(arr))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			  errmsg("start_logical_replication expects NOT NULL options")));
	}
	else if (ndim == 1)
	{
		int			nelems;
		Datum	   *datum_opts;
		int			i;

		Assert(ARR_ELEMTYPE(arr) == TEXTOID);

		deconstruct_array(arr, TEXTOID, -1, false, 'i',
						  &datum_opts, NULL, &nelems);

		if (nelems % 2 != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("options need to be specified pairwise")));
		}

		for (i = 0; i < nelems; i += 2)
		{
			char	   *name = TextDatumGetCString(datum_opts[i]);
			char	   *opt = TextDatumGetCString(datum_opts[i + 1]);

			options = lappend(options, makeDefElem(name, (Node *) makeString(opt)));
		}
	}

	p->tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = p->tupstore;
	rsinfo->setDesc = p->tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/*
	 * XXX: It's impolite to ignore our argument and keep decoding until the
	 * current position.
	 */
	if (!RecoveryInProgress())
		now = GetFlushRecPtr();
	else
		now = GetXLogReplayRecPtr(NULL);

	CheckLogicalDecodingRequirements();
	ReplicationSlotAcquire(NameStr(*name));

	PG_TRY();
	{
		ctx = CreateDecodingContext(false,
									NULL,
									MyReplicationSlot->confirmed_flush,
									options,
									logical_read_local_xlog_page,
									LogicalOutputPrepareWrite,
									LogicalOutputWrite);

		ctx->output_writer_private = p;

		startptr = MyReplicationSlot->restart_decoding;

		elog(LOG, "Starting logical replication from %X/%X to %X/%X, previously flushed %X/%X",
			 (uint32) (MyReplicationSlot->restart_decoding >> 32),
			 (uint32) MyReplicationSlot->restart_decoding,
			 (uint32) (MyReplicationSlot->confirmed_flush >> 32),
			 (uint32) MyReplicationSlot->confirmed_flush,
			 (uint32) (now >> 32), (uint32) now);

		CurrentResourceOwner = ResourceOwnerCreate(CurrentResourceOwner, "logical decoding");

		/* invalidate non-timetravel entries */
		InvalidateSystemCaches();

		while ((startptr != InvalidXLogRecPtr && startptr < now) ||
			   (ctx->reader->EndRecPtr && ctx->reader->EndRecPtr < now))
		{
			XLogRecord *record;
			char	   *errm = NULL;

			record = XLogReadRecord(ctx->reader, startptr, &errm);
			if (errm)
				elog(ERROR, "%s", errm);

			startptr = InvalidXLogRecPtr;

			/*
			 * The {begin_txn,change,commit_txn}_wrapper callbacks above will
			 * store the description into our tuplestore.
			 */
			if (record != NULL)
				DecodeRecordIntoReorderBuffer(ctx, record);
		}

		/*
		 * If everything went well, we can perform orderly cleanup, otherwise
		 * memory context cleanup has to do all the chin-ups. Will call the
		 * "cleanup_slot" callback.
		 */
		FreeDecodingContext(ctx);
	}
	PG_CATCH();
	{
		ReplicationSlotRelease();

		/*
		 * clear timetravel entries: XXX allowed in aborted TXN?
		 */
		InvalidateSystemCaches();

		PG_RE_THROW();
	}
	PG_END_TRY();

	rp = ctx->reader->EndRecPtr;
	if (rp >= now)
	{
		elog(DEBUG1, "Reached endpoint (wanted: %X/%X, got: %X/%X)",
			 (uint32) (now >> 32), (uint32) now,
			 (uint32) (rp >> 32), (uint32) rp);
	}

	tuplestore_donestoring(tupstore);

	CurrentResourceOwner = old_resowner;

	/*
	 * Next time, start where we left off. (Hunting things, the family
	 * business..)
	 */
	if (ctx->reader->EndRecPtr != InvalidXLogRecPtr && confirm)
		LogicalConfirmReceivedLocation(ctx->reader->EndRecPtr);

	ReplicationSlotRelease();
	InvalidateSystemCaches();

	return (Datum) 0;
}

Datum
decoding_slot_get_changes(PG_FUNCTION_ARGS)
{
	Datum ret = decoding_slot_get_changes_guts(fcinfo, true);
	return ret;
}

Datum
decoding_slot_peek_changes(PG_FUNCTION_ARGS)
{
	Datum ret = decoding_slot_get_changes_guts(fcinfo, false);
	return ret;
}

Datum
decoding_slot_get_binary_changes(PG_FUNCTION_ARGS)
{
	Datum ret = decoding_slot_get_changes_guts(fcinfo, true);
	return ret;
}

Datum
decoding_slot_peek_binary_changes(PG_FUNCTION_ARGS)
{
	Datum ret = decoding_slot_get_changes_guts(fcinfo, false);
	return ret;
}

Datum
drop_replication_slot(PG_FUNCTION_ARGS)
{
	Name		name = PG_GETARG_NAME(0);

	check_permissions();

	CheckSlotRequirements();

	ReplicationSlotDrop(NameStr(*name));

	PG_RETURN_INT32(0);
}
