/*-------------------------------------------------------------------------
 *
 * slotfuncs.c
 *
 *	   Support functions for using replication slots
 *

 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logicalfuncs.c
 *
 */

#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "nodes/makefuncs.h"

#include "utils/builtins.h"

#include "replication/slot.h"

#include "storage/fd.h"

/*
 * pg_get_replication_slots - SQL SRF showing active replication slots.
 */
Datum
pg_get_replication_slots(PG_FUNCTION_ARGS)
{
#define PG_STAT_GET_LOGICAL_DECODING_SLOTS_COLS 8
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	int			i;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* FIXME: what permissions do we require? */

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *slot = &ReplicationSlotCtl->replication_slots[i];
		Datum		values[PG_STAT_GET_LOGICAL_DECODING_SLOTS_COLS];
		bool		nulls[PG_STAT_GET_LOGICAL_DECODING_SLOTS_COLS];
		char		location[MAXFNAMELEN];
		const char *slot_name;
		const char *plugin;
		TransactionId catalog_xmin;
		TransactionId data_xmin;
		XLogRecPtr	last_req;
		bool		active;
		Oid			database;

		SpinLockAcquire(&slot->mutex);
		if (!slot->in_use)
		{
			SpinLockRelease(&slot->mutex);
			continue;
		}
		else
		{
			catalog_xmin = slot->catalog_xmin;
			data_xmin = slot->data_xmin;
			active = slot->active;
			database = slot->database;
			last_req = slot->restart_decoding;
			slot_name = pstrdup(NameStr(slot->name));
			plugin = pstrdup(NameStr(slot->plugin));
		}
		SpinLockRelease(&slot->mutex);

		memset(nulls, 0, sizeof(nulls));

		snprintf(location, sizeof(location), "%X/%X",
				 (uint32) (last_req >> 32), (uint32) last_req);

		values[0] = CStringGetTextDatum(slot_name);
		values[1] = CStringGetTextDatum(plugin);
		values[2] = CStringGetTextDatum("logical");
		values[3] = database;
		values[4] = BoolGetDatum(active);
		values[5] = TransactionIdGetDatum(catalog_xmin);
		values[6] = TransactionIdGetDatum(data_xmin);
		values[7] = CStringGetTextDatum(location);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}
