/*-------------------------------------------------------------------------
 *
 * test_decoding.c
 *		  example output plugin for the logical decoding functionality
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/test_decoding/test_decoding.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"

#include "catalog/pg_class.h"
#include "catalog/pg_type.h"

#include "nodes/parsenodes.h"

#include "replication/output_plugin.h"
#include "replication/logical.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


PG_MODULE_MAGIC;

void		_PG_init(void);

typedef struct
{
	MemoryContext context;
	bool		include_xids;
	bool		include_timestamp;
} TestDecodingData;

/* These must be available to pg_dlsym() */
extern void pg_decode_init(LogicalDecodingContext *ctx, bool is_init);
extern void pg_decode_cleanup(LogicalDecodingContext *ctx);
extern void pg_decode_begin_txn(LogicalDecodingContext *ctx,
					ReorderBufferTXN *txn);
extern void pg_decode_commit_txn(LogicalDecodingContext *ctx,
					 ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
extern void pg_decode_change(LogicalDecodingContext *ctx,
				 ReorderBufferTXN *txn, Relation rel,
				 ReorderBufferChange *change);

void
_PG_init(void)
{
}

/* initialize this plugin */
void
pg_decode_init(LogicalDecodingContext *ctx, bool is_init)
{
	ListCell   *option;
	TestDecodingData *data;

	AssertVariableIsOfType(&pg_decode_init, LogicalDecodeInitCB);

	data = palloc(sizeof(TestDecodingData));
	data->context = AllocSetContextCreate(TopMemoryContext,
										  "text conversion context",
										  ALLOCSET_DEFAULT_MINSIZE,
										  ALLOCSET_DEFAULT_INITSIZE,
										  ALLOCSET_DEFAULT_MAXSIZE);
	data->include_xids = true;
	data->include_timestamp = false;

	ctx->output_plugin_private = data;

	foreach(option, ctx->output_plugin_options)
	{
		DefElem    *elem = lfirst(option);

		Assert(elem->arg == NULL || IsA(elem->arg, String));

		if (strcmp(elem->defname, "include-xids") == 0)
		{
			/* if option does not provide a value, it means its value is true */
			if (elem->arg == NULL)
				data->include_xids = true;
			else if (!parse_bool(strVal(elem->arg), &data->include_xids))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "include-timestamp") == 0)
		{
			if (elem->arg == NULL)
				data->include_timestamp = true;
			else if (!parse_bool(strVal(elem->arg), &data->include_timestamp))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"%s\" = \"%s\" is unknown",
							elem->defname,
							elem->arg ? strVal(elem->arg) : "(null)")));
		}
	}
}

/* cleanup this plugin's resources */
void
pg_decode_cleanup(LogicalDecodingContext *ctx)
{
	TestDecodingData *data = ctx->output_plugin_private;

	AssertVariableIsOfType(&pg_decode_cleanup, LogicalDecodeCleanupCB);

	/* cleanup our own resources via memory context reset */
	MemoryContextDelete(data->context);
}

/* BEGIN callback */
void
pg_decode_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	TestDecodingData *data = ctx->output_plugin_private;

	AssertVariableIsOfType(&pg_decode_begin_txn, LogicalDecodeBeginCB);

	ctx->prepare_write(ctx, txn->end_lsn, txn->xid);
	if (data->include_xids)
		appendStringInfo(ctx->out, "BEGIN %u", txn->xid);
	else
		appendStringInfoString(ctx->out, "BEGIN");
	ctx->write(ctx, txn->end_lsn, txn->xid);
}

/* COMMIT callback */
void
pg_decode_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
					 XLogRecPtr commit_lsn)
{
	TestDecodingData *data = ctx->output_plugin_private;

	AssertVariableIsOfType(&pg_decode_commit_txn, LogicalDecodeCommitCB);

	ctx->prepare_write(ctx, txn->end_lsn, txn->xid);
	if (data->include_xids)
		appendStringInfo(ctx->out, "COMMIT %u", txn->xid);
	else
		appendStringInfoString(ctx->out, "COMMIT");

	if (data->include_timestamp)
		appendStringInfo(ctx->out, " (at %s)",
						 timestamptz_to_str(txn->commit_time));

	ctx->write(ctx, txn->end_lsn, txn->xid);
}

/* print the tuple 'tuple' into the StringInfo s */
static void
tuple_to_stringinfo(StringInfo s, TupleDesc tupdesc, HeapTuple tuple, bool skip_nulls)
{
	int			natt;
	Oid			oid;

	/* print oid of tuple, it's not included in the TupleDesc */
	if ((oid = HeapTupleHeaderGetOid(tuple->t_data)) != InvalidOid)
	{
		appendStringInfo(s, " oid[oid]:%u", oid);
	}

	/* print all columns individually */
	for (natt = 0; natt < tupdesc->natts; natt++)
	{
		Form_pg_attribute attr; /* the attribute itself */
		Oid			typid;		/* type of current attribute */
		HeapTuple	type_tuple; /* information about a type */
		Form_pg_type type_form;
		Oid			typoutput;	/* output function */
		bool		typisvarlena;
		Datum		origval;	/* possibly toasted Datum */
		Datum		val;		/* definitely detoasted Datum */
		char	   *outputstr = NULL;
		bool		isnull;		/* column is null? */

		attr = tupdesc->attrs[natt];

		/*
		 * don't print dropped columns, we can't be sure everything is
		 * available for them
		 */
		if (attr->attisdropped)
			continue;

		/*
		 * Don't print system columns, oid will already have been printed if
		 * present.
		 */
		if (attr->attnum < 0)
			continue;

		typid = attr->atttypid;

		/* gather type name */
		type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
		if (!HeapTupleIsValid(type_tuple))
			elog(ERROR, "cache lookup failed for type %u", typid);
		type_form = (Form_pg_type) GETSTRUCT(type_tuple);

		/* get Datum from tuple */
		origval = fastgetattr(tuple, natt + 1, tupdesc, &isnull);

		if (isnull && skip_nulls)
			continue;

		/* print attribute name */
		appendStringInfoChar(s, ' ');
		appendStringInfoString(s, NameStr(attr->attname));

		/* print attribute type */
		appendStringInfoChar(s, '[');
		appendStringInfoString(s, NameStr(type_form->typname));
		appendStringInfoChar(s, ']');

		/* query output function */
		getTypeOutputInfo(typid,
						  &typoutput, &typisvarlena);

		ReleaseSysCache(type_tuple);

		if (isnull)
			outputstr = "(null)";
		else if (typisvarlena && VARATT_IS_EXTERNAL_ONDISK(origval))
			outputstr = "(unchanged-toast-datum)";
		else if (typisvarlena)
			val = PointerGetDatum(PG_DETOAST_DATUM(origval));
		else
			val = origval;

		/* call output function if necessary */
		if (outputstr == NULL)
			outputstr = OidOutputFunctionCall(typoutput, val);

		/* print data */
		appendStringInfoChar(s, ':');
		appendStringInfoString(s, outputstr);
	}
}

/*
 * callback for individual changed tuples
 */
void
pg_decode_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				 Relation relation, ReorderBufferChange *change)
{
	TestDecodingData *data;
	Form_pg_class class_form;
	TupleDesc	tupdesc;
	MemoryContext old;

	AssertVariableIsOfType(&pg_decode_change, LogicalDecodeChangeCB);

	data = ctx->output_plugin_private;
	class_form = RelationGetForm(relation);
	tupdesc = RelationGetDescr(relation);

	/* Avoid leaking memory by using and resetting our own context */
	old = MemoryContextSwitchTo(data->context);

	ctx->prepare_write(ctx, change->lsn, txn->xid);

	appendStringInfoString(ctx->out, "table \"");
	appendStringInfoString(ctx->out, NameStr(class_form->relname));
	appendStringInfoString(ctx->out, "\":");

	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			appendStringInfoString(ctx->out, " INSERT:");
			if (change->tp.newtuple == NULL)
				appendStringInfoString(ctx->out, " (no-tuple-data)");
			else
				tuple_to_stringinfo(ctx->out, tupdesc,
									&change->tp.newtuple->tuple,
									false);
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			appendStringInfoString(ctx->out, " UPDATE:");
			if (change->tp.oldtuple != NULL)
			{
				appendStringInfoString(ctx->out, " old-key:");
				tuple_to_stringinfo(ctx->out, tupdesc,
									&change->tp.oldtuple->tuple,
									true);
				appendStringInfoString(ctx->out, " new-tuple:");
			}

			if (change->tp.newtuple == NULL)
				appendStringInfoString(ctx->out, " (no-tuple-data)");
			else
				tuple_to_stringinfo(ctx->out, tupdesc,
									&change->tp.newtuple->tuple,
									false);
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			appendStringInfoString(ctx->out, " DELETE:");

			/* if there was no PK, we only know that a delete happened */
			if (change->tp.oldtuple == NULL)
				appendStringInfoString(ctx->out, " (no-tuple-data)");
			/* In DELETE, only the replica identity is present; display that */
			else
				tuple_to_stringinfo(ctx->out, tupdesc,
									&change->tp.oldtuple->tuple,
									true);
			break;
	}

	MemoryContextSwitchTo(old);
	MemoryContextReset(data->context);

	ctx->write(ctx, change->lsn, txn->xid);
}
