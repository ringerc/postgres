/*-------------------------------------------------------------------------
 *
 * test_elog_annotations.c
 *		Exercise errannot() / errannotf() plumbing from SQL.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/modules/test_elog_annotations/test_elog_annotations.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_test_errannot_emit);
PG_FUNCTION_INFO_V1(pg_test_errannot_rethrow);
PG_FUNCTION_INFO_V1(pg_test_errannot_throwdata);

/*
 * Decode an SQL elevel name into the corresponding numeric constant.  We
 * deliberately exclude FATAL/PANIC because we don't want test helpers to
 * tear down the cluster, and we exclude ERROR from the "emit" path because
 * raising ERROR would interfere with the function-return contract; the
 * rethrow helper takes a separate code path for ERROR-level testing.
 */
static int
parse_elevel(const char *name, bool allow_error)
{
	if (strcmp(name, "DEBUG5") == 0)
		return DEBUG5;
	if (strcmp(name, "DEBUG4") == 0)
		return DEBUG4;
	if (strcmp(name, "DEBUG3") == 0)
		return DEBUG3;
	if (strcmp(name, "DEBUG2") == 0)
		return DEBUG2;
	if (strcmp(name, "DEBUG1") == 0)
		return DEBUG1;
	if (strcmp(name, "LOG") == 0)
		return LOG;
	if (strcmp(name, "INFO") == 0)
		return INFO;
	if (strcmp(name, "NOTICE") == 0)
		return NOTICE;
	if (strcmp(name, "WARNING") == 0)
		return WARNING;
	if (allow_error && strcmp(name, "ERROR") == 0)
		return ERROR;
	ereport(ERROR,
			errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("unsupported elevel \"%s\"", name));
	pg_unreachable();
}

/*
 * Walk a (keys[], values[]) pair of text[] arrays, invoking the supplied
 * callback for each row.  Both arrays must have the same number of
 * elements, neither may contain NULLs, and 1-D shapes are required.
 */
typedef void (*kv_callback) (const char *key, const char *value);

static int
foreach_kv(ArrayType *keys, ArrayType *values, kv_callback cb)
{
	Datum	   *k_datums;
	Datum	   *v_datums;
	bool	   *k_nulls;
	bool	   *v_nulls;
	int			k_count;
	int			v_count;

	if (ARR_NDIM(keys) > 1 || ARR_NDIM(values) > 1)
		ereport(ERROR,
				errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				errmsg("annotation key/value arrays must be one-dimensional"));

	deconstruct_array_builtin(keys, TEXTOID, &k_datums, &k_nulls, &k_count);
	deconstruct_array_builtin(values, TEXTOID, &v_datums, &v_nulls, &v_count);

	if (k_count != v_count)
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("annotation key/value arrays must have the same length"));

	for (int i = 0; i < k_count; i++)
	{
		if (k_nulls[i] || v_nulls[i])
			ereport(ERROR,
					errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					errmsg("annotation key/value entries must not be NULL"));
		cb(TextDatumGetCString(k_datums[i]),
		   TextDatumGetCString(v_datums[i]));
	}
	return 0;					/* shaped like err*() helpers */
}

/*
 * Trampolines used by the array walkers below.  They forward to errannot()
 * or errannotf() respectively.  errannotf() is exercised via a fixed
 * "%s/%d" format so we get coverage of the printf path without having to
 * surface format strings through SQL.
 */
static void
plain_cb(const char *key, const char *value)
{
	(void) errannot(key, value);
}

static void
formatted_cb(const char *key, const char *value)
{
	(void) errannotf(key, "fmt:%s/%d", value, (int) strlen(value));
}

/*
 * replay_annotations --- re-attach the annotations from a saved ErrorData
 * to whichever ereport() call surrounds us.  Used by the lifecycle helper
 * below to verify that annotations survive CopyErrorData() / FlushErrorState()
 * by re-emitting them into a fresh log record.  Returns 0 so it can be
 * dropped into an ereport() argument list like the err*() helpers.
 */
static int
replay_annotations(const ErrorAnnotation *src)
{
	for (; src != NULL; src = src->next)
		(void) errannot(src->key, src->value);
	return 0;
}

/*
 * pg_test_errannot_emit --- attach the supplied annotations and emit a
 * single log/notice/warning message.  The message goes through the
 * standard ereport() pipeline so the JSON, CSV, and stderr destinations
 * all see it.
 *
 *   SELECT pg_test_errannot_emit('LOG', 'hello',
 *                                ARRAY['trace_id'], ARRAY['abc'],
 *                                ARRAY['ext.dur'], ARRAY['100']);
 */
Datum
pg_test_errannot_emit(PG_FUNCTION_ARGS)
{
	char	   *elevel_name = TextDatumGetCString(PG_GETARG_DATUM(0));
	char	   *msg = TextDatumGetCString(PG_GETARG_DATUM(1));
	ArrayType  *keys = PG_GETARG_ARRAYTYPE_P(2);
	ArrayType  *values = PG_GETARG_ARRAYTYPE_P(3);
	ArrayType  *fkeys = PG_GETARG_ARRAYTYPE_P(4);
	ArrayType  *fvalues = PG_GETARG_ARRAYTYPE_P(5);
	int			elevel = parse_elevel(elevel_name, false);

	/*
	 * Emit a single ereport() containing all annotations.  errannot()
	 * (called via the plain_cb / formatted_cb trampolines) is only valid
	 * between errstart() and errfinish(), which the ereport() macro
	 * arranges for the duration of its argument list.
	 */
	ereport(elevel,
			errmsg_internal("%s", msg),
			foreach_kv(keys, values, plain_cb),
			foreach_kv(fkeys, fvalues, formatted_cb));

	PG_RETURN_VOID();
}

/*
 * pg_test_errannot_rethrow --- exercise CopyErrorData()/FreeErrorDataContents
 * lifecycle for annotations.
 *
 * We raise a synthetic ERROR carrying annotations inside PG_TRY, catch it,
 * CopyErrorData() out, FlushErrorState(), and then re-emit a LOG record
 * that re-attaches the saved annotations via replay_annotations().  The
 * TAP test inspects that LOG record to verify the annotations survived
 * the copy.  This is preferred over calling ReThrowError() to drive the
 * verification: ereport(ERROR) inside PG_TRY does *not* call
 * EmitErrorReport (see errfinish), and the outer handler that does
 * EmitErrorReport on the rethrown error can be a several PG_TRY frames
 * out, making the log-side verification fragile.
 */
Datum
pg_test_errannot_rethrow(PG_FUNCTION_ARGS)
{
	ArrayType  *keys = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *values = PG_GETARG_ARRAYTYPE_P(1);
	MemoryContext oldcxt = CurrentMemoryContext;
	ErrorData  *copy = NULL;

	PG_TRY();
	{
		ereport(ERROR,
				errcode(ERRCODE_RAISE_EXCEPTION),
				errmsg_internal("inner ereport that PG_CATCH will swallow"),
				foreach_kv(keys, values, plain_cb));
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcxt);
		copy = CopyErrorData();
		FlushErrorState();
	}
	PG_END_TRY();

	/* The annotations must have survived CopyErrorData(). */
	if (copy->annotations == NULL)
		ereport(ERROR,
				errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("annotations were lost across CopyErrorData()"));

	/*
	 * Emit a fresh LOG record carrying the surviving annotations re-
	 * attached via replay_annotations().  The TAP test grep's for the
	 * "rethrow probe" message and asserts on the annotation values.
	 */
	ereport(LOG,
			errmsg_internal("rethrow probe"),
			replay_annotations(copy->annotations));

	FreeErrorData(copy);
	PG_RETURN_VOID();
}

/*
 * pg_test_errannot_throwdata --- verify annotations survive ThrowErrorData().
 *
 * Memory-context correctness proof for the ThrowErrorData path:
 *
 *   1. We build an ErrorData in a short-lived child context (child of
 *      CurrentMemoryContext).  Its annotations live in that child context.
 *   2. ThrowErrorData() calls errstart(), which allocates a fresh live
 *      error-stack entry with assoc_context = ErrorContext, then copies
 *      every field — including annotations — into ErrorContext.  The
 *      child-context originals are never read again by the error subsystem.
 *   3. Inside PG_CATCH we CopyErrorData() (into CurrentMemoryContext = oldcxt)
 *      then FlushErrorState() which resets ErrorContext.  The copy's annotations
 *      live in oldcxt and therefore survive.
 *   4. We delete the child context to show its storage is already gone, and
 *      then emit a LOG with the copy's annotations re-attached.  If the
 *      annotations had NOT been copied out of the child context they would
 *      be dangling pointers at this point.
 *
 * The TAP test inspects the LOG record to confirm the correct values.
 */
Datum
pg_test_errannot_throwdata(PG_FUNCTION_ARGS)
{
	ArrayType  *keys = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *values = PG_GETARG_ARRAYTYPE_P(1);
	MemoryContext oldcxt = CurrentMemoryContext;
	MemoryContext child;
	ErrorData  *src;
	ErrorData  *copy = NULL;

	/*
	 * Build a throwable ErrorData in a child context.  We use palloc0 so
	 * every field we don't set explicitly is zeroed (same as what errstart
	 * initialises).  The annotations we attach via foreach_kv will be
	 * allocated in this child context.
	 */
	child = AllocSetContextCreate(CurrentMemoryContext,
								  "errannot_throwdata_child",
								  ALLOCSET_SMALL_SIZES);
	MemoryContextSwitchTo(child);

	src = palloc0(sizeof(ErrorData));
	src->elevel = ERROR;
	src->sqlerrcode = ERRCODE_RAISE_EXCEPTION;
	src->assoc_context = child;
	/* filename/lineno/funcname left as NULL/0 — ThrowErrorData tolerates that */

	/*
	 * Attach annotations.  These are allocated in the child context because
	 * src->assoc_context == child and set_annotation() switches into
	 * assoc_context.  (errannot() cannot be called here because there is no
	 * live ereport() in progress; we call the internal helper directly via the
	 * public array-walking path.)
	 *
	 * We can't call errannot() outside an ereport() so we set annotations by
	 * hand using the same structure that set_annotation() would produce.
	 */
	{
		Datum	   *k_datums,
				   *v_datums;
		bool	   *k_nulls,
				   *v_nulls;
		int			k_count,
					v_count;

		deconstruct_array_builtin(keys, TEXTOID, &k_datums, &k_nulls, &k_count);
		deconstruct_array_builtin(values, TEXTOID, &v_datums, &v_nulls, &v_count);

		if (k_count != v_count)
			ereport(ERROR,
					errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("annotation key/value arrays must have the same length"));

		/* Build list in child context (assoc_context = child) */
		for (int i = 0; i < k_count; i++)
		{
			ErrorAnnotation *ann;

			if (k_nulls[i] || v_nulls[i])
				ereport(ERROR,
						errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("annotation key/value entries must not be NULL"));

			ann = palloc(sizeof(ErrorAnnotation));
			ann->key = pstrdup(TextDatumGetCString(k_datums[i]));
			ann->value = pstrdup(TextDatumGetCString(v_datums[i]));
			ann->next = src->annotations;
			src->annotations = ann;
		}
	}

	MemoryContextSwitchTo(oldcxt);

	/*
	 * Now throw the error.  ThrowErrorData() will copy the annotations out of
	 * the child context and into ErrorContext before it longjmps.
	 */
	PG_TRY();
	{
		ThrowErrorData(src);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcxt);
		copy = CopyErrorData();
		FlushErrorState();			/* resets ErrorContext */
	}
	PG_END_TRY();

	/*
	 * Delete the child context that held the original annotations.  After this
	 * the src->annotations pointers are dangling.  The live-error copy's
	 * annotations were copied into ErrorContext by ThrowErrorData, and then
	 * into oldcxt by CopyErrorData — so copy->annotations must be intact.
	 */
	MemoryContextDelete(child);
	src = NULL;					/* prevent accidental use */

	if (copy == NULL || copy->annotations == NULL)
		ereport(ERROR,
				errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("annotations were lost across ThrowErrorData()"));

	/*
	 * Re-emit as a LOG so the TAP test can grep for the annotation values.
	 */
	ereport(LOG,
			errmsg_internal("throwdata probe"),
			replay_annotations(copy->annotations));

	FreeErrorData(copy);
	PG_RETURN_VOID();
}
