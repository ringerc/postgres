/* -------------------------------------------------------------------------
 *
 * pg_rowsecurity.c
 *    routines to support manipulation of the pg_rowsecurity catalog
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_class.h"
#include "catalog/pg_rowsecurity.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "parser/parse_clause.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

/*
 * Load row-security policy from the catalog, and keep it on
 * the relation cache.
 */
void
RelationBuildRowSecurity(Relation relation)
{
	Relation		catalog;
	ScanKeyData		skey;
	SysScanDesc		sscan;
	HeapTuple		tuple;
	MemoryContext	oldcxt;
	MemoryContext	rscxt = NULL;
	RowSecurityDesc	*rsdesc = NULL;

	catalog = heap_open(RowSecurityRelationId, AccessShareLock);

	ScanKeyInit(&skey,
				Anum_pg_rowsecurity_rsecrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(relation)));
	sscan = systable_beginscan(catalog, RowSecurityRelidIndexId, true,
							   NULL, 1, &skey);
	PG_TRY();
	{
		while (HeapTupleIsValid(tuple = systable_getnext(sscan)))
		{
			Datum	value;
			bool	isnull;
			char   *temp;

			if (!rsdesc)
			{
				rscxt = AllocSetContextCreate(CacheMemoryContext,
											  "Row-security descriptor",
											  ALLOCSET_SMALL_MINSIZE,
											  ALLOCSET_SMALL_INITSIZE,
											  ALLOCSET_SMALL_MAXSIZE);
				oldcxt = MemoryContextSwitchTo(rscxt);
				rsdesc = palloc0(sizeof(RowSecurityDesc));
				rsdesc->rscxt = rscxt;
				MemoryContextSwitchTo(oldcxt);
			}
			value = heap_getattr(tuple, Anum_pg_rowsecurity_rseccmd,
								 RelationGetDescr(catalog), &isnull);
			Assert(!isnull);

			if (DatumGetChar(value) != ROWSECURITY_CMD_ALL)
			{
				ereport(WARNING,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("Per-command row-security not implemented")));
				continue;
			}

			value = heap_getattr(tuple, Anum_pg_rowsecurity_rsecqual,
								 RelationGetDescr(catalog), &isnull);
			Assert(!isnull);
			temp = TextDatumGetCString(value);

			oldcxt = MemoryContextSwitchTo(rscxt);
			rsdesc->rsall.rsecid = HeapTupleGetOid(tuple);
			rsdesc->rsall.qual = (Expr *) stringToNode(temp);
			Assert(exprType((Node *)rsdesc->rsall.qual) == BOOLOID);
			rsdesc->rsall.hassublinks
				= contain_subplans((Node *)rsdesc->rsall.qual);
			MemoryContextSwitchTo(oldcxt);

			pfree(temp);
		}
	}
	PG_CATCH();
	{
		if (rscxt != NULL)
			MemoryContextDelete(rscxt);
		PG_RE_THROW();
	}
	PG_END_TRY();

	systable_endscan(sscan);
	heap_close(catalog, AccessShareLock);

	relation->rsdesc = rsdesc;
}

/*
 * Parse the supplied row-security policy, and insert/update a row
 * of pg_rowsecurity catalog.
 */
static void
InsertOrUpdatePolicyRow(Relation relation, char rseccmd, Node *clause)
{
	Oid				relationId = RelationGetRelid(relation);
	Oid				rowsecId;
	ParseState	   *pstate;
	RangeTblEntry  *rte;
	Node		   *qual;
	Relation		catalog;
	ScanKeyData		skeys[2];
	SysScanDesc		sscan;
	HeapTuple		oldtup;
	HeapTuple		newtup;
	Datum			values[Natts_pg_rowsecurity];
	bool			isnull[Natts_pg_rowsecurity];
	bool			replaces[Natts_pg_rowsecurity];
	ObjectAddress	target;
	ObjectAddress	myself;

	/* Parse the supplied clause */
	pstate = make_parsestate(NULL);

	rte = addRangeTableEntryForRelation(pstate, relation,
										NULL, false, false);
	addRTEtoQuery(pstate, rte, false, true, true);

	qual = transformWhereClause(pstate, copyObject(clause),
								EXPR_KIND_ROW_SECURITY,
								"ROW SECURITY");
	/* zero-clear */
	memset(values,   0, sizeof(values));
	memset(replaces, 0, sizeof(replaces));
	memset(isnull,   0, sizeof(isnull));

	/* Update or Insert an entry to pg_rowsecurity catalog  */
	catalog = heap_open(RowSecurityRelationId, RowExclusiveLock);

	ScanKeyInit(&skeys[0],
				Anum_pg_rowsecurity_rsecrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(relation)));
	ScanKeyInit(&skeys[1],
				Anum_pg_rowsecurity_rseccmd,
				BTEqualStrategyNumber, F_CHAREQ,
				CharGetDatum(rseccmd));
	sscan = systable_beginscan(catalog, RowSecurityRelidIndexId, true,
							   NULL, 2, skeys);
	oldtup = systable_getnext(sscan);
	if (HeapTupleIsValid(oldtup))
	{
		rowsecId = HeapTupleGetOid(oldtup);

		replaces[Anum_pg_rowsecurity_rsecqual - 1] = true;
		values[Anum_pg_rowsecurity_rsecqual - 1]
			= CStringGetTextDatum(nodeToString(qual));

		newtup = heap_modify_tuple(oldtup,
								   RelationGetDescr(catalog),
								   values, isnull, replaces);
		simple_heap_update(catalog, &newtup->t_self, newtup);

		deleteDependencyRecordsFor(RowSecurityRelationId, rowsecId, false);
	}
	else
	{
		values[Anum_pg_rowsecurity_rsecrelid - 1]
			= ObjectIdGetDatum(relationId);
		values[Anum_pg_rowsecurity_rseccmd - 1]
			= CharGetDatum(rseccmd);
		values[Anum_pg_rowsecurity_rsecqual - 1]
			= CStringGetTextDatum(nodeToString(qual));
		newtup = heap_form_tuple(RelationGetDescr(catalog),
								 values, isnull);
		rowsecId = simple_heap_insert(catalog, newtup);
	}
	CatalogUpdateIndexes(catalog, newtup);

	heap_freetuple(newtup);

	/* records dependencies of row-security policy and relation/columns */
	target.classId = RelationRelationId;
	target.objectId = relationId;
	target.objectSubId = 0;

	myself.classId = RowSecurityRelationId;
	myself.objectId = rowsecId;
	myself.objectSubId = 0;

	recordDependencyOn(&myself, &target, DEPENDENCY_AUTO);

	recordDependencyOnExpr(&myself, qual, pstate->p_rtable,
						   DEPENDENCY_NORMAL);
	free_parsestate(pstate);

	systable_endscan(sscan);
	heap_close(catalog, RowExclusiveLock);
}

/*
 * Remove row-security policy row of pg_rowsecurity
 */
static void
DeletePolicyRow(Relation relation, char rseccmd)
{
	Assert(rseccmd == ROWSECURITY_CMD_ALL);

	if (relation->rsdesc)
	{
		ObjectAddress	address;

		address.classId = RowSecurityRelationId;
		address.objectId = relation->rsdesc->rsall.rsecid;
		address.objectSubId = 0;

		performDeletion(&address, DROP_RESTRICT, 0);
	}
	else
	{
		/* Nothing to do here */
		elog(INFO, "relation %s has no row-security policy, skipped",
			 RelationGetRelationName(relation));
	}
}

/*
 * Guts of row-security policy deletion.
 */
void
RemoveRowSecurityById(Oid rowsecId)
{
	Relation	catalog;
	ScanKeyData	skey;
	SysScanDesc	sscan;
	HeapTuple	tuple;
	Relation	rel;
	Oid			relid;

	catalog = heap_open(RowSecurityRelationId, RowExclusiveLock);

	ScanKeyInit(&skey,
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(rowsecId));
	sscan = systable_beginscan(catalog, RowSecurityOidIndexId, true,
							   NULL, 1, &skey);
	tuple = systable_getnext(sscan);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "could not find tuple for row-security %u", rowsecId);

	/*
	 * Open and exclusive-lock the relation the row-security belongs to.
	 */
	relid = ((Form_pg_rowsecurity) GETSTRUCT(tuple))->rsecrelid;

	rel = heap_open(relid, AccessExclusiveLock);

	simple_heap_delete(catalog, &tuple->t_self);

	/* Ensure relcache entries of other session being rebuilt */
	CacheInvalidateRelcache(rel);

	heap_close(rel, NoLock);

	systable_endscan(sscan);
	heap_close(catalog, RowExclusiveLock);
}

/*
 * ALTER TABLE <name> SET ROW SECURITY (...) OR
 *                    RESET ROW SECURITY
 */
void
ATExecSetRowSecurity(Relation relation, const char *cmdname, Node *clause)
{
	Oid			relid = RelationGetRelid(relation);
	char		rseccmd;

	if (strcmp(cmdname, "all") == 0)
		rseccmd = ROWSECURITY_CMD_ALL;
	else
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Row-security for \"%s\" is not implemented yet",
						cmdname)));

	if (clause != NULL)
	{
		InsertOrUpdatePolicyRow(relation, rseccmd, clause);

		/*
		 * Also, turn on relhasrowsecurity, if not.
		 */
		if (!RelationGetForm(relation)->relhasrowsecurity)
		{
			Relation	class_rel = heap_open(RelationRelationId,
											  RowExclusiveLock);
			HeapTuple	tuple;

			tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
			if (!HeapTupleIsValid(tuple))
				elog(ERROR, "cache lookup failed for relation %u", relid);

			((Form_pg_class) GETSTRUCT(tuple))->relhasrowsecurity = true;

			simple_heap_update(class_rel, &tuple->t_self, tuple);
			CatalogUpdateIndexes(class_rel, tuple);

			heap_freetuple(tuple);
			heap_close(class_rel, RowExclusiveLock);
		}
	}
	else
		DeletePolicyRow(relation, rseccmd);

	CacheInvalidateRelcache(relation);
}
