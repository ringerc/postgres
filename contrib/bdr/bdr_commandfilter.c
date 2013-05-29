#include "postgres.h"
#include "fmgr.h"
#include "tcop/utility.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "access/heapam.h"
#include "catalog/namespace.h"

/*
 * bdr_commandfilter.c: a ProcessUtility_hook to prevent a cluster from running
 * commands that BDR does not yet support.
 */

static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;

static bool bdr_permit_unsafe_commands;

/*
 * Check the passed rangevar, locking it and looking it up in the cache
 * then determine if the relation requires logging to WAL. If it does, then
 * right now BDR won't cope with it and we must reject the operation that
 * touches this relation.
 */
static void
error_on_persistent_rv(RangeVar *rv, const char * cmdtag, LOCKMODE lockmode, int severity, bool missing_ok)
{
	bool needswal;
	Relation rel;
	if (rv == NULL)
		ereport(severity, (errmsg("Unqualified command %s is unsafe with BDR active.", cmdtag)));
	rel = heap_openrv_extended(rv, lockmode, missing_ok);
	if (rel != NULL)
	{
		needswal = RelationNeedsWAL(rel);
		heap_close(rel, lockmode);
		if (needswal)
			ereport(severity, (errmsg("%s may only affect UNLOGGED or TEMPORARY tables when BDR is active; %s is a regular table", cmdtag, rv->relname)));
	}
}

static void
bdr_commandfilter(Node *parsetree,
                const char *queryString,
		ProcessUtilityContext context,
                ParamListInfo params,
                DestReceiver *dest,
                char *completionTag)
{
	int severity = ERROR;
	ListCell *cell;
	DropStmt* dropStatement;
	RenameStmt *renameStatement;
	VacuumStmt *vacuumStatement;
	AlterTableStmt* alterTableStatement;

	ereport(DEBUG4, (errmsg_internal("bdr_commandfilter ProcessUtility_hook invoked")));
	if (bdr_permit_unsafe_commands)
		severity = WARNING;
	

	switch (nodeTag(parsetree)) {
		case T_TruncateStmt:
			/*
			 * Constraints on permanent tables may reference only
			 * permanent tables. So if we can verify that there are
			 * no permanent tables in the list it's OK to permit
			 * the command even in the presence of CASCADE; we don't
			 * have to resolve the cascade because we know it's not
			 * going to affect permanent tables if none appear in the
			 * relation list.
			 */
			foreach(cell, ((TruncateStmt*)parsetree)->relations)
				error_on_persistent_rv(lfirst(cell), "TRUNCATE", AccessExclusiveLock, severity, false);
			break;

		case T_ClusterStmt:
			error_on_persistent_rv(((ClusterStmt*)parsetree)->relation, "CLUSTER", AccessExclusiveLock, severity, false);
			break;

		case T_VacuumStmt:
			/* We must prevent VACUUM FULL, but allow normal VACUUM */
		 	/* TODO: Permit VACUUM FULL on UNLOGGED or TEMPORARY tables */
			vacuumStatement = (VacuumStmt *) parsetree;
			if ( vacuumStatement->options & VACOPT_FULL)
			{
				/* VACUUM FULL might still be OK if it's on a TEMPORARY or UNLOGGED table */
				error_on_persistent_rv(vacuumStatement->relation, "VACUUM FULL", AccessExclusiveLock, severity, false);
			}
			break;

		case T_AlterTableStmt:
			alterTableStatement = (AlterTableStmt*)parsetree;
			error_on_persistent_rv(alterTableStatement->relation, "ALTER TABLE", AccessExclusiveLock, severity, alterTableStatement->missing_ok);
			break;

		case T_RenameStmt:
			renameStatement = (RenameStmt*)parsetree;
			if (renameStatement->renameType == OBJECT_TABLE) {
				error_on_persistent_rv(renameStatement->relation, "ALTER TABLE ... RENAME", AccessExclusiveLock, severity, renameStatement->missing_ok);
			}
			break;

		case T_DropStmt:
			/*
			 * DROP is fun to handle, since a DropStmt might be for a matview, index,
			 * etc, not just a table, per the comment on RemoveRelations in tablecmds.c
			 *
			 * We only handle the table case.
			 */
 			dropStatement = (DropStmt*)parsetree;
			if (dropStatement->removeType == OBJECT_TABLE)
			{
				foreach(cell, dropStatement->objects)
				{
					/* TODO: Is there any case where the nested list can be >1 entry long? If not, why is there a nested list? */
					if ( ((List*)lfirst(cell))->length != 1)
						elog(WARNING, "Internal error: Expected one-item nested list in DROP");
					else
						error_on_persistent_rv(makeRangeVarFromNameList((List *) lfirst(cell)), "DROP TABLE", AccessExclusiveLock, severity, dropStatement->missing_ok);
				}
			}
			break;

		case T_CreateTableAsStmt:
		case T_CreateStmt:
			/* relpersistence is already set for CREATE */
			if ((((CreateStmt*)parsetree)->relation->relpersistence) == 'p')
				ereport(severity, (errmsg("CREATE TABLE is only safe for UNLOGGED or TEMPORARY tables when BDR is active")));
				break;

		case T_DropOwnedStmt:
			/* We don't try to handle DROP OWNED at this point, there's just too much logic */
			ereport(severity, (errmsg("DROP OWNED is unsafe with BDR active unless it only affects TEMPORARY and UNLOGGED tables or non-table objects")));
			break;

				
		default:
			break;
	}

	if (next_ProcessUtility_hook)
	{
		ereport(DEBUG4, (errmsg_internal("bdr_commandfilter ProcessUtility_hook handing off to next hook ")));
		(*next_ProcessUtility_hook) (parsetree, queryString, context, params,
					dest, completionTag);
	}
	else
	{
		ereport(DEBUG4, (errmsg_internal("bdr_commandfilter ProcessUtility_hook invoking standard_ProcessUtility")));
		standard_ProcessUtility(parsetree, queryString, context, params,
					dest, completionTag);
	}
}

void _PG_init(void);

/* Module load */
void
init_bdr_commandfilter(void)
{
	ereport(DEBUG4, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg_internal("bdr_commandfilter ProcessUtility_hook installed")));
        next_ProcessUtility_hook = ProcessUtility_hook;
        ProcessUtility_hook = bdr_commandfilter;

        DefineCustomBoolVariable("bdr.permit_unsafe_commands",
			"Allow commands that might cause data or replication problems under BDR to run",
			NULL,
			&bdr_permit_unsafe_commands,
			false, PGC_SUSET,
			0,
			NULL, NULL, NULL);

}
