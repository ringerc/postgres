#include "postgres.h"
#include "fmgr.h"
#include "tcop/utility.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "access/heapam.h"

/*
 * bdr_commandfilter.c: a ProcessUtility_hook to prevent a cluster from running
 * commands that BDR does not yet support.
 */

static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;

static bool bdr_permit_unsafe_commands;

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
	RangeVar *rv;
	Relation rel;
	bool needswal;

	ereport(DEBUG4, (errmsg_internal("bdr_commandfilter ProcessUtility_hook invoked")));
	if (bdr_permit_unsafe_commands)
		severity = WARNING;
	

	/* TODO: Permit 'unsafe' statements on TEMPORARY and UNLOGGED tables */
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
			{
				rv = lfirst(cell);
				rel = heap_openrv(rv, AccessExclusiveLock);
				needswal = RelationNeedsWAL(rel);
				heap_close(rel, AccessExclusiveLock);
				if (needswal)
					ereport(ERROR, (errmsg("TRUNCATE may only affect UNLOGGED or TEMPORARY tables when BDR is active; %s is a regular table", RelationGetRelationName(rel))));
			}
			break;

		case T_ClusterStmt:
			/* TODO: Permit CLUSTER on UNLOGGED or TEMPORARY tables */
			ereport(severity, (errmsg("CLUSTER is unsafe with BDR active.")));
			break;

		case T_VacuumStmt:
			/* We must prevent VACUUM FULL, but allow normal VACUUM */
			/* TODO: Permit VACUUM FULL on UNLOGGED or TEMPORARY tables */
			if ( ((VacuumStmt *) parsetree)->options & VACOPT_FULL)
				ereport(severity, (errmsg("VACUUM FULL is unsafe with BDR active, use only ordinary VACUUM.")));
			break;

		case T_RenameStmt:
		case T_CreateTableAsStmt:
		case T_DropStmt:
		case T_DropOwnedStmt:
			ereport(WARNING, (errmsg("%s is unsafe with BDR active unless it only affects TEMPORARY and UNLOGGED tables or non-table objects", completionTag)));
			break;

		case T_CreateStmt:
			if ((((CreateStmt*)parsetree)->relation->relpersistence) == 'p')
				ereport(severity, (errmsg("CREATE TABLE is only safe for UNLOGGED or TEMPORARY tables when BDR is active")));
				break;

		case T_AlterTableStmt:
			if ((((AlterTableStmt*)parsetree)->relation->relpersistence) == 'p')
				ereport(severity, (errmsg("Most ALTER TABLE commands are only safe for UNLOGGED or TEMPORARY tables when BDR is active")));
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
