#include "postgres.h"
#include "fmgr.h"
#include "tcop/utility.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "utils/guc.h"

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

	ereport(DEBUG4, (errmsg_internal("bdr_commandfilter ProcessUtility_hook invoked")));

	/* TODO: Permit 'unsafe' statements on TEMPORARY and UNLOGGED tables */
	switch (nodeTag(parsetree)) {
		case T_AlterTableStmt:
		case T_AlterTableCmd:
		case T_TruncateStmt:
		case T_ClusterStmt:
			if (bdr_permit_unsafe_commands)
				ereport(WARNING, (errmsg("Command %s is unsafe with BDR active but is being permitted because bdr.permit_unsafe_commands is on.", completionTag)));
			else
				ereport(ERROR, (errmsg("Command %s is unsafe with BDR active, see the documentation and bdr.permit_unsafe_commands", completionTag)));
			break;
		case T_VacuumStmt:
			/* We must prevent VACUUM FULL, but allow normal VACUUM */
			/* TODO */
			ereport(WARNING, (errmsg("VACUUM FULL is unsafe with BDR active, use only ordinary VACUUM.")));
			break;
		case T_RenameStmt:
		case T_CreateStmt:
		case T_CreateTableAsStmt:
		case T_DropStmt:
		case T_DropOwnedStmt:
			ereport(WARNING, (errmsg("Command %s is unsafe with BDR active unless it only affects TEMPORARY and UNLOGGED tables or non-table objects", completionTag)));
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
