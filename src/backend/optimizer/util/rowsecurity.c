/*
 * rewrite/rowsecurity.c
 *    Routines to support row-security feature
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_rowsecurity.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "optimizer/clauses.h"
#include "optimizer/prep.h"
#include "optimizer/rowsecurity.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteHandler.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "tcop/utility.h"

/* hook to allow extensions to apply their own security policy */
row_security_policy_hook_type	row_security_policy_hook = NULL;

static void
add_uid_plan_inval(PlannerGlobal* glob);

static List *
pull_row_security_policy(CmdType cmd, Relation relation);

/*
 * Check the given RTE to see whether it's already had row-security
 * quals expanded and, if not, prepend any row-security rules
 * from built-in or plug-in sources to the securityQuals.
 *
 * Returns true if any quals were added.
 */
bool
prepend_row_security_quals(PlannerInfo* root, RangeTblEntry* rte)
{
	List	   *rowsecquals;
	Relation 	rel;
	Oid			userid;
	int			sec_context;

	GetUserIdAndSecContext(&userid, &sec_context);

	bool qualsAdded = false;
	if (rte->relid >= FirstNormalObjectId
		&& rte->relkind == 'r'
		&& !rte->rowsec_done
		&& !(sec_context & SECURITY_ROW_LEVEL_DISABLED))
	{
		rel = heap_open(rte->relid, NoLock);
		rowsecquals = pull_row_security_policy(root->parse->commandType, rel);
		if (rowsecquals)
		{
			rte->securityQuals = list_concat(rowsecquals, rte->securityQuals);
			qualsAdded = true;
		}
		heap_close(rel, NoLock);
		rte->rowsec_done = true;
	}
	if (qualsAdded)
		add_uid_plan_inval(root->glob);
	return qualsAdded;
}

/*
 * pull_row_security_policy
 *
 * Fetches the configured row-security policy of both built-in catalogs and any
 * extensions. If any policy is found a list of qualifier expressions is
 * returned, where each is treated as a securityQual.
 */
static List *
pull_row_security_policy(CmdType cmd, Relation relation)
{
	List   *quals = NIL;
	Expr   *qual = NULL;

	/*
	 * Pull the row-security policy configured with built-in features,
	 * if unprivileged users. Please note that superuser can bypass it.
	 */
	if (relation->rsdesc && !superuser())
	{
		RowSecurityDesc *rsdesc = relation->rsdesc;
		qual = copyObject(rsdesc->rsall.qual);
		quals = lcons(qual, quals);
	}

	/*
	 * Also, ask extensions whether they want to apply their own
	 * row-security policy. If both built-in and extension has
	 * their own policy they're applied as nested qualifiers.
	 */
	if (row_security_policy_hook)
	{
		List   *temp;

		temp = (*row_security_policy_hook)(cmd, relation);
		if (temp != NIL)
			lcons(temp, quals);
	}
	return quals;
}

/*
 * Row-security plans are dependent on the current user id because of the if
 * (superuser) test. So row-security plans must be invalidated if the user id
 * changes.
 */
static void
add_uid_plan_inval(PlannerGlobal* glob)
{
	PlanInvalItem  *pi_item;

	if (!OidIsValid(glob->planUserId))
	{
		/* Plan invalidation on session user-id */
		glob->planUserId = GetUserId();
	
		/* Plan invalidation on catalog updates of pg_authid */
		pi_item = makeNode(PlanInvalItem);
		pi_item->cacheId = AUTHOID;
		pi_item->hashValue =
		GetSysCacheHashValue1(AUTHOID,
	    					  ObjectIdGetDatum(glob->planUserId));
		glob->invalItems = lappend(glob->invalItems, pi_item);
	}
	else
		Assert(glob->planUserId == GetUserId());

}
