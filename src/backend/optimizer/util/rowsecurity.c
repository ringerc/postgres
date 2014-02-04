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
	ListCell   *lc;
	Relation 	rel;
	Oid			userid;
	int			sec_context;
	bool qualsAdded = false;

	GetUserIdAndSecContext(&userid, &sec_context);

	if (rte->relid >= FirstNormalObjectId
		&& rte->relkind == 'r'
		&& !rte->rowsec_done
		&& !(sec_context & SECURITY_ROW_LEVEL_DISABLED))
	{
		/*
		 * Test for infinite recursion. Each RTE carrys a list of relids
		 * whose row-security qualifiers were expanded to generate that RTE.
		 *
		 * For RTEs that arent't the result of row-security qual expansion
		 * it's NIL. Otherwise, it's our breadcrumb trail. If we find ourself
		 * on the breadcrumb trail, we're in a loop and need to bail out.
		 *
		 * See row_security_expanded_rel for how the parent list is copied into
		 * RTEs in subqueries after row-security expansion.
		 */
		foreach(lc, rte->rowSecurityParents)
			if (lfirst_oid(lc) == rte->relid)
				/* TODO: we have enough info to report the recursion path. Do it. */
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("infinite recursion detected for relation \"%s\"",
							 get_rel_name(rte->relid))));

		/* rowSecurityParents will get copied to the subquery RTE and any nested
		 * RTEs in sublinks, so we can just append our own oid to it here. */
		rte->rowSecurityParents = lcons_oid(rte->relid, rte->rowSecurityParents);

		/* Then fetch the row-security qual and add it to the list of quals
		 * to be expanded by expand_security_quals */
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
List *
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

struct set_rowsecparent_walker_context
{
	List		*parentList;
};

static bool
set_rowsecparent_walker(Node* node, struct set_rowsecparent_walker_context *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry* rte = (RangeTblEntry*)node;
		/* Sanity check - parent list must be NIL, or exaclty what we're going
		 * to be copying into place anyway. */
		Assert(rte->rowSecurityParents == NIL || equal(rte->rowSecurityParents, context->parentList));
		if (rte->rtekind == RTE_RELATION)
		{
			/* We'll hit the top level RangeTblEntry that's already got our
			 * parentList, so only modify entries with no parent list. */
			if (rte->rowSecurityParents == NIL)
				rte->rowSecurityParents = (List*) copyObject(context->parentList);
			
		}
		/* Examine any children, in case this is a subquery node or has securityQuals */
		return true;
	}

	/* Recurse into subqueries, that's why we're here */
	if (IsA(node, Query))
		return query_tree_walker((Query *) node, &set_rowsecparent_walker, (void*) context, QTW_EXAMINE_RTES);

	return expression_tree_walker(node, &set_rowsecparent_walker, (void *) context);
}

/*
 * After row-security quals are expanded by expand_security_quals, it's
 * necessary to copy the rowSecurityParents list in the RTE's top level (as set
 * up by prepend_row_security_quals above) to any inner RTEs in the product
 * query. When those RTEs are examined and expanded we can check for infinite
 * recursion at that point.
 *
 * This is a wrapper around a walker.
 */
void
row_security_expanded_rel(PlannerInfo *root, RangeTblEntry* rte, Oid parentRelid)
{
	Assert(rte->rtekind == RTE_SUBQUERY);
	struct set_rowsecparent_walker_context context;
	context.parentList = rte->rowSecurityParents;
	(void) query_tree_walker(rte->subquery, &set_rowsecparent_walker, &context, QTW_EXAMINE_RTES);
}
