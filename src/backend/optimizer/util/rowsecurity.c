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
 * apply_row_security_rte
 *
 * Apply row-security policy to a RangeTblEntry if policy for the
 * RTE's Relation exists.
 *
 * In addition to the initial RTE scan in subquery planner, this also
 * gets called directly from expand_inherited_rtentry to ensure
 * that row-security quals applied to children get added.
 */
bool
apply_row_security_rte(PlannerInfo *root, RangeTblEntry* rte)
{
	Relation		rel;
	List		   *rowsecquals;
	bool			result = false;

	Assert(rte->rtekind == RTE_RELATION);
	rel = heap_open(rte->relid, NoLock);
	rowsecquals = pull_row_security_policy(root->parse->commandType, rel);
	if (rowsecquals)
	{
		rte->securityQuals = lcons(rowsecquals, rte->securityQuals);
		result = true;
	}
	heap_close(rel, NoLock);
	return result;
}

/*
 * Scan the rangetable of the Query, looking for relations with
 * row-security policies. Where a policy is found, add its securityQuals
 * to the RTE then recurse into the quals
 */
static bool
apply_row_security_rangetbl(PlannerInfo *root)
{
	ListCell *lc;
	RangeTblEntry * rte;
	bool policy_found = false;

	foreach (lc, root->parse->rtable)
	{
		rte = (RangeTblEntry*) lfirst(lc);
		/* Only act on normal tables that are not builtin system catalogs */
		if (rte->rtekind == RTE_RELATION && rte->relid >= FirstNormalObjectId)
			policy_found &= apply_row_security_rte(root, rte);
	}
	return policy_found;
}

/*
 * apply_row_security_policy
 *
 * Entrypoint to apply configured row-security policy of the relation.
 *
 * In the case when the supplied query references relations with row-security
 * policy, the relation's RangeTblEntry has the row-security qualifiers
 * appended to the RTE's securityQual list. This will cause the optimizer to
 * replace the relation with a security_barrier subquery over the original
 * relation
 *
 * That happens after inheritance expansion has occurred, so any qual on a
 * parent relation automatically applies to the child as the RTE is copied.
 * RLS quals are checked for during inheritance expansion and any child
 * table-specific quals are applied then, producing a second level of
 * security_barrier subquery.
 */
void
apply_row_security_policy(PlannerInfo *root)
{
	Query	   *parse = root->parse;
	Oid			curr_userid;
	int			curr_seccxt;

	/*
	 * Mode checks. In case when SECURITY_ROW_LEVEL_DISABLED is set,
	 * no row-level security policy should be applied regardless
	 * whether it is built-in or extension.
	 */
	GetUserIdAndSecContext(&curr_userid, &curr_seccxt);
	if (curr_seccxt & SECURITY_ROW_LEVEL_DISABLED)
		return;

	if (apply_row_security_rangetbl(root))
	{
		/*
		 * We added at least one securityQual, making the plan
		 * potentially dependent on whether the current user
		 * is row-security exempt.
		 */

		PlannerGlobal  *glob = root->glob;
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
}
