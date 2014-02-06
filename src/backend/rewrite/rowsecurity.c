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
#include "parser/parsetree.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rowsecurity.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "tcop/utility.h"

/* hook to allow extensions to apply their own security policy */
row_security_policy_hook_type	row_security_policy_hook = NULL;

/*
 * Check the given RTE to see whether it's already had row-security
 * quals expanded and, if not, prepend any row-security rules
 * from built-in or plug-in sources to the securityQuals.
 *
 * Returns true if any quals were added.
 */
static bool
prepend_row_security_quals(Query* root, RangeTblEntry* rte)
{
	List	   *rowsecquals;
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
		/* Then fetch the row-security qual and add it to the list of quals
		 * to be expanded by expand_security_quals. Rewriting will recurse into
		 * securityQuals. */
		rel = heap_open(rte->relid, NoLock);
		rowsecquals = pull_row_security_policy(root->commandType, rel);
		if (rowsecquals)
		{
			rte->securityQuals = list_concat(rowsecquals, rte->securityQuals);
			qualsAdded = true;
		}
		heap_close(rel, NoLock);
		rte->rowsec_done = true;
	}
	if (qualsAdded)
		elog(WARNING, "Added quals, but not plan invalidation for user id");
		/* add_uid_plan_inval(root->glob); */
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

struct rewrite_mutator_context
{
	List   *rewrite_events;
};

static Node*
row_security_rewrite_mutator(Node *node, struct rewrite_mutator_context *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Query))
	{
		Query *q = (Query*) node;
		List *rewriteproducts;

		/* TODO: check for recursion and update rewrite_events */

		/* We need to rewrite this Query node, and replace it with the product of the
		 * rewrite process. For now, extra product queries aren't supported. */
		rewriteproducts = RewriteQuery(q, context->rewrite_events);
		if (list_length(rewriteproducts) == 1)
		{
			Query *newquery = (Query*) linitial(rewriteproducts); 
			Assert(IsA(newquery, Query));
			/* Recurse into the rewritten subquery, return the mutated product */
			return (Node*) query_tree_mutator((Query *) node, &row_security_rewrite_mutator, (void*) context, 0);
		} else {
			/* TODO: better error, see CTE errors in RewriteQuery */
			elog(ERROR, "Bad query rewrite product on row security qual subquery. Better error goes here");
		}
	}

	return expression_tree_mutator(node, &row_security_rewrite_mutator, (void *) context);
}

struct policy_walker_context
{
	Query  *parsetree;
	bool	policy_applied;
	List   *rewrite_events;
};


static bool
row_security_apply_walker(Node *node, struct policy_walker_context *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry*)node;
		/* Apply security policy to the RTE, tagging it with securityQuals */
		if (prepend_row_security_quals(context->parsetree, rte))
		{
			/* TODO: All this should happen inside prepend_row_security_quals ? */
			struct rewrite_mutator_context rewritecontext;
			rewritecontext.rewrite_events = context->rewrite_events;

			context->policy_applied = true;
			/* The new securityQuals could contain subqueries that must be 
			 * rewritten, and have row security quals applied. If we return
			 * now we'll recurse into the newly added securityQuals, but they
			 * won't have been rewritten, so things like views will remain
			 * unexpanded and they'll be injected into the plan tree later,
			 * causing chaos. We must rewrite any subqueries in the quals,
			 * replacing them with the rewrite product, and only after rewrite
			 * walk the product to check for security barrier quals to expand.
			 *
			 * We can just replace the securityQuals list at this point. It
			 * didn't exist before we entered row_security_apply_walker, and
			 * it'll be visited by the walker _after_ we return, so we're saving
			 * a lot of overhead vs a mutator.
			 */
			rte->securityQuals = (List*) expression_tree_mutator( (Node*)(rte->securityQuals), &row_security_rewrite_mutator, (void*)&rewritecontext);
		}
		return true;
	}

	if (IsA(node, Query))
	{
		Query *q = (Query*) node;
		Query *oldquery;
		bool ret;

		/* Recurse into subqueries, that's why we're here */
		oldquery = context->parsetree;
		context->parsetree = q;
		ret = query_tree_walker((Query *) node, &row_security_apply_walker, (void*) context, QTW_EXAMINE_RTES);
		context->parsetree = oldquery;
		return ret;
	}

	return expression_tree_walker(node, &row_security_apply_walker, (void *) context);
}

/*
 * Iterates over a range table, checking each RTE for row-security quals and applying
 * them if found.
 *
 * See prepend_row_security_quals for the interesting bits.
 */
bool
apply_row_security_policies(Query *parsetree, List *rewrite_events)
{
	struct policy_walker_context context;
	context.policy_applied = false;
	context.parsetree = parsetree;
	context.rewrite_events = rewrite_events;
	(void) query_tree_walker(parsetree, &row_security_apply_walker, &context, QTW_EXAMINE_RTES);
	return context.policy_applied;
}
