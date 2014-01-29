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
 * Check the given RTE to see whether it's already had row-security
 * quals expanded and, if not, prepend any row-security rules
 * from built-in or plug-in sources to the securityQuals.
 *
 * Returns true if any quals were added.
 */
bool
prepend_row_security_quals(CmdType commandType, RangeTblEntry* rte)
{
	List	   *rowsecquals;
	Relation 	rel;
	Oid			userid;
	int			sec_context;

	GetUserIdAndSecContext(&userid, &sec_context);

	bool qualsAdded = false;
	if (rte->relid >= FirstNormalObjectId
		&& rte->relkind == RTE_RELATION
		&& !rte->rowsec_done
		&& !(sec_context & SECURITY_ROW_LEVEL_DISABLED))
	{
		rel = heap_open(rte->relid, NoLock);
		rowsecquals = pull_row_security_policy(commandType, rel);
		if (rowsecquals)
		{
			rte->securityQuals = list_concat(rowsecquals, rte->securityQuals);
			qualsAdded = true;
		}
		heap_close(rel, NoLock);
		rte->rowsec_done = true;
	}
	return qualsAdded;
}
