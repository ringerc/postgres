/*
 * optimizer/util/rowsecurity.c
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

/* flags to pull row-security policy */
#define RSEC_FLAG_HAS_SUBLINKS			0x0001

/* hook to allow extensions to apply their own security policy */
row_security_policy_hook_type	row_security_policy_hook = NULL;

/*
 * make_pseudo_column
 *
 * It makes a target-entry node that references underlying column.
 * Its tle->expr is usualy Var node, but may be Const for dummy NULL
 * if the supplied attribute was already dropped.
 */
static TargetEntry *
make_pseudo_column(RangeTblEntry *subrte, AttrNumber attnum)
{
	Expr   *expr;
	char   *resname;

	Assert(subrte->rtekind == RTE_RELATION && OidIsValid(subrte->relid));
	if (attnum == InvalidAttrNumber)
	{
		expr = (Expr *) makeWholeRowVar(subrte, (Index) 1, 0, false);
		resname = get_rel_name(subrte->relid);
	}
	else
	{
		HeapTuple	tuple;
		Form_pg_attribute	attform;

		tuple = SearchSysCache2(ATTNUM,
								ObjectIdGetDatum(subrte->relid),
								Int16GetDatum(attnum));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for attribute %d of relation %u",
				 attnum, subrte->relid);
		attform = (Form_pg_attribute) GETSTRUCT(tuple);

		if (attform->attisdropped)
		{
			char	namebuf[NAMEDATALEN];

			/* Insert NULL just for a placeholder of dropped column */
			expr = (Expr *) makeConst(INT4OID,
									  -1,
									  InvalidOid,
									  sizeof(int32),
									  (Datum) 0,
									  true,		/* isnull */
									  true);	/* byval */
			sprintf(namebuf, "dummy-%d", (int)attform->attnum);
			resname = pstrdup(namebuf);
		}
		else
		{
			expr = (Expr *) makeVar((Index) 1,
									attform->attnum,
									attform->atttypid,
									attform->atttypmod,
									attform->attcollation,
									0);
			resname = pstrdup(NameStr(attform->attname));
		}
		ReleaseSysCache(tuple);
	}
	return makeTargetEntry(expr, -1, resname, false);
}

/*
 * lookup_pseudo_column
 *
 * It looks-up resource number of the target-entry relevant to the given
 * Var-node that references the row-security subquery. If required column
 * is not in the subquery's target-list, this function also adds new one
 * and returns its resource number.
 */
static AttrNumber
lookup_pseudo_column(PlannerInfo *root,
					 RangeTblEntry *rte, AttrNumber varattno)
{
	Query		   *subqry;
	RangeTblEntry  *subrte;
	TargetEntry	   *subtle;
	ListCell	   *cell;

	Assert(rte->rtekind == RTE_SUBQUERY &&
		   rte->subquery->querySource == QSRC_ROW_SECURITY);

	subqry = rte->subquery;
	foreach (cell, subqry->targetList)
	{
		subtle = lfirst(cell);

		/*
		 * If referenced artifical column is already constructed on the
		 * target-list of row-security subquery, nothing to do any more.
		 */
		if (IsA(subtle->expr, Var))
		{
			Var	   *subvar = (Var *)subtle->expr;

			Assert(subvar->varno == 1);
			if (subvar->varattno == varattno)
				return subtle->resno;
		}
	}

	/*
	 * OK, we don't have an artifical column relevant to the required ones,
	 * so let's create a new artifical column on demand.
	 */
	subrte = rt_fetch((Index) 1, subqry->rtable);
	subtle = make_pseudo_column(subrte, varattno);
	subtle->resno = list_length(subqry->targetList) + 1;

	subqry->targetList = lappend(subqry->targetList, subtle);
	rte->eref->colnames = lappend(rte->eref->colnames,
								  makeString(pstrdup(subtle->resname)));
	return subtle->resno;
}

/*
 * fixup_varnode_walker
 *
 * It recursively fixes up references to the relation to be replaced by
 * row-security sub-query, and adds pseudo columns relevant to the
 * underlying system columns or whole row-reference on demand.
 */
typedef struct {
	PlannerInfo	*root;
	int		varlevelsup;
	Index  *vartrans;
} fixup_varnode_context;

static bool
fixup_varnode_walker(Node *node, fixup_varnode_context *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var			   *var = (Var *) node;
		List		   *rtable = context->root->parse->rtable;
		RangeTblEntry  *rte;
		ListCell	   *cell;

		/*
		 * Ignore it, if Var node does not reference the Query currently
		 * we focus on.
		 */
		if (var->varlevelsup != context->varlevelsup)
			return false;

		if (context->vartrans[var->varno] > 0)
		{
			Index	rtindex_trans = context->vartrans[var->varno];

			rte = rt_fetch(rtindex_trans, rtable);
			Assert(rte->rtekind == RTE_SUBQUERY &&
				   rte->subquery->querySource == QSRC_ROW_SECURITY);

			var->varno = var->varnoold = rtindex_trans;
			var->varattno = lookup_pseudo_column(context->root, rte,
													 var->varattno);
		}
		else
		{
			rte = rt_fetch(var->varno, rtable);
			if (rte->rtekind == RTE_RELATION && rte->inh)
			{
				foreach (cell, context->root->append_rel_list)
				{
					AppendRelInfo  *appinfo = lfirst(cell);
					RangeTblEntry  *child_rte;

					if (appinfo->parent_relid != var->varno)
						continue;

					child_rte = rt_fetch(appinfo->child_relid, rtable);
					if (child_rte->rtekind == RTE_SUBQUERY &&
						child_rte->subquery->querySource == QSRC_ROW_SECURITY)
						(void) lookup_pseudo_column(context->root,
													child_rte,
													var->varattno);
				}
			}
		}
	}
	else if (IsA(node, RangeTblRef))
	{
		RangeTblRef  *rtr = (RangeTblRef *) node;

		if (context->varlevelsup == 0 &&
			context->vartrans[rtr->rtindex] != 0)
			rtr->rtindex = context->vartrans[rtr->rtindex];
	}
	else if (IsA(node, Query))
	{
		bool	result;

		context->varlevelsup++;
		result = query_tree_walker((Query *) node,
								   fixup_varnode_walker,
								   (void *) context, 0);
		context->varlevelsup--;

		return result;
	}
	return expression_tree_walker(node,
								  fixup_varnode_walker,
								  (void *) context);
}

/*
 * check_infinite_recursion
 *
 * It is a wrong row-security configuration, if we try to expand
 * the relation inside of row-security subquery originated from
 * same relation!
 */
static void
check_infinite_recursion(PlannerInfo *root, Oid relid)
{
	PlannerInfo	   *parent = root->parent_root;

	if (parent && parent->parse->querySource == QSRC_ROW_SECURITY)
	{
		RangeTblEntry  *rte = rt_fetch(1, parent->parse->rtable);

		Assert(rte->rtekind == RTE_RELATION && OidIsValid(rte->relid));

		if (relid == rte->relid)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("infinite recursion detected for relation \"%s\"",
							get_rel_name(relid))));
		check_infinite_recursion(parent, relid);
	}
}

/*
 * expand_rtentry_with_policy
 *
 * It extends a range-table entry of row-security sub-query with supplied
 * security policy, and append it on the parse->rtable.
 * This sub-query contains pseudo columns that reference underlying
 * regular columns (at least, references to system column or whole of
 * table reference shall be added on demand), and simple scan on the
 * target relation.
 * Any Var nodes that referenced the relation pointed by rtindex shall
 * be adjusted to reference this sub-query instead. walker
 */
static Index
expand_rtentry_with_policy(PlannerInfo *root, Index rtindex,
						   Expr *qual, int flags)
{
	Query		   *parse = root->parse;
	RangeTblEntry  *rte = rt_fetch(rtindex, parse->rtable);
	Query		   *subqry;
	RangeTblEntry  *subrte;
	RangeTblRef	   *subrtr;
	TargetEntry	   *subtle;
	RangeTblEntry  *newrte;
	HeapTuple		tuple;
	AttrNumber		nattrs;
	AttrNumber		attnum;
	List		   *targetList = NIL;
	List		   *colNameList = NIL;
	PlanRowMark	   *rowmark;

	Assert(rte->rtekind == RTE_RELATION && !rte->inh);

	/* check recursion to prevent infinite loop */
	check_infinite_recursion(root, rte->relid);

	/* Expand views inside SubLink node */
	if (flags & RSEC_FLAG_HAS_SUBLINKS)
		QueryRewriteExpr((Node *)qual, list_make1_oid(rte->relid));

	/*
	 * Construction of sub-query
	 */
	subqry = (Query *) makeNode(Query);
	subqry->commandType = CMD_SELECT;
	subqry->querySource = QSRC_ROW_SECURITY;

	subrte = copyObject(rte);
	subqry->rtable = list_make1(subrte);

	subrtr = makeNode(RangeTblRef);
	subrtr->rtindex = 1;
	subqry->jointree = makeFromExpr(list_make1(subrtr), (Node *) qual);
	if (flags & RSEC_FLAG_HAS_SUBLINKS)
		subqry->hasSubLinks = true;

	/*
	 * Construction of TargetEntries that reference underlying columns.
	 */
	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rte->relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", rte->relid);
	nattrs = ((Form_pg_class) GETSTRUCT(tuple))->relnatts;
	ReleaseSysCache(tuple);

	for (attnum = 1; attnum <= nattrs; attnum++)
	{
		subtle = make_pseudo_column(subrte, attnum);
		subtle->resno = list_length(targetList) + 1;
		Assert(subtle->resno == attnum);

		targetList = lappend(targetList, subtle);
		colNameList = lappend(colNameList,
							  makeString(pstrdup(subtle->resname)));
	}
	subqry->targetList = targetList;

	/* Expand RengeTblEntry with this sub-query */
	newrte = makeNode(RangeTblEntry);
	newrte->rtekind = RTE_SUBQUERY;
	newrte->subquery = subqry;
	newrte->security_barrier = true;
	newrte->rowsec_relid = rte->relid;
	newrte->eref = makeAlias(get_rel_name(rte->relid), colNameList);

	parse->rtable = lappend(parse->rtable, newrte);

	/*
	 * Fix up PlanRowMark if needed, then add references to 'tableoid' and
	 * 'ctid' that shall be added to handle row-level locking.
	 * Also see preprocess_targetlist() that adds some junk attributes.
	 */
	rowmark = get_plan_rowmark(root->rowMarks, rtindex);
	if (rowmark)
	{
		if (rowmark->rti == rowmark->prti)
			rowmark->rti = rowmark->prti = list_length(parse->rtable);
		else
			rowmark->rti = list_length(parse->rtable);

		lookup_pseudo_column(root, newrte, SelfItemPointerAttributeNumber);
		lookup_pseudo_column(root, newrte, TableOidAttributeNumber);
	}
	return list_length(parse->rtable);
}

/*
 * pull_row_security_policy
 *
 * It pulls the configured row-security policy of both built-in and
 * extensions. If any, it returns expression tree.
 */
static Expr *
pull_row_security_policy(CmdType cmd, Relation relation, int *p_flags)
{
	Expr   *quals = NULL;
	int		flags = 0;

	/*
	 * Pull the row-security policy configured with built-in features,
	 * if unprivileged users. Please note that superuser can bypass it.
	 */
	if (relation->rsdesc && !superuser())
	{
		RowSecurityDesc *rsdesc = relation->rsdesc;

		quals = copyObject(rsdesc->rsall.qual);
		if (rsdesc->rsall.hassublinks)
			flags |= RSEC_FLAG_HAS_SUBLINKS;
	}

	/*
	 * Also, ask extensions whether they want to apply their own
	 * row-security policy. If both built-in and extension has
	 * their own policy, it shall be merged.
	 */
	if (row_security_policy_hook)
	{
		List   *temp;

		temp = (*row_security_policy_hook)(cmd, relation);
		if (temp != NIL)
		{
			if ((flags & RSEC_FLAG_HAS_SUBLINKS) == 0 &&
				contain_subplans((Node *) temp))
				flags |= RSEC_FLAG_HAS_SUBLINKS;

			if (quals != NULL)
				temp = lappend(temp, quals);

			if (list_length(temp) == 1)
				quals = (Expr *)list_head(temp);
			else if (list_length(temp) > 1)
				quals = makeBoolExpr(AND_EXPR, temp, -1);
		}
	}
	*p_flags = flags;
	return quals;
}

/*
 * copy_row_security_policy
 *
 * It construct a row-security subquery instead of raw COPY TO statement,
 * if target relation has a row-level security policy
 */
bool
copy_row_security_policy(CopyStmt *stmt, Relation rel, List *attnums)
{
	Expr		  *quals;
	int			   flags;
	Query		  *parse;
	RangeTblEntry  *rte;
	RangeTblRef	   *rtr;
	TargetEntry	   *tle;
	Var			   *var;
	ListCell	   *cell;

	if (stmt->is_from)
		return false;

	quals = pull_row_security_policy(CMD_SELECT, rel, &flags);
	if (!quals)
		return false;

	parse = (Query *) makeNode(Query);
	parse->commandType = CMD_SELECT;
	parse->querySource = QSRC_ROW_SECURITY;

	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(rel);
	rte->relkind = RelationGetForm(rel)->relkind;

	foreach (cell, attnums)
	{
		HeapTuple	tuple;
		Form_pg_attribute	attform;
		AttrNumber	attno = lfirst_int(cell);

		tuple = SearchSysCache2(ATTNUM,
								ObjectIdGetDatum(RelationGetRelid(rel)),
								Int16GetDatum(attno));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for attribute %d of relation %s",
				 attno, RelationGetRelationName(rel));
		attform = (Form_pg_attribute) GETSTRUCT(tuple);

		var = makeVar((Index) 1,
					  attform->attnum,
					  attform->atttypid,
					  attform->atttypmod,
					  attform->attcollation,
					  0);
		tle = makeTargetEntry((Expr *) var,
							  list_length(parse->targetList) + 1,
							  pstrdup(NameStr(attform->attname)),
							  false);
		parse->targetList = lappend(parse->targetList, tle);

		ReleaseSysCache(tuple);

		rte->selectedCols = bms_add_member(rte->selectedCols,
								attno - FirstLowInvalidHeapAttributeNumber);
	}
	rte->inFromCl = true;
	rte->requiredPerms = ACL_SELECT;

	rtr = makeNode(RangeTblRef);
	rtr->rtindex = 1;

	parse->jointree = makeFromExpr(list_make1(rtr), (Node *) quals);
	parse->rtable = list_make1(rte);
	if (flags & RSEC_FLAG_HAS_SUBLINKS)
		parse->hasSubLinks = true;

	stmt->query = (Node *) parse;

	return true;
}

/*
 * apply_row_security_relation
 *
 * It applies row-security policy on a particular relation being specified.
 * If this relation is top of the inheritance tree, it also checks inherited
 * children.
 */
static bool
apply_row_security_relation(PlannerInfo *root, Index *vartrans,
							CmdType cmd, Index rtindex)
{
	Query		   *parse = root->parse;
	RangeTblEntry  *rte = rt_fetch(rtindex, parse->rtable);
	Relation		rel;
	Expr		   *qual;
	int				flags;
	bool			result = false;

	if (!rte->inh)
	{
		rel = heap_open(rte->relid, NoLock);
		qual = pull_row_security_policy(cmd, rel, &flags);
		if (qual)
		{
			vartrans[rtindex]
				= expand_rtentry_with_policy(root, rtindex, qual, flags);
			if (parse->resultRelation == rtindex)
				parse->sourceRelation = vartrans[rtindex];
			result = true;
		}
		heap_close(rel, NoLock);
	}
	else
	{
		/*
		 * In case when relation has inherited children, we try to apply
		 * row-level security policy of them if configured.
		 * In addition to regular replacement with a sub-query, we need
		 * to adjust rtindex of AppendRelInfo and varno of translated_vars.
		 * It makes sub-queries perform like regular relations being
		 * inherited from a particular parent relation. So, a table scan
		 * may have underlying a relation scan and two sub-query scans for
		 * instance. If it is result relation of UPDATE or DELETE command,
		 * rtindex to the original relation (regular relation) has to be
		 * kept because sub-query cannot perform as an updatable relation.
		 * So, we save it on child_result of AppendRelInfo; that shall be
		 * used to track relations to be modified at inheritance_planner().
		 */
		ListCell   *lc1, *lc2;

		foreach (lc1, root->append_rel_list)
		{
			AppendRelInfo  *apinfo = lfirst(lc1);

			if (apinfo->parent_relid != rtindex)
				continue;

			if (apply_row_security_relation(root, vartrans, cmd,
											apinfo->child_relid))
			{
				/*
				 * Save the rtindex of actual relation to be modified,
				 * if parent relation is result relation of this query.
				 */
				if (parse->resultRelation == rtindex)
					apinfo->child_result = apinfo->child_relid;

				apinfo->child_relid = vartrans[apinfo->child_relid];
				/* Adjust varno to reference pseudo columns */
				foreach (lc2, apinfo->translated_vars)
				{
					Var	   *var = lfirst(lc2);

					if (var)
						var->varno = apinfo->child_relid;
				}
				result = true;
			}
		}
	}
	return result;
}

/*
 * apply_row_security_recursive
 *
 * It walks on the given join-tree to replace relations with row-level
 * security policy by a simple sub-query.
 */
static bool
apply_row_security_recursive(PlannerInfo *root, Index *vartrans, Node *jtnode)
{
	bool	result = false;

	if (jtnode == NULL)
		return false;
	if (IsA(jtnode, RangeTblRef))
	{
		Index			rtindex = ((RangeTblRef *) jtnode)->rtindex;
		Query		   *parse = root->parse;
		RangeTblEntry  *rte = rt_fetch(rtindex, parse->rtable);
		CmdType			cmd;

		/* Only relation can have row-security policy */
		if (rte->rtekind != RTE_RELATION)
			return false;

		/*
		 * Prevents infinite recursion. Please note that rtindex == 1
		 * of the row-security subquery is a relation being already
		 * processed on the upper level.
		 */
		if (parse->querySource == QSRC_ROW_SECURITY && rtindex == 1)
			return false;

		/* Is it a result relation of UPDATE or DELETE command? */
		if (parse->resultRelation == rtindex)
			cmd = parse->commandType;
		else
			cmd = CMD_SELECT;

		/* Try to apply row-security policy, if configured */
		result = apply_row_security_relation(root, vartrans, cmd, rtindex);
	}
	else if (IsA(jtnode, FromExpr))
	{
		FromExpr   *f = (FromExpr *) jtnode;
		ListCell   *l;

		foreach (l, f->fromlist)
		{
			if (apply_row_security_recursive(root, vartrans, lfirst(l)))
				result = true;
		}
	}
	else if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) jtnode;

		if (apply_row_security_recursive(root, vartrans, j->larg))
			result = true;
		if (apply_row_security_recursive(root, vartrans, j->rarg))
			result = true;
	}
	else
		elog(ERROR, "unexpected node type: %d", (int) nodeTag(jtnode));

	return result;
}

/*
 * apply_row_security_policy
 *
 * Entrypoint to apply configured row-security policy of the relation.
 *
 * In case when the supplied query references relations with row-security
 * policy, its RangeTblEntry shall be replaced by a row-security subquery
 * that has simple scan on the referenced table with policy qualifiers.
 * Of course, security-barrier shall be set on the subquery to prevent
 * unexpected push-down of functions without leakproof flag.
 *
 * For example, when table t1 has a security policy "(x % 2 = 0)", the
 * following query:
 *   SELECT * FROM t1 WHERE f_leak(y)
 * performs as if
 *   SELECT * FROM (
 *     SELECT x, y FROM t1 WHERE (x % 2 = 0)
 *   ) AS t1 WHERE f_leak(y)
 * would be given. Because the sub-query has security barrier flag, 
 * configured security policy qualifier is always executed prior to
 * user given functions.
 */
void
apply_row_security_policy(PlannerInfo *root)
{
	Query	   *parse = root->parse;
	Oid			curr_userid;
	int			curr_seccxt;
	Index	   *vartrans;

	/*
	 * Mode checks. In case when SECURITY_ROW_LEVEL_DISABLED is set,
	 * no row-level security policy should be applied regardless
	 * whether it is built-in or extension.
	 */
	GetUserIdAndSecContext(&curr_userid, &curr_seccxt);
	if (curr_seccxt & SECURITY_ROW_LEVEL_DISABLED)
		return;

	vartrans = palloc0(sizeof(Index) * (list_length(parse->rtable) + 1));
	if (apply_row_security_recursive(root, vartrans, (Node *)parse->jointree))
	{
		PlannerGlobal  *glob = root->glob;
		PlanInvalItem  *pi_item;
		fixup_varnode_context context;

		/*
		 * Constructed Plan with row-level security policy depends on
		 * properties of current user (database superuser can bypass
		 * configured row-security policy!), thus, it has to be
		 * invalidated when its assumption was changed.
		 */
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

		/*
		 * Var-nodes that referenced RangeTblEntry to be replaced by
		 * row-security sub-query have to be adjusted for appropriate
		 * reference to the underlying pseudo column of the relation.
		 */
		context.root = root;
		context.varlevelsup = 0;
		context.vartrans = vartrans;
		query_tree_walker(parse,
						  fixup_varnode_walker,
						  (void *) &context,
						  QTW_IGNORE_RETURNING);
	}
	pfree(vartrans);
}
