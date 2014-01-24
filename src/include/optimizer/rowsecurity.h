/* -------------------------------------------------------------------------
 *
 * rowsecurity.h
 *    prototypes for optimizer/rowsecurity.c
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * -------------------------------------------------------------------------
 */
#ifndef ROWSECURITY_H
#define ROWSECURITY_H

#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "nodes/relation.h"
#include "utils/rel.h"

typedef List *(*row_security_policy_hook_type)(CmdType cmdtype,
											   Relation relation);
extern PGDLLIMPORT row_security_policy_hook_type row_security_policy_hook;

extern bool prepend_row_security_quals(PlannerInfo* root, RangeTblEntry* rte);

extern List *pull_row_security_policy(CmdType cmd, Relation relation);

extern void row_security_expanded_rel(PlannerInfo *root, Query *subquery, Oid parentRelid);


#endif	/* ROWSECURITY_H */
