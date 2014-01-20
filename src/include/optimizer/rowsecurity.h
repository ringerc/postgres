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

extern bool	copy_row_security_policy(CopyStmt *stmt,
									 Relation relation, List *attnums);
extern void	apply_row_security_policy(PlannerInfo *root);

#endif	/* ROWSECURITY_H */
