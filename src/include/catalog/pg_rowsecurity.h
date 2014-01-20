/*
 * pg_rowsecurity.h
 *   definition of the system catalog for row-security policy (pg_rowsecurity)
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 */
#ifndef PG_ROWSECURITY_H
#define PG_ROWSECURITY_H

#include "catalog/genbki.h"
#include "nodes/primnodes.h"
#include "utils/memutils.h"
#include "utils/relcache.h"

/* ----------------
 *		pg_rowlevelsec definition. cpp turns this into
 *		typedef struct FormData_pg_rowlevelsec
 * ----------------
 */
#define RowSecurityRelationId	5000

CATALOG(pg_rowsecurity,5000)
{
	/* Oid of the relation that has row-security policy */
	Oid				rsecrelid;

	/* One of ROWSECURITY_CMD_* below */
	char			rseccmd;
#ifdef CATALOG_VARLEN
	pg_node_tree	rsecqual;
#endif
} FormData_pg_rowsecurity;

/* ----------------
 *		Form_pg_rowlevelsec corresponds to a pointer to a row with
 *		the format of pg_rowlevelsec relation.
 * ----------------
 */
typedef FormData_pg_rowsecurity *Form_pg_rowsecurity;

/* ----------------
 * 		compiler constants for pg_rowlevelsec
 * ----------------
 */
#define Natts_pg_rowsecurity				3
#define Anum_pg_rowsecurity_rsecrelid		1
#define Anum_pg_rowsecurity_rseccmd			2
#define Anum_pg_rowsecurity_rsecqual		3

#define ROWSECURITY_CMD_ALL			'a'
#define ROWSECURITY_CMD_SELECT		's'
#define ROWSECURITY_CMD_INSERT		'i'
#define ROWSECURITY_CMD_UPDATE		'u'
#define ROWSECURITY_CMD_DELETE		'd'

typedef struct
{
	Oid			rsecid;
	Expr	   *qual;
	bool		hassublinks;
} RowSecurityEntry;

typedef struct
{
	MemoryContext		rscxt;
	RowSecurityEntry	rsall;	/* row-security policy for ALL */
} RowSecurityDesc;

extern void	RelationBuildRowSecurity(Relation relation);
extern void	ATExecSetRowSecurity(Relation relation,
								 const char *cmdname, Node *clause);
extern void	RemoveRowSecurityById(Oid relationId);

#endif  /* PG_ROWSECURITY_H */
