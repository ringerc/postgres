/*-------------------------------------------------------------------------
 *
 * tcopprot.h
 *	  prototypes for postgres.c.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/tcop/tcopprot.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TCOPPROT_H
#define TCOPPROT_H

#include "nodes/params.h"
#include "nodes/plannodes.h"
#include "storage/procsignal.h"
#include "utils/guc.h"
#include "utils/queryenvironment.h"

typedef struct ExplainState ExplainState;	/* defined in explain_state.h */

extern PGDLLIMPORT CommandDest whereToSendOutput;
extern PGDLLIMPORT const char *debug_query_string;
extern PGDLLIMPORT int PostAuthDelay;
extern PGDLLIMPORT int client_connection_check_interval;

/* GUC-configurable parameters */

typedef enum
{
	LOGSTMT_NONE,				/* log no statements */
	LOGSTMT_DDL,				/* log data definition statements */
	LOGSTMT_MOD,				/* log modification statements, plus DDL */
	LOGSTMT_ALL,				/* log all statements */
} LogStmtLevel;

extern PGDLLIMPORT bool Log_disconnections;
extern PGDLLIMPORT int log_statement;

/* Flags for restrict_nonsystem_relation_kind value */
#define RESTRICT_RELKIND_VIEW			0x01
#define RESTRICT_RELKIND_FOREIGN_TABLE	0x02

extern PGDLLIMPORT int restrict_nonsystem_relation_kind;

/*
 * Hook fired by PostgresMain immediately before each ReadyForQuery
 * message is sent --- the v3 protocol cycle boundary, after the
 * command(s) have completed and just before the server announces
 * itself idle again.
 *
 * Intended use: end-of-cycle teardown that needs to run once per
 * round-trip, not once per statement.  ReadyForQuery isn't
 * per-statement (multi-statement simple-Query, Bind/Execute
 * between Syncs, copy completion, error-recovery skip-till-Sync
 * all share one), so callers needing per-statement granularity
 * should combine post_parse_analyze_hook, ExecutorEnd_hook, and
 * ProcessUtility_hook instead.
 *
 * Chaining: this is a single function pointer.  Multiple extensions
 * sharing the hook MUST chain explicitly --- the second installer
 * silently overrides the first otherwise:
 *
 *	 static pre_ready_for_query_hook_type prev_hook;
 *
 *	 static void my_hook(void) {
 *	     ... do work ...
 *	     if (prev_hook)
 *	         prev_hook();
 *	 }
 *
 *	 void _PG_init(void) {
 *	     prev_hook = pre_ready_for_query_hook;
 *	     pre_ready_for_query_hook = my_hook;
 *	 }
 *
 * Error handling: PostgresMain wraps the call in PG_TRY/PG_CATCH and
 * logs+swallows any error raised by the hook so an ereport from
 * teardown code does not produce an infinite cycle (sigsetjmp
 * recovery re-sets send_ready_for_query, which would otherwise
 * re-fire the hook).  Hook authors should still treat their bodies
 * as non-throwing; the catch is a safety net, not a licence to
 * ignore errors.
 */
typedef void (*pre_ready_for_query_hook_type) (void);
extern PGDLLIMPORT pre_ready_for_query_hook_type pre_ready_for_query_hook;

extern List *pg_parse_query(const char *query_string);
extern List *pg_rewrite_query(Query *query);
extern List *pg_analyze_and_rewrite_fixedparams(RawStmt *parsetree,
												const char *query_string,
												const Oid *paramTypes, int numParams,
												QueryEnvironment *queryEnv);
extern List *pg_analyze_and_rewrite_varparams(RawStmt *parsetree,
											  const char *query_string,
											  Oid **paramTypes,
											  int *numParams,
											  QueryEnvironment *queryEnv);
extern List *pg_analyze_and_rewrite_withcb(RawStmt *parsetree,
										   const char *query_string,
										   ParserSetupHook parserSetup,
										   void *parserSetupArg,
										   QueryEnvironment *queryEnv);
extern PlannedStmt *pg_plan_query(Query *querytree, const char *query_string,
								  int cursorOptions,
								  ParamListInfo boundParams,
								  ExplainState *es);
extern List *pg_plan_queries(List *querytrees, const char *query_string,
							 int cursorOptions,
							 ParamListInfo boundParams);

extern void die(SIGNAL_ARGS);
pg_noreturn extern void quickdie(SIGNAL_ARGS);
extern void StatementCancelHandler(SIGNAL_ARGS);
pg_noreturn extern void FloatExceptionHandler(SIGNAL_ARGS);
extern void HandleRecoveryConflictInterrupt(void);
extern void ProcessClientReadInterrupt(bool blocked);
extern void ProcessClientWriteInterrupt(bool blocked);

extern void process_postgres_switches(int argc, char *argv[],
									  GucContext ctx, const char **dbname);
pg_noreturn extern void PostgresSingleUserMain(int argc, char *argv[],
											   const char *username);
pg_noreturn extern void PostgresMain(const char *dbname,
									 const char *username);
extern void ResetUsage(void);
extern void ShowUsage(const char *title);
extern int	check_log_duration(char *msec_str, bool was_logged);
extern void set_debug_options(int debug_flag,
							  GucContext context, GucSource source);
extern bool set_plan_disabling_options(const char *arg,
									   GucContext context, GucSource source);
extern const char *get_stats_option_name(const char *arg);

#endif							/* TCOPPROT_H */
