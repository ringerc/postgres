/*-------------------------------------------------------------------------
 *
 * pg_sdt_probe.h
 *	  Hook for bridging PostgreSQL SDT trace points to OpenTelemetry spans.
 *
 *	  This header is included by the generated probes.h (via pg_trace.h) and
 *	  therefore must not add any #include directives of its own.  The types
 *	  int64 and PGDLLIMPORT are provided by c.h / postgres.h, which is always
 *	  included before pg_trace.h in every backend translation unit.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/utils/pg_sdt_probe.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_SDT_PROBE_H
#define PG_SDT_PROBE_H

/* Tagged argument passed from a TRACE_POSTGRESQL_* probe to the hook. */
typedef struct PgSdtArg
{
	char		tag;			/* 'i' = int64, 's' = const char * */
	union
	{
		int64		i;
		const char *s;
	}			v;
} PgSdtArg;

/* Curated probe identifiers (subset of probes.d that map to OTel spans). */
typedef enum PgSdtProbeId
{
	PG_SDT_TRANSACTION_START,
	PG_SDT_TRANSACTION_COMMIT,
	PG_SDT_TRANSACTION_ABORT,
	PG_SDT_QUERY_START,
	PG_SDT_QUERY_DONE,
	PG_SDT_QUERY_PARSE_START,
	PG_SDT_QUERY_PARSE_DONE,
	PG_SDT_QUERY_REWRITE_START,
	PG_SDT_QUERY_REWRITE_DONE,
	PG_SDT_QUERY_PLAN_START,
	PG_SDT_QUERY_PLAN_DONE,
	PG_SDT_QUERY_EXECUTE_START,
	PG_SDT_QUERY_EXECUTE_DONE,
	PG_SDT_SORT_START,
	PG_SDT_SORT_DONE,
	PG_SDT_SMGR_MD_READ_START,
	PG_SDT_SMGR_MD_READ_DONE,
	PG_SDT_SMGR_MD_WRITE_START,
	PG_SDT_SMGR_MD_WRITE_DONE
} PgSdtProbeId;

/*
 * Optional hook, default NULL.  When set (by an extension), the curated
 * TRACE_POSTGRESQL_* macros call it in addition to the normal probe.
 */
extern PGDLLIMPORT void (*pg_sdt_probe_hook) (int probe_id,
											  const PgSdtArg *args, int nargs);

#endif							/* PG_SDT_PROBE_H */
