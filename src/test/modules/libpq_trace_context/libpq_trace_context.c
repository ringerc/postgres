/*-------------------------------------------------------------------------
 *
 * libpq_trace_context.c
 *	  Tiny libpq client driver for the libpq_trace_context TAP test.
 *
 * Run with one of several modes; each mode exercises a specific aspect
 * of the trace-context API (PQsetTraceContext, PQattachTraceContext,
 * PQtraceContextAvailable).  The driver emits a short status line on
 * stdout; the TAP harness asserts on that line and additionally
 * inspects the server log to confirm that trace context actually
 * reached (or did not reach) the server.  Server-side verification
 * relies on the test_trace_context loadable module.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/modules/libpq_trace_context/libpq_trace_context.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "libpq-fe.h"

/*
 * A valid W3C traceparent to use in tests.
 */
#define TEST_TRACEPARENT \
	"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

#define TEST_TRACESTATE "vendor1=test"

/*
 * No-op SELECT used purely to drive the protocol forward.
 */
#define SELECT_NOOP		"SELECT 1"

/*
 * Server-side introspection query (provided by the test_trace_context
 * module).  Returns whether a trace context is currently active and the
 * active traceparent value, so a pipelined command can report whether it
 * observed the context that was in effect when the server processed it.
 */
#define SELECT_PROBE	"SELECT test_tc_is_active(), test_tc_traceparent()"

/* Number of commands queued in each pipeline test. */
#define PIPELINE_NCMDS	3


static void
die_connerr(PGconn *conn, const char *what)
{
	fprintf(stderr, "libpq_trace_context: %s: %s",
			what, conn ? PQerrorMessage(conn) : "(no conn)");
	if (conn)
		PQfinish(conn);
	exit(1);
}

static void
run_noop(PGconn *conn)
{
	PGresult   *res;

	res = PQexec(conn, SELECT_NOOP);
	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "libpq_trace_context: SELECT failed: %s",
				PQerrorMessage(conn));
		if (res != NULL)
			PQclear(res);
		PQfinish(conn);
		exit(1);
	}
	PQclear(res);
}

/*
 * Run PIPELINE_NCMDS probe queries inside a single pipeline, terminated by
 * PQpipelineSync, and confirm that every command observed the trace context.
 *
 * Each probe is "SELECT test_tc_is_active(), test_tc_traceparent()", so the
 * server reports, per command, whether a context was active when it ran and
 * what the active traceparent was.  This proves per-command delivery within
 * the pipeline, independent of how many 'M' messages libpq chose to emit.
 *
 * The caller must already have armed (PQsetTraceContext) or attached
 * (PQattachTraceContext) the context before calling this.
 */
static void
run_pipeline_probe(PGconn *conn)
{
	int			i;

	if (PQenterPipelineMode(conn) != 1)
		die_connerr(conn, "PQenterPipelineMode");

	/* Queue several extended-protocol commands into the pipeline. */
	for (i = 0; i < PIPELINE_NCMDS; i++)
	{
		if (PQsendQueryParams(conn, SELECT_PROBE,
							  0, NULL, NULL, NULL, NULL, 0) != 1)
			die_connerr(conn, "PQsendQueryParams (pipeline)");
	}

	/* Terminate the pipeline with a Sync, flushing everything to the server. */
	if (PQpipelineSync(conn) != 1)
		die_connerr(conn, "PQpipelineSync");

	/* Read back one result set per queued command and verify each. */
	for (i = 0; i < PIPELINE_NCMDS; i++)
	{
		PGresult   *res = PQgetResult(conn);
		char	   *active;
		char	   *tp;

		if (res == NULL)
		{
			fprintf(stderr,
					"libpq_trace_context: pipeline: unexpected NULL result at cmd %d\n",
					i);
			PQfinish(conn);
			exit(1);
		}
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(stderr,
					"libpq_trace_context: pipeline cmd %d failed: %s",
					i, PQerrorMessage(conn));
			PQclear(res);
			PQfinish(conn);
			exit(1);
		}

		active = PQgetisnull(res, 0, 0) ? NULL : PQgetvalue(res, 0, 0);
		tp = PQgetisnull(res, 0, 1) ? NULL : PQgetvalue(res, 0, 1);

		/* Each command must have seen the context active and the right value. */
		if (active == NULL || strcmp(active, "t") != 0)
		{
			fprintf(stderr,
					"libpq_trace_context: pipeline cmd %d did NOT observe an active context\n",
					i);
			PQclear(res);
			PQfinish(conn);
			exit(1);
		}
		if (tp == NULL || strcmp(tp, TEST_TRACEPARENT) != 0)
		{
			fprintf(stderr,
					"libpq_trace_context: pipeline cmd %d observed wrong traceparent: %s\n",
					i, tp ? tp : "(null)");
			PQclear(res);
			PQfinish(conn);
			exit(1);
		}
		PQclear(res);

		/* Each command is followed by its own result terminator (NULL). */
		res = PQgetResult(conn);
		if (res != NULL)
		{
			fprintf(stderr,
					"libpq_trace_context: pipeline cmd %d: expected result terminator\n",
					i);
			PQclear(res);
			PQfinish(conn);
			exit(1);
		}
	}

	/* The PQpipelineSync produces a PGRES_PIPELINE_SYNC result. */
	{
		PGresult   *res = PQgetResult(conn);

		if (res == NULL || PQresultStatus(res) != PGRES_PIPELINE_SYNC)
		{
			fprintf(stderr,
					"libpq_trace_context: pipeline: expected PGRES_PIPELINE_SYNC: %s",
					PQerrorMessage(conn));
			if (res != NULL)
				PQclear(res);
			PQfinish(conn);
			exit(1);
		}
		PQclear(res);
	}

	if (PQexitPipelineMode(conn) != 1)
		die_connerr(conn, "PQexitPipelineMode");
}

static void
usage(const char *argv0)
{
	fprintf(stderr,
			"usage: %s <conninfo> <mode>\n"
			"Modes:\n"
			"  available            print '1' if PQtraceContextAvailable, '0' otherwise\n"
			"  attach               PQattachTraceContext, run SELECT, print 'ok'\n"
			"  set_armed            PQsetTraceContext, two SELECTs (re-emits), print 'ok'\n"
			"  set_null             PQsetTraceContext(NULL) to disarm, print 'ok'\n"
			"  none                 SELECT only (no context), print 'ok'\n"
			"  pipeline_armed       PQsetTraceContext, pipeline of N cmds + Sync, print 'ok'\n"
			"  pipeline_oneshot     PQattachTraceContext, pipeline of N cmds + Sync, print 'ok'\n"
			"  pipeline_oneshot_2nd PQattachTraceContext, one armed pipeline then a clean one, print 'ok'\n",
			argv0);
	exit(2);
}

int
main(int argc, char **argv)
{
	const char *conninfo;
	const char *mode;
	PGconn	   *conn;
	char	   *conninfo_with_proto;

	if (argc < 3)
		usage(argv[0]);
	conninfo = argv[1];
	mode = argv[2];

	/*
	 * Explicitly request the latest protocol so that PQtraceContextAvailable
	 * returns 1 when connecting to a 3.3-capable server.  Without this, libpq
	 * defaults to negotiating protocol 3.0 for backward compatibility with
	 * older servers and pgbouncers.
	 */
	conninfo_with_proto = malloc(strlen(conninfo) + 32);
	if (conninfo_with_proto == NULL)
	{
		fprintf(stderr, "libpq_trace_context: out of memory\n");
		exit(1);
	}
	sprintf(conninfo_with_proto, "%s max_protocol_version=latest", conninfo);

	conn = PQconnectdb(conninfo_with_proto);
	free(conninfo_with_proto);
	if (PQstatus(conn) != CONNECTION_OK)
		die_connerr(conn, "connection failed");

	if (strcmp(mode, "available") == 0)
	{
		printf("%d\n", PQtraceContextAvailable(conn));
	}
	else if (strcmp(mode, "attach") == 0)
	{
		if (!PQtraceContextAvailable(conn))
			die_connerr(conn, "trace context not available on this connection");
		if (!PQattachTraceContext(conn, TEST_TRACEPARENT, TEST_TRACESTATE))
			die_connerr(conn, "PQattachTraceContext");
		run_noop(conn);
		printf("ok\n");
	}
	else if (strcmp(mode, "set_armed") == 0)
	{
		/*
		 * PQsetTraceContext arms the connection; libpq re-emits 'M' before
		 * each subsequent operation until disarmed.  Verify that two
		 * consecutive SELECTs both receive the trace context.
		 */
		if (!PQtraceContextAvailable(conn))
			die_connerr(conn, "trace context not available on this connection");
		if (!PQsetTraceContext(conn, TEST_TRACEPARENT, TEST_TRACESTATE))
			die_connerr(conn, "PQsetTraceContext");
		run_noop(conn);
		run_noop(conn);
		printf("ok\n");
	}
	else if (strcmp(mode, "set_null") == 0)
	{
		/*
		 * PQsetTraceContext with NULL traceparent disarms the connection.
		 * Verify that a subsequent SELECT does not carry any trace context.
		 */
		if (!PQtraceContextAvailable(conn))
			die_connerr(conn, "trace context not available on this connection");
		if (!PQsetTraceContext(conn, TEST_TRACEPARENT, NULL))
			die_connerr(conn, "PQsetTraceContext");
		/* Disarm */
		if (!PQsetTraceContext(conn, NULL, NULL))
			die_connerr(conn, "PQsetTraceContext(NULL)");
		run_noop(conn);
		printf("ok\n");
	}
	else if (strcmp(mode, "none") == 0)
	{
		run_noop(conn);
		printf("ok\n");
	}
	else if (strcmp(mode, "pipeline_armed") == 0)
	{
		/*
		 * Arm the connection, then run a pipeline of several commands plus a
		 * Sync.  While armed, the context must reach every command in the
		 * pipeline.  run_pipeline_probe asserts per-command observation
		 * server-side.
		 */
		if (!PQtraceContextAvailable(conn))
			die_connerr(conn, "trace context not available on this connection");
		if (!PQsetTraceContext(conn, TEST_TRACEPARENT, TEST_TRACESTATE))
			die_connerr(conn, "PQsetTraceContext");
		run_pipeline_probe(conn);
		printf("ok\n");
	}
	else if (strcmp(mode, "pipeline_oneshot") == 0)
	{
		/*
		 * Attach a one-shot context, then run a pipeline.  The single 'M' is
		 * emitted before the first command; the server keeps it active until
		 * the pipeline's RFQ, so every command in this one pipeline observes
		 * it (asserted per-command server-side).
		 */
		if (!PQtraceContextAvailable(conn))
			die_connerr(conn, "trace context not available on this connection");
		if (!PQattachTraceContext(conn, TEST_TRACEPARENT, TEST_TRACESTATE))
			die_connerr(conn, "PQattachTraceContext");
		run_pipeline_probe(conn);
		printf("ok\n");
	}
	else if (strcmp(mode, "pipeline_oneshot_2nd") == 0)
	{
		/*
		 * A one-shot covers exactly the first pipeline.  After that pipeline's
		 * Sync (RFQ), it must NOT be re-emitted: a second pipeline runs
		 * untagged.  We assert the second pipeline sees no active context.
		 */
		int			i;

		if (!PQtraceContextAvailable(conn))
			die_connerr(conn, "trace context not available on this connection");
		if (!PQattachTraceContext(conn, TEST_TRACEPARENT, TEST_TRACESTATE))
			die_connerr(conn, "PQattachTraceContext");

		/* First pipeline: covered by the one-shot. */
		run_pipeline_probe(conn);

		/* Second pipeline: one-shot is consumed; must be untagged. */
		if (PQenterPipelineMode(conn) != 1)
			die_connerr(conn, "PQenterPipelineMode (2nd)");
		for (i = 0; i < PIPELINE_NCMDS; i++)
		{
			if (PQsendQueryParams(conn, SELECT_PROBE,
								  0, NULL, NULL, NULL, NULL, 0) != 1)
				die_connerr(conn, "PQsendQueryParams (2nd pipeline)");
		}
		if (PQpipelineSync(conn) != 1)
			die_connerr(conn, "PQpipelineSync (2nd)");

		for (i = 0; i < PIPELINE_NCMDS; i++)
		{
			PGresult   *res = PQgetResult(conn);
			char	   *active;

			if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
			{
				fprintf(stderr,
						"libpq_trace_context: 2nd pipeline cmd %d failed: %s",
						i, PQerrorMessage(conn));
				if (res != NULL)
					PQclear(res);
				PQfinish(conn);
				exit(1);
			}
			active = PQgetisnull(res, 0, 0) ? NULL : PQgetvalue(res, 0, 0);
			/* The context from the first pipeline must have been cleared. */
			if (active == NULL || strcmp(active, "f") != 0)
			{
				fprintf(stderr,
						"libpq_trace_context: 2nd pipeline cmd %d unexpectedly saw an active context\n",
						i);
				PQclear(res);
				PQfinish(conn);
				exit(1);
			}
			PQclear(res);
			res = PQgetResult(conn);	/* result terminator */
			if (res != NULL)
			{
				PQclear(res);
				fprintf(stderr,
						"libpq_trace_context: 2nd pipeline cmd %d: expected terminator\n",
						i);
				PQfinish(conn);
				exit(1);
			}
		}
		{
			PGresult   *res = PQgetResult(conn);

			if (res == NULL || PQresultStatus(res) != PGRES_PIPELINE_SYNC)
			{
				fprintf(stderr,
						"libpq_trace_context: 2nd pipeline: expected PGRES_PIPELINE_SYNC\n");
				if (res != NULL)
					PQclear(res);
				PQfinish(conn);
				exit(1);
			}
			PQclear(res);
		}
		if (PQexitPipelineMode(conn) != 1)
			die_connerr(conn, "PQexitPipelineMode (2nd)");

		printf("ok\n");
	}
	else
	{
		usage(argv[0]);
	}

	PQfinish(conn);
	return 0;
}
