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
			"  none                 SELECT only (no context), print 'ok'\n",
			argv0);
	exit(2);
}

int
main(int argc, char **argv)
{
	const char *conninfo;
	const char *mode;
	PGconn	   *conn;

	if (argc < 3)
		usage(argv[0]);
	conninfo = argv[1];
	mode = argv[2];

	conn = PQconnectdb(conninfo);
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
	else
	{
		usage(argv[0]);
	}

	PQfinish(conn);
	return 0;
}
