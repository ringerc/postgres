/*-------------------------------------------------------------------------
 *
 * libpq_headers.c
 *	  Tiny libpq client driver for the libpq_headers TAP test.
 *
 * Run with one of several modes; each mode exercises a specific aspect
 * of the per-message protocol-headers API.  The driver always emits a
 * single short status line on stdout; the TAP harness asserts on that
 * line and additionally inspects the server log to confirm that
 * headers actually reached (or did not reach) the server.  Server-side
 * verification relies on the test_protocol_headers loadable module,
 * which logs every set/clear event from its registered handlers.
 *
 * Most modes require the server to have affirmatively negotiated
 * _pq_.headers --- this is checked at the top.  The not_negotiated
 * mode is the exception: it expects PQheadersAvailable() to be 0 and
 * verifies that PQattachHeader() fails cleanly in that case.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/modules/libpq_headers/libpq_headers.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "libpq-fe.h"

/*
 * Use the test_protocol_headers handler's "test_tx." prefix.  That
 * module is loaded via shared_preload_libraries in the TAP test and
 * registers transaction-scoped logging for any key under this prefix.
 */
#define TEST_KEY		"test_tx.alpha"

/*
 * No-op SELECT used purely to drive the protocol forward after the
 * client has queued (or chosen not to queue) headers.  The 'M' message,
 * if queued, gets flushed by pqsendQueryStart before this query's Q
 * message goes out, so the handler fires before the SELECT executes.
 */
#define SELECT_NOOP		"SELECT 1"


static void
die_connerr(PGconn *conn, const char *what)
{
	fprintf(stderr, "libpq_headers: %s: %s",
			what, conn ? PQerrorMessage(conn) : "(no conn)");
	if (conn)
		PQfinish(conn);
	exit(1);
}

/*
 * Run a one-row SELECT, discard the result.  Used to drive the
 * protocol so any queued 'M' gets sent; we don't care about the
 * row, only the side effect on the server.
 */
static void
run_noop(PGconn *conn)
{
	PGresult   *res;

	res = PQexec(conn, SELECT_NOOP);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "libpq_headers: SELECT failed: %s",
				PQerrorMessage(conn));
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
			"usage: %s <conninfo> <mode> [<value> ...]\n"
			"Modes:\n"
			"  available                  print '1' if PQheadersAvailable, '0' otherwise\n"
			"  attach <value>             attach test_tx.alpha=<value>, run SELECT, print 'ok'\n"
			"  none                       SELECT only (no attach), print 'ok'\n"
			"  clear <value>              attach, PQclearHeaders, run SELECT, print 'ok'\n"
			"  reuse <value>              attach, two SELECTs (queue resets between them), print 'ok'\n"
			"  null_key                   PQattachHeader(NULL key); print '0' if rejected\n"
			"  not_negotiated             expect PQheadersAvailable=0; verify PQattachHeader rejects\n",
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
		printf("%d\n", PQheadersAvailable(conn));
	}
	else if (strcmp(mode, "not_negotiated") == 0)
	{
		/*
		 * Used when the server has the feature turned off via the
		 * protocol_headers GUC.  PQheadersAvailable must be 0, and
		 * PQattachHeader must refuse to queue.
		 */
		if (PQheadersAvailable(conn))
		{
			fprintf(stderr, "libpq_headers: expected PQheadersAvailable=0 but got 1\n");
			PQfinish(conn);
			return 1;
		}
		if (PQattachHeader(conn, TEST_KEY, "ignored"))
		{
			fprintf(stderr, "libpq_headers: PQattachHeader unexpectedly succeeded\n");
			PQfinish(conn);
			return 1;
		}
		printf("rejected\n");
	}
	else
	{
		/*
		 * From here on we require the server to have negotiated the
		 * feature.  Any failure to do so is a test setup error.
		 */
		if (!PQheadersAvailable(conn))
			die_connerr(conn, "server did not negotiate _pq_.headers");

		if (strcmp(mode, "attach") == 0)
		{
			if (argc != 4)
				usage(argv[0]);
			if (!PQattachHeader(conn, TEST_KEY, argv[3]))
				die_connerr(conn, "PQattachHeader");
			run_noop(conn);
			printf("ok\n");
		}
		else if (strcmp(mode, "none") == 0)
		{
			run_noop(conn);
			printf("ok\n");
		}
		else if (strcmp(mode, "clear") == 0)
		{
			if (argc != 4)
				usage(argv[0]);
			if (!PQattachHeader(conn, TEST_KEY, argv[3]))
				die_connerr(conn, "PQattachHeader");
			PQclearHeaders(conn);
			run_noop(conn);
			printf("ok\n");
		}
		else if (strcmp(mode, "reuse") == 0)
		{
			if (argc != 4)
				usage(argv[0]);
			if (!PQattachHeader(conn, TEST_KEY, argv[3]))
				die_connerr(conn, "PQattachHeader");
			/* First SELECT: header attached -> handler fires on server. */
			run_noop(conn);
			/* Second SELECT: the queue was consumed by the first
			 * PQexec.  No M is sent and no second set log line should
			 * appear. */
			run_noop(conn);
			printf("ok\n");
		}
		else if (strcmp(mode, "null_key") == 0)
		{
			/* libpq must defend against NULL args rather than crashing. */
			if (PQattachHeader(conn, NULL, "value"))
			{
				fprintf(stderr, "libpq_headers: PQattachHeader(NULL key) unexpectedly succeeded\n");
				PQfinish(conn);
				return 1;
			}
			printf("0\n");
		}
		else
		{
			usage(argv[0]);
		}
	}

	PQfinish(conn);
	return 0;
}
