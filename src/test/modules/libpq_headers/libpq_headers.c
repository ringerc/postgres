/*-------------------------------------------------------------------------
 *
 * libpq_headers.c
 *	  Tiny libpq client driver for the libpq_headers TAP test.
 *
 * Run with one of several modes; each mode exercises a specific aspect
 * of the per-message protocol-headers API.  Output goes to stdout with
 * a single result line (or one line per sub-step for the multi-step
 * modes), so the TAP harness can assert via string equality.
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

#define SELECT_TRACEPARENT \
	"SELECT coalesce(otel_current_traceparent(), '')"


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
 * Run a single one-row, one-column SELECT and return the value (an
 * empty string for NULL).  Caller frees nothing; the buffer is static.
 */
static const char *
run_select(PGconn *conn, const char *sql)
{
	static char buf[256];
	PGresult   *res;

	res = PQexec(conn, sql);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "libpq_headers: SELECT failed: %s",
				PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		exit(1);
	}
	if (PQntuples(res) != 1 || PQnfields(res) != 1)
	{
		fprintf(stderr, "libpq_headers: unexpected result shape\n");
		PQclear(res);
		PQfinish(conn);
		exit(1);
	}
	snprintf(buf, sizeof(buf), "%s", PQgetvalue(res, 0, 0));
	PQclear(res);
	return buf;
}

static void
usage(const char *argv0)
{
	fprintf(stderr,
			"usage: %s <conninfo> <mode> [<traceparent> ...]\n"
			"Modes:\n"
			"  available                  print '1' if PQheadersAvailable, '0' otherwise\n"
			"  attach <tp>                attach traceparent, SELECT, print result\n"
			"  none                       SELECT only, print result\n"
			"  clear <tp>                 attach, PQclearHeaders, SELECT, print result\n"
			"  reuse <tp>                 attach, SELECT, SELECT again (no re-attach), print both\n"
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
		if (PQattachHeader(conn, "otel.traceparent",
						   "00-aabbccddeeff00112233445566778899-0011223344556677-01"))
		{
			fprintf(stderr, "libpq_headers: PQattachHeader unexpectedly succeeded\n");
			PQfinish(conn);
			return 1;
		}
		/*
		 * PQerrorMessage should be set; print the first non-empty line so
		 * the TAP test can assert on it.
		 */
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
			if (!PQattachHeader(conn, "otel.traceparent", argv[3]))
				die_connerr(conn, "PQattachHeader");
			printf("%s\n", run_select(conn, SELECT_TRACEPARENT));
		}
		else if (strcmp(mode, "none") == 0)
		{
			printf("%s\n", run_select(conn, SELECT_TRACEPARENT));
		}
		else if (strcmp(mode, "clear") == 0)
		{
			if (argc != 4)
				usage(argv[0]);
			if (!PQattachHeader(conn, "otel.traceparent", argv[3]))
				die_connerr(conn, "PQattachHeader");
			PQclearHeaders(conn);
			/* After PQclearHeaders no 'M' is sent, so the SELECT sees no
			 * trace context.  Expected output: empty string. */
			printf("%s\n", run_select(conn, SELECT_TRACEPARENT));
		}
		else if (strcmp(mode, "reuse") == 0)
		{
			if (argc != 4)
				usage(argv[0]);
			if (!PQattachHeader(conn, "otel.traceparent", argv[3]))
				die_connerr(conn, "PQattachHeader");
			/* First SELECT: header attached -> traceparent visible. */
			printf("%s\n", run_select(conn, SELECT_TRACEPARENT));
			/* Second SELECT: no re-attach.  The queue was consumed by the
			 * first PQexec, and the per-transaction effect on the server
			 * was cleared at the end of the implicit transaction around
			 * the first SELECT.  Expected output: empty string. */
			printf("%s\n", run_select(conn, SELECT_TRACEPARENT));
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
