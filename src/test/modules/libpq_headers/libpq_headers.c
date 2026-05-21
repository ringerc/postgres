/*-------------------------------------------------------------------------
 *
 * libpq_headers.c
 *	  Tiny libpq client for the libpq_headers TAP test.
 *
 * Connects to the cluster the TAP harness has set up, verifies that
 * _pq_.headers was negotiated, attaches an OpenTelemetry traceparent
 * header via PQattachHeader, runs a query that inspects the resulting
 * server-side state (otel_current_traceparent()), and prints the
 * single-row result on stdout.  The TAP test then asserts on both the
 * stdout value and the server log.
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

static void
die(PGconn *conn, const char *what)
{
	fprintf(stderr, "libpq_headers: %s: %s",
			what, conn ? PQerrorMessage(conn) : "(no conn)");
	if (conn)
		PQfinish(conn);
	exit(1);
}

int
main(int argc, char **argv)
{
	const char *conninfo;
	const char *mode;
	const char *traceparent;
	PGconn	   *conn;
	PGresult   *res;

	if (argc != 4)
	{
		fprintf(stderr,
				"usage: %s <conninfo> <mode> <traceparent>\n"
				"   mode = 'attach'  : attach traceparent, then SELECT\n"
				"   mode = 'none'    : do not attach, just SELECT\n",
				argv[0]);
		return 2;
	}
	conninfo = argv[1];
	mode = argv[2];
	traceparent = argv[3];

	conn = PQconnectdb(conninfo);
	if (PQstatus(conn) != CONNECTION_OK)
		die(conn, "connection failed");

	/*
	 * The server must have affirmatively negotiated _pq_.headers; without
	 * that, PQattachHeader will refuse to queue and the test cannot
	 * proceed.
	 */
	if (!PQheadersAvailable(conn))
	{
		fprintf(stderr,
				"libpq_headers: server did not negotiate _pq_.headers\n");
		PQfinish(conn);
		return 1;
	}

	if (strcmp(mode, "attach") == 0)
	{
		if (!PQattachHeader(conn, "otel.traceparent", traceparent))
			die(conn, "PQattachHeader");
	}

	/*
	 * SELECT through the OTel contrib's introspection function.  When
	 * the header was attached, the result row carries the active
	 * traceparent; otherwise it is NULL.
	 */
	res = PQexec(conn, "SELECT coalesce(otel_current_traceparent(), '')");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "libpq_headers: SELECT failed: %s",
				PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		return 1;
	}
	if (PQntuples(res) != 1 || PQnfields(res) != 1)
	{
		fprintf(stderr, "libpq_headers: unexpected result shape\n");
		PQclear(res);
		PQfinish(conn);
		return 1;
	}
	printf("%s\n", PQgetvalue(res, 0, 0));

	PQclear(res);
	PQfinish(conn);
	return 0;
}
