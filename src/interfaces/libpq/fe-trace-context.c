/*-------------------------------------------------------------------------
 *
 * fe-trace-context.c
 *	  client-side trace-context protocol message ('M' / TraceContext).
 *
 * Availability is determined solely by the negotiated protocol version
 * (conn->pversion >= 3.3).  No startup opt-in, no ParameterStatus
 * acknowledgement required.
 *
 * Application code sets or attaches trace context via PQsetTraceContext
 * (armed: re-emits once per pipeline) or PQattachTraceContext (one-shot:
 * emits once then clears).  The 'M' message is flushed at the start of
 * each PQsend* / PQexec* operation via pqFlushTraceContext.
 *
 * Trace context is advisory only and must not be used by the server side
 * in authorization decisions.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/interfaces/libpq/fe-trace-context.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <string.h>
#include <stdlib.h>

#include "libpq/protocol.h"
#include "libpq-fe.h"
#include "libpq-int.h"

/*
 * PQtraceContextAvailable --- 1 if the negotiated protocol supports
 * trace context (>= 3.3).
 */
int
PQtraceContextAvailable(const PGconn *conn)
{
	if (conn == NULL)
		return 0;
	return PG_PROTOCOL_MINOR(conn->pversion) >= 3 ? 1 : 0;
}

/*
 * PQsetTraceContext --- arm the connection: libpq emits a fresh 'M' at the
 * start of each subsequent pipeline until disarmed.
 *
 * Pass traceparent = NULL to disarm and free stored strings.
 * tracestate may be NULL (treated as "").
 * Returns 1 on success, 0 on failure (conn->errorMessage set).
 */
int
PQsetTraceContext(PGconn *conn, const char *traceparent,
				  const char *tracestate)
{
	if (conn == NULL)
		return 0;

	/* Disarm: free stored strings and clear flags. */
	if (traceparent == NULL)
	{
		pqReleaseTraceContext(conn);
		return 1;
	}

	if (!PQtraceContextAvailable(conn))
	{
		libpq_append_conn_error(conn,
								"server protocol version does not support trace context (need >= 3.3)");
		return 0;
	}

	{
		char	   *tp_copy = strdup(traceparent);
		char	   *ts_copy = (tracestate != NULL) ? strdup(tracestate) : strdup("");

		if (tp_copy == NULL || ts_copy == NULL)
		{
			free(tp_copy);
			free(ts_copy);
			libpq_append_conn_error(conn, "out of memory");
			return 0;
		}

		/* Replace existing. */
		free(conn->tcTraceparent);
		free(conn->tcTracestate);
		conn->tcTraceparent = tp_copy;
		conn->tcTracestate = ts_copy;
		conn->tcArmed = true;
		/* One-shot is cleared when arming (armed supersedes one-shot). */
		conn->tcPendingOneShot = false;
	}
	return 1;
}

/*
 * PQattachTraceContext --- one-shot: emit one 'M' before the next message;
 * covers that pipeline until its RFQ, then is not re-sent.
 *
 * tracestate may be NULL (treated as "").
 * Returns 1 on success, 0 on failure.
 */
int
PQattachTraceContext(PGconn *conn, const char *traceparent,
					 const char *tracestate)
{
	if (conn == NULL)
		return 0;

	if (traceparent == NULL)
	{
		libpq_append_conn_error(conn,
								"PQattachTraceContext: traceparent must not be NULL");
		return 0;
	}

	if (!PQtraceContextAvailable(conn))
	{
		libpq_append_conn_error(conn,
								"server protocol version does not support trace context (need >= 3.3)");
		return 0;
	}

	{
		char	   *tp_copy = strdup(traceparent);
		char	   *ts_copy = (tracestate != NULL) ? strdup(tracestate) : strdup("");

		if (tp_copy == NULL || ts_copy == NULL)
		{
			free(tp_copy);
			free(ts_copy);
			libpq_append_conn_error(conn, "out of memory");
			return 0;
		}

		free(conn->tcTraceparent);
		free(conn->tcTracestate);
		conn->tcTraceparent = tp_copy;
		conn->tcTracestate = ts_copy;
		conn->tcPendingOneShot = true;
		/* Don't clear tcArmed - armed mode stays armed. */
	}
	return 1;
}

/*
 * pqFlushTraceContext --- emit one 'M' message if armed or a one-shot
 * is pending.  Called at the start of each PQsend* / PQexec* operation.
 *
 * Returns 0 on success (or nothing to send), EOF on send failure.
 */
int
pqFlushTraceContext(PGconn *conn)
{
	if (conn == NULL)
		return 0;

	if (!conn->tcArmed && !conn->tcPendingOneShot)
		return 0;

	if (conn->tcTraceparent == NULL)
	{
		/* Shouldn't happen, but be defensive. */
		conn->tcArmed = false;
		conn->tcPendingOneShot = false;
		return 0;
	}

	if (pqPutMsgStart(PqMsg_TraceContext, conn) < 0)
		goto fail;
	if (pqPuts(conn->tcTraceparent, conn) < 0)
		goto fail;
	if (pqPuts(conn->tcTracestate ? conn->tcTracestate : "", conn) < 0)
		goto fail;
	if (pqPutMsgEnd(conn) < 0)
		goto fail;

	/* Clear one-shot after emitting; armed stays armed. */
	if (conn->tcPendingOneShot && !conn->tcArmed)
		pqReleaseTraceContext(conn);
	else
		conn->tcPendingOneShot = false;

	return 0;

fail:
	/* On send failure, drop the pending state to avoid stale re-send. */
	pqReleaseTraceContext(conn);
	return EOF;
}

/*
 * pqReleaseTraceContext --- free trace-context strings and clear flags.
 */
void
pqReleaseTraceContext(PGconn *conn)
{
	free(conn->tcTraceparent);
	free(conn->tcTracestate);
	conn->tcTraceparent = NULL;
	conn->tcTracestate = NULL;
	conn->tcArmed = false;
	conn->tcPendingOneShot = false;
}
