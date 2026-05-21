/*-------------------------------------------------------------------------
 *
 * fe-headers.c
 *	  client-side per-message protocol headers ('M' / RequestHeaders).
 *
 * The client opts in by sending _pq_.headers=1 in the StartupMessage
 * (handled in fe-protocol3.c).  The server affirmatively acknowledges
 * by emitting a "protocol_features" ParameterStatus containing the
 * negotiated feature names (parsed in fe-exec.c's
 * pqSaveParameterStatus).  Only that affirmative ack flips
 * conn->headersAvailable to true; the absence of
 * NegotiateProtocolVersion is not sufficient by itself, since an
 * intermediary may have silently stripped the opt-in.
 *
 * Application code queues headers via PQattachHeader(); the queue is
 * flushed as a single 'M' message immediately before the next
 * PQsend* / PQexec* operation, via pqFlushHeaders().
 *
 * Headers are advisory only and must not be used by the server side
 * in authorization decisions.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/interfaces/libpq/fe-headers.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <string.h>

#include "libpq/protocol.h"
#include "libpq-fe.h"
#include "libpq-int.h"

#define INITIAL_HEADER_CAPACITY 8

static bool ensure_capacity(PGconn *conn);


/*
 * PQattachHeader --- queue a (key, value) header for the next operation.
 *
 * Returns 1 on success, 0 on failure (conn->errorMessage set).
 */
int
PQattachHeader(PGconn *conn, const char *key, const char *value)
{
	char	   *key_copy;
	char	   *val_copy;

	if (conn == NULL)
		return 0;

	if (key == NULL || value == NULL)
	{
		libpq_append_conn_error(conn,
								"PQattachHeader: key and value must not be NULL");
		return 0;
	}

	if (!conn->headersAvailable)
	{
		libpq_append_conn_error(conn,
								"server did not negotiate _pq_.headers; PQattachHeader is not available on this connection");
		return 0;
	}

	/*
	 * Disallow embedded NUL bytes --- the protocol's NUL-terminated string
	 * encoding cannot represent them and the server would misparse the
	 * message.  strlen() finds the first NUL; if it equals the underlying
	 * argument length the caller's string is clean.  We can't directly know
	 * the underlying length, but the only way a NUL inside the visible
	 * string can occur is via deliberate construction; in normal usage
	 * strlen of a C string is the canonical length, so checking that the
	 * key and value contain no further data is sufficient.  We restrict
	 * key and value to the length strlen reports.
	 */
	/* (We accept whatever strlen() reports; there's no way for a caller's
	 * normal C string to embed a NUL that strlen would miss.) */

	if (!ensure_capacity(conn))
	{
		libpq_append_conn_error(conn, "out of memory");
		return 0;
	}

	key_copy = strdup(key);
	val_copy = strdup(value);
	if (key_copy == NULL || val_copy == NULL)
	{
		free(key_copy);
		free(val_copy);
		libpq_append_conn_error(conn, "out of memory");
		return 0;
	}

	conn->queuedHeaders[conn->nQueuedHeaders].key = key_copy;
	conn->queuedHeaders[conn->nQueuedHeaders].value = val_copy;
	conn->nQueuedHeaders++;
	return 1;
}

/*
 * PQclearHeaders --- discard queued headers without sending.
 */
void
PQclearHeaders(PGconn *conn)
{
	if (conn == NULL)
		return;
	pqReleaseQueuedHeaders(conn);
}

/*
 * PQheadersAvailable --- 1 if the server confirmed _pq_.headers.
 */
int
PQheadersAvailable(const PGconn *conn)
{
	if (conn == NULL)
		return 0;
	return conn->headersAvailable ? 1 : 0;
}


/*
 * pqFlushHeaders --- emit any queued headers as a single 'M' message
 * and clear the queue.
 *
 * Returns 0 on success, EOF on send failure.  A queue of zero headers
 * is a no-op (returns 0).  Caller is expected to have already verified
 * conn->headersAvailable for any caller-visible operation; we also
 * defensively check it here.
 */
int
pqFlushHeaders(PGconn *conn)
{
	int			n = conn->nQueuedHeaders;

	if (n == 0)
		return 0;

	if (!conn->headersAvailable)
	{
		/*
		 * Defensive: should not happen because PQattachHeader refuses to
		 * queue without negotiation.  Drop the queue silently if it does.
		 */
		pqReleaseQueuedHeaders(conn);
		return 0;
	}

	if (pqPutMsgStart(PqMsg_RequestHeaders, conn) < 0)
		return EOF;
	if (pqPutInt(n, 2, conn) < 0)
		return EOF;
	for (int i = 0; i < n; i++)
	{
		if (pqPuts(conn->queuedHeaders[i].key, conn) < 0)
			return EOF;
		if (pqPuts(conn->queuedHeaders[i].value, conn) < 0)
			return EOF;
	}
	if (pqPutMsgEnd(conn) < 0)
		return EOF;

	pqReleaseQueuedHeaders(conn);
	return 0;
}

/*
 * pqReleaseQueuedHeaders --- free queued entries and zero the count.
 * Leaves the array allocation in place for reuse.
 */
void
pqReleaseQueuedHeaders(PGconn *conn)
{
	for (int i = 0; i < conn->nQueuedHeaders; i++)
	{
		free(conn->queuedHeaders[i].key);
		free(conn->queuedHeaders[i].value);
		conn->queuedHeaders[i].key = NULL;
		conn->queuedHeaders[i].value = NULL;
	}
	conn->nQueuedHeaders = 0;
}


/*
 * Grow the queuedHeaders array if necessary.  Returns true on success.
 */
static bool
ensure_capacity(PGconn *conn)
{
	int			needed = conn->nQueuedHeaders + 1;
	int			newcap;
	PQqueuedHeader *newarr;

	if (needed <= conn->queuedHeadersCapacity)
		return true;

	newcap = (conn->queuedHeadersCapacity == 0)
		? INITIAL_HEADER_CAPACITY
		: conn->queuedHeadersCapacity * 2;
	if (newcap < needed)
		newcap = needed;

	newarr = (PQqueuedHeader *) realloc(conn->queuedHeaders,
										sizeof(PQqueuedHeader) * newcap);
	if (newarr == NULL)
		return false;

	conn->queuedHeaders = newarr;
	conn->queuedHeadersCapacity = newcap;
	return true;
}
