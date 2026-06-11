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
 *
 * Threading: the same rule as every other PGconn-mutating libpq call
 * applies.  A PGconn is owned by exactly one thread; calling this
 * function from a different thread while the owner is also using
 * `conn` is undefined behaviour.  This includes the read-only
 * PQheadersAvailable() / pqReleaseQueuedHeaders() helpers.
 *
 * Embedded NULs: the v3 protocol's RequestHeaders message encodes
 * each (key, value) entry as two NUL-terminated strings.  This API
 * takes C strings, so anything past the first '\0' in `key` or
 * `value` is *silently dropped on the floor* --- the caller's
 * intent for the dropped bytes is unrepresentable on the wire.
 * If you have keys or values with embedded NUL bytes (e.g. binary
 * data), this API can't carry them; the protocol layer can't
 * either.  Notable failure mode: a zero-initialised, not-yet-
 * populated buffer (`char k[N] = {0};`) reads as an empty string;
 * that's why we reject empty keys below.
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

	/*
	 * Reject empty key.  Empty key cannot match any handler-registered
	 * prefix on the server side, and it's almost always a caller bug:
	 * the typical shape is "I passed a zero-initialised buffer thinking
	 * strlen() would tell me something useful".  Empty value is fine
	 * --- protocol convention is "empty value means clear this key".
	 */
	if (key[0] == '\0')
	{
		libpq_append_conn_error(conn,
								"PQattachHeader: key must not be empty");
		return 0;
	}

	if (!conn->headersAvailable)
	{
		libpq_append_conn_error(conn,
								"server did not negotiate _pq_.headers; PQattachHeader is not available on this connection");
		return 0;
	}

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

	/*
	 * The wire-format entry count is an Int16.  Refuse to serialize a
	 * queue that would silently truncate; drop the queue and fail loudly
	 * so the caller's next protocol message isn't corrupted by a count
	 * that doesn't match what we'd actually emit.  In practice an
	 * application would have to call PQattachHeader >65535 times to hit
	 * this; treat it as application error.
	 */
	if (n > UINT16_MAX)
	{
		libpq_append_conn_error(conn,
								"too many queued protocol headers (%d > %u)",
								n, (unsigned) UINT16_MAX);
		pqReleaseQueuedHeaders(conn);
		return EOF;
	}

	if (pqPutMsgStart(PqMsg_RequestHeaders, conn) < 0)
		goto fail;
	if (pqPutInt(n, 2, conn) < 0)
		goto fail;
	for (int i = 0; i < n; i++)
	{
		if (pqPuts(conn->queuedHeaders[i].key, conn) < 0)
			goto fail;
		if (pqPuts(conn->queuedHeaders[i].value, conn) < 0)
			goto fail;
	}
	if (pqPutMsgEnd(conn) < 0)
		goto fail;

	pqReleaseQueuedHeaders(conn);
	return 0;

fail:
	/*
	 * On any send failure the partial message is lost.  Drop the queue
	 * too so that a subsequent operation does not re-send stale headers.
	 */
	pqReleaseQueuedHeaders(conn);
	return EOF;
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
