/*-------------------------------------------------------------------------
 *
 * fe-auth-channel.c
 *	  libpq client-side surface for the protocol-level role-management
 *	  channel (irrevocable-privilege-drop Phase 4 / design §16).
 *
 *	  Five public entry points:
 *	    PQauthChannelEnabled  — inspector
 *	    PQauthSetRole         — send V (AuthSetRole)
 *	    PQauthSetSession      — send e (AuthSetSession)
 *	    PQauthResetRole       — send U (AuthResetRole)
 *	    PQauthResetSession    — send b (AuthResetSession)
 *
 *	  The send functions are synchronous and block until they receive
 *	  the matching Y (AuthLockResponse) or an ErrorResponse.  They
 *	  return a PGresult that the caller must PQclear().  The cookie
 *	  returned by a WITH COOKIE set lives in the result's first
 *	  binary field (PQgetvalue/PQgetlength), but only for that result.
 *
 * Copyright (c) 2024-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/interfaces/libpq/fe-auth-channel.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "libpq-fe.h"
#include "libpq-int.h"
#include "libpq/protocol.h"
#include "fe-auth.h"
#include "port/pg_bswap.h"

int
PQauthChannelEnabled(const PGconn *conn)
{
	if (!conn)
		return 0;
	return conn->auth_channel_enabled ? 1 : 0;
}

/*
 * Drain notice and parameter-status messages and return when we hit
 * either an AuthLockResponse, ErrorResponse, or ReadyForQuery.  The
 * caller is responsible for parsing the specific message.  Returns
 * the message tag found (one of 'Y', 'E', 'Z'), or 0 on failure.
 */
static char
auth_channel_recv(PGconn *conn, PGresult **err_out, char **payload_out,
				  int *payload_len_out)
{
	char		id;
	int			msgLength;
	int			avail;
	bool		needInput = false;

	*err_out = NULL;
	*payload_out = NULL;
	*payload_len_out = 0;

	for (;;)
	{
		if (needInput)
		{
			if (pqWait(true, false, (PGconn *) conn) ||
				pqReadData((PGconn *) conn) < 0)
				return 0;
		}

		conn->inCursor = conn->inStart;
		if (pqGetc(&id, (PGconn *) conn))
		{
			needInput = true;
			continue;
		}
		if (pqGetInt(&msgLength, 4, (PGconn *) conn))
		{
			needInput = true;
			continue;
		}

		if (msgLength < 4)
			return 0;

		msgLength -= 4;
		avail = conn->inEnd - conn->inCursor;
		if (avail < msgLength)
		{
			needInput = true;
			continue;
		}

		/* Have a complete message at conn->inCursor for msgLength bytes. */
		if (id == PqMsg_AuthLockResponse || id == PqMsg_ErrorResponse ||
			id == PqMsg_ReadyForQuery)
		{
			char	   *body = malloc(msgLength + 1);

			if (!body)
				return 0;
			memcpy(body, conn->inBuffer + conn->inCursor, msgLength);
			body[msgLength] = '\0';
			conn->inCursor += msgLength;
			conn->inStart = conn->inCursor;
			*payload_out = body;
			*payload_len_out = msgLength;
			return id;
		}

		/*
		 * Any other message type (NoticeResponse, ParameterStatus, etc.):
		 * skip past it so the next message can be parsed.  We deliberately
		 * don't dispatch to pqParseInput3 here because it has its own state
		 * machine expectations; for the auth-channel send/recv path we only
		 * care about Y/E/Z and quietly consume anything else.
		 */
		conn->inCursor += msgLength;
		conn->inStart = conn->inCursor;
	}
}

/*
 * Build a synthetic PGresult representing an auth-channel outcome.
 * On AuthLockResponse: status 0/1 maps to PGRES_COMMAND_OK; other
 * statuses map to PGRES_FATAL_ERROR with the AuthLockResponse's
 * message text as the diagnostic.
 *
 * For OK_COOKIE responses, the cookie bytes are returned via the
 * result's binary field zero.  Use PQgetvalue(res, 0, 0) +
 * PQgetlength(res, 0, 0) to extract.
 */
static PGresult *
auth_channel_make_result(PGconn *conn, char id, const char *payload, int payload_len)
{
	if (id == PqMsg_ErrorResponse)
	{
		PGresult   *res = PQmakeEmptyPGresult(conn, PGRES_FATAL_ERROR);
		const char *p;
		const char *end;

		if (!res)
			return NULL;
		/* The standard ErrorResponse parser uses inCursor; here we
		 * decode minimally for the message field. */
		p = payload;
		end = payload + payload_len;

		while (p < end && *p)
		{
			char		field = *p++;
			const char *value = p;

			while (p < end && *p)
				p++;
			if (p >= end)
				break;
			if (field == PG_DIAG_MESSAGE_PRIMARY)
			{
				pqSaveErrorResult(conn);
				appendPQExpBufferStr(&conn->errorMessage, value);
				appendPQExpBufferChar(&conn->errorMessage, '\n');
				PQclear(res);
				res = PQmakeEmptyPGresult(conn, PGRES_FATAL_ERROR);
			}
			p++;
		}
		return res;
	}

	/* AuthLockResponse */
	{
	int			status;
	uint32		cookie_len_raw;
	int			cookie_len;
	const char *cookie_ptr;
	const char *message_ptr;

	if (payload_len < 5)
		return PQmakeEmptyPGresult(conn, PGRES_FATAL_ERROR);

	status = (unsigned char) payload[0];
	memcpy(&cookie_len_raw, payload + 1, 4);
	cookie_len = (int) pg_ntoh32(cookie_len_raw);
	cookie_ptr = payload + 5;
	message_ptr = cookie_ptr + cookie_len;

	if (status == 0 || status == 1)
	{
		PGresult   *res = PQmakeEmptyPGresult(conn, PGRES_COMMAND_OK);

		if (res && status == 1 && cookie_len > 0)
		{
			/* Make a one-field binary tuple holding the cookie bytes. */
			static const char *colname = "cookie";
			res->numAttributes = 1;
			res->binary = 1;
			res->attDescs = (PGresAttDesc *)
				pqResultAlloc(res, sizeof(PGresAttDesc), true);
			if (!res->attDescs)
			{
				PQclear(res);
				return NULL;
			}
			res->attDescs[0].name = pqResultStrdup(res, colname);
			res->attDescs[0].tableid = 0;
			res->attDescs[0].columnid = 0;
			res->attDescs[0].format = 1;	/* binary */
			res->attDescs[0].typid = 17;	/* BYTEAOID */
			res->attDescs[0].typlen = -1;
			res->attDescs[0].atttypmod = -1;

			res->tuples = (PGresAttValue **)
				pqResultAlloc(res, sizeof(PGresAttValue *), true);
			if (!res->tuples)
			{
				PQclear(res);
				return NULL;
			}
			res->tuples[0] = (PGresAttValue *)
				pqResultAlloc(res, sizeof(PGresAttValue), true);
			if (!res->tuples[0])
			{
				PQclear(res);
				return NULL;
			}
			res->tuples[0][0].len = cookie_len;
			res->tuples[0][0].value = pqResultAlloc(res, cookie_len, true);
			if (!res->tuples[0][0].value)
			{
				PQclear(res);
				return NULL;
			}
			memcpy(res->tuples[0][0].value, cookie_ptr, cookie_len);
			res->ntups = 1;
		}
		return res;
	}

	/* Non-OK status — synthesise an error. */
	pqSaveErrorResult(conn);
	appendPQExpBuffer(&conn->errorMessage,
					  "auth-channel operation failed (status=%d): %s\n",
					  status, message_ptr);
	return PQmakeEmptyPGresult(conn, PGRES_FATAL_ERROR);
	}							/* close AuthLockResponse block */
}

/*
 * Common sender for V/e/U/b messages.  msgtype is the tag byte to
 * send; body/body_len is the variable-length payload (after the
 * 4-byte length field).
 */
static PGresult *
auth_channel_send_and_recv(PGconn *conn, char msgtype,
						   const char *body, int body_len)
{
	char		id;
	char	   *payload = NULL;
	int			payload_len = 0;
	PGresult   *err_res;
	PGresult   *res;

	if (!conn)
		return NULL;

	if (conn->sock == PGINVALID_SOCKET || conn->asyncStatus != PGASYNC_IDLE ||
		conn->pipelineStatus != PQ_PIPELINE_OFF)
	{
		libpq_append_conn_error(conn, "connection not idle");
		return PQmakeEmptyPGresult(conn, PGRES_FATAL_ERROR);
	}

	if (!conn->auth_channel_enabled)
	{
		libpq_append_conn_error(conn,
								"auth_channel not negotiated (connect with auth_channel=1)");
		return PQmakeEmptyPGresult(conn, PGRES_FATAL_ERROR);
	}

	if (pqPutMsgStart(msgtype, conn) < 0 ||
		pqPutnchar(body, body_len, conn) < 0 ||
		pqPutMsgEnd(conn) < 0 ||
		pqFlush(conn))
	{
		return PQmakeEmptyPGresult(conn, PGRES_FATAL_ERROR);
	}

	id = auth_channel_recv(conn, &err_res, &payload, &payload_len);
	if (id == 0)
	{
		if (payload)
			free(payload);
		return PQmakeEmptyPGresult(conn, PGRES_FATAL_ERROR);
	}

	res = auth_channel_make_result(conn, id, payload, payload_len);
	if (payload)
		free(payload);

	/* Drain the trailing ReadyForQuery. */
	while (1)
	{
		char		drain_id;
		char	   *drain_payload = NULL;
		int			drain_len = 0;
		PGresult   *drain_err = NULL;

		drain_id = auth_channel_recv(conn, &drain_err, &drain_payload, &drain_len);
		if (drain_payload)
			free(drain_payload);
		if (drain_id == 0)
			break;
		if (drain_id == PqMsg_ReadyForQuery)
		{
			conn->asyncStatus = PGASYNC_IDLE;
			conn->xactStatus = PQTRANS_IDLE;
			break;
		}
		/* Any other tag is unexpected; loop and hope for ReadyForQuery. */
	}

	return res;
}

/*
 * Build the body bytes for V/e (AuthSetRole / AuthSetSession):
 *   Int8 kind, Cstring role_name, Int32 flags=0
 */
static char *
build_set_body(const char *role_name, PGauthLockKind kind, int *len_out)
{
	int			rlen = strlen(role_name);
	int			total = 1 + rlen + 1 + 4;
	char	   *body = malloc(total);

	if (!body)
		return NULL;
	body[0] = (char) kind;
	memcpy(body + 1, role_name, rlen);
	body[1 + rlen] = '\0';
	memset(body + 1 + rlen + 1, 0, 4);	/* flags = 0 */
	*len_out = total;
	return body;
}

/*
 * Build the body bytes for U/b (AuthResetRole / AuthResetSession):
 *   Int8 has_cookie, [Int32 len + ByteN cookie]
 */
static char *
build_reset_body(const void *cookie, size_t cookie_len, int *len_out)
{
	char	   *body;

	if (cookie == NULL || cookie_len == 0)
	{
		body = malloc(1);
		if (!body)
			return NULL;
		body[0] = 0;
		*len_out = 1;
		return body;
	}

	if (cookie_len > 1024)
		return NULL;

	body = malloc(1 + 4 + cookie_len);
	if (!body)
		return NULL;
	body[0] = 1;
	{
		uint32		len_be = pg_hton32(cookie_len);

		memcpy(body + 1, &len_be, 4);
	}
	memcpy(body + 5, cookie, cookie_len);
	*len_out = 5 + cookie_len;
	return body;
}

PGresult *
PQauthSetRole(PGconn *conn, const char *role_name, PGauthLockKind kind)
{
	int			body_len;
	char	   *body;
	PGresult   *res;

	if (!role_name)
	{
		libpq_append_conn_error(conn, "role_name must not be NULL");
		return PQmakeEmptyPGresult(conn, PGRES_FATAL_ERROR);
	}
	body = build_set_body(role_name, kind, &body_len);
	if (!body)
		return PQmakeEmptyPGresult(conn, PGRES_FATAL_ERROR);
	res = auth_channel_send_and_recv(conn, PqMsg_AuthSetRole, body, body_len);
	free(body);
	return res;
}

PGresult *
PQauthSetSession(PGconn *conn, const char *role_name, PGauthLockKind kind)
{
	int			body_len;
	char	   *body;
	PGresult   *res;

	if (!role_name)
	{
		libpq_append_conn_error(conn, "role_name must not be NULL");
		return PQmakeEmptyPGresult(conn, PGRES_FATAL_ERROR);
	}
	body = build_set_body(role_name, kind, &body_len);
	if (!body)
		return PQmakeEmptyPGresult(conn, PGRES_FATAL_ERROR);
	res = auth_channel_send_and_recv(conn, PqMsg_AuthSetSession, body, body_len);
	free(body);
	return res;
}

PGresult *
PQauthResetRole(PGconn *conn, const void *cookie, size_t cookie_len)
{
	int			body_len;
	char	   *body = build_reset_body(cookie, cookie_len, &body_len);
	PGresult   *res;

	if (!body)
		return PQmakeEmptyPGresult(conn, PGRES_FATAL_ERROR);
	res = auth_channel_send_and_recv(conn, PqMsg_AuthResetRole, body, body_len);
	free(body);
	return res;
}

PGresult *
PQauthResetSession(PGconn *conn, const void *cookie, size_t cookie_len)
{
	int			body_len;
	char	   *body = build_reset_body(cookie, cookie_len, &body_len);
	PGresult   *res;

	if (!body)
		return PQmakeEmptyPGresult(conn, PGRES_FATAL_ERROR);
	res = auth_channel_send_and_recv(conn, PqMsg_AuthResetSession, body, body_len);
	free(body);
	return res;
}
