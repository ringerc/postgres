/*-------------------------------------------------------------------------
 *
 * otel_semconv.h
 *	  Attribute names: OpenTelemetry semantic conventions, and the pg.*
 *	  names this project defines.
 *
 * Use these instead of spelling names out, so producers agree with each
 * other and with the OTel conventions.  OTEL_SC_* are OTel semantic
 * convention names (stable database, server/client, network, exception,
 * code and service conventions).  OTEL_PG_* are this project's own.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * otel_api/otel_semconv.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef OTEL_SEMCONV_H
#define OTEL_SEMCONV_H

/* Database */
#define OTEL_SC_DB_SYSTEM_NAME			"db.system.name"
#define OTEL_SC_DB_SYSTEM_POSTGRESQL	"postgresql"	/* value */
#define OTEL_SC_DB_NAMESPACE			"db.namespace"
#define OTEL_SC_DB_QUERY_TEXT			"db.query.text"
#define OTEL_SC_DB_QUERY_SUMMARY		"db.query.summary"
#define OTEL_SC_DB_OPERATION_NAME		"db.operation.name"
#define OTEL_SC_DB_COLLECTION_NAME		"db.collection.name"
#define OTEL_SC_DB_STORED_PROCEDURE_NAME "db.stored_procedure.name"
#define OTEL_SC_DB_RESPONSE_STATUS_CODE	"db.response.status_code"	/* SQLSTATE */
#define OTEL_SC_DB_RESPONSE_RETURNED_ROWS "db.response.returned_rows"

/* Server, client, network */
#define OTEL_SC_SERVER_ADDRESS			"server.address"
#define OTEL_SC_SERVER_PORT				"server.port"
#define OTEL_SC_CLIENT_ADDRESS			"client.address"
#define OTEL_SC_CLIENT_PORT				"client.port"
#define OTEL_SC_NETWORK_PEER_ADDRESS	"network.peer.address"
#define OTEL_SC_NETWORK_PEER_PORT		"network.peer.port"

/* Exceptions (the "exception" span event) */
#define OTEL_SC_EXCEPTION_EVENT			"exception"	/* event name */
#define OTEL_SC_EXCEPTION_TYPE			"exception.type"
#define OTEL_SC_EXCEPTION_MESSAGE		"exception.message"

/* Source code location */
#define OTEL_SC_CODE_FUNCTION_NAME		"code.function.name"
#define OTEL_SC_CODE_FILE_PATH			"code.file.path"
#define OTEL_SC_CODE_LINE_NUMBER		"code.line.number"

/* Resource */
#define OTEL_SC_SERVICE_NAME			"service.name"
#define OTEL_SC_SERVICE_VERSION			"service.version"
#define OTEL_SC_SERVICE_INSTANCE_ID		"service.instance.id"
#define OTEL_SC_HOST_NAME				"host.name"
#define OTEL_SC_PROCESS_PID				"process.pid"

/*
 * pg.* attributes defined by this project.
 */

/* Session */
#define OTEL_PG_SESSION_USER			"pg.session.user"
#define OTEL_PG_APPLICATION_NAME		"pg.application_name"
#define OTEL_PG_BACKEND_TYPE			"pg.backend_type"
#define OTEL_PG_DATABASE_OID			"pg.database.oid"

/* Query */
#define OTEL_PG_QUERY_ID				"pg.query_id"
#define OTEL_PG_EXPLAIN_PLAN			"pg.explain_plan"

/* Errors: attributes of the "exception" event beyond the OTel ones */
#define OTEL_PG_ERROR_ELEVEL			"pg.error.elevel"
#define OTEL_PG_ERROR_DETAIL			"pg.error.detail"
#define OTEL_PG_ERROR_HINT				"pg.error.hint"
#define OTEL_PG_ERROR_CONTEXT			"pg.error.context"

/* Parallel query */
#define OTEL_PG_PARALLEL_WORKERS_PLANNED	"pg.parallel.workers_planned"
#define OTEL_PG_PARALLEL_WORKERS_LAUNCHED	"pg.parallel.workers_launched"

/* Where a span came from, e.g. "sdt_probe" */
#define OTEL_PG_SPAN_SOURCE				"pg.otel.span_source"

/*
 * Keys for trace context attached to server log messages with errannot().
 * Extensions that annotate log messages with trace context should use
 * the same keys, so log processors can correlate them.
 */
#define OTEL_ERRANNOT_KEY_TRACE_ID		"trace_id"
#define OTEL_ERRANNOT_KEY_SPAN_ID		"span_id"
#define OTEL_ERRANNOT_KEY_TRACE_FLAGS	"trace_flags"

#endif							/* OTEL_SEMCONV_H */
