/* otel_api--0.2.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION otel_api" to load this file. \quit

CREATE FUNCTION otel_current_traceparent()
RETURNS text
AS 'MODULE_PATHNAME', 'otel_current_traceparent'
LANGUAGE C STABLE PARALLEL SAFE;

-- Per-backend counts of spans otel_api dropped, refused or repaired.
CREATE FUNCTION otel_api_counters(OUT name text, OUT value bigint)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'otel_api_counters'
LANGUAGE C VOLATILE PARALLEL RESTRICTED;
