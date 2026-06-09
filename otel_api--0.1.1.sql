/* otel_api--0.1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION otel_api" to load this file. \quit

CREATE FUNCTION otel_current_traceparent()
RETURNS text
AS 'MODULE_PATHNAME', 'otel_current_traceparent'
LANGUAGE C STABLE PARALLEL SAFE;
