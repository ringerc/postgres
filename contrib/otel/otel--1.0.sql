/* contrib/otel/otel--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION otel" to load this file. \quit

CREATE FUNCTION otel_current_traceparent()
RETURNS text
AS 'MODULE_PATHNAME', 'otel_current_traceparent'
LANGUAGE C STABLE PARALLEL SAFE;
