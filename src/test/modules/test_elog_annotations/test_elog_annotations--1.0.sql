/* src/test/modules/test_elog_annotations/test_elog_annotations--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_elog_annotations" to load this file. \quit

CREATE FUNCTION pg_test_errannot_emit(
    elevel       text,
    msg          text,
    ann_keys     text[] DEFAULT '{}',
    ann_vals     text[] DEFAULT '{}',
    fmt_keys     text[] DEFAULT '{}',
    fmt_vals     text[] DEFAULT '{}')
RETURNS void
AS 'MODULE_PATHNAME', 'pg_test_errannot_emit'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION pg_test_errannot_rethrow(
    ann_keys text[],
    ann_vals text[])
RETURNS void
AS 'MODULE_PATHNAME', 'pg_test_errannot_rethrow'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION pg_test_errannot_throwdata(
    ann_keys text[],
    ann_vals text[])
RETURNS void
AS 'MODULE_PATHNAME', 'pg_test_errannot_throwdata'
LANGUAGE C VOLATILE STRICT;
