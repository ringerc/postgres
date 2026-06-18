/* otel_postgres_tracing--0.1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION otel_postgres_tracing" to load this file. \quit

-- This extension has no SQL surface.  The query-tracing hooks are
-- installed at _PG_init via shared_preload_libraries; this CREATE
-- EXTENSION exists solely so the module's catalogue entry can be
-- tracked alongside the loadable library.
