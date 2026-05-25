/* contrib/otel_demo_exporter/otel_demo_exporter--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION otel_demo_exporter" to load this file. \quit

-- No SQL surface; the module is a pure exporter activated via
-- shared_preload_libraries.  CREATE EXTENSION is provided only so the
-- module can be tracked alongside other extensions in pg_extension.
