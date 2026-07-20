/* tests/otel_test_exporter/test_otel_exporter--0.1.1.sql */

\echo Use "CREATE EXTENSION test_otel_exporter" to load this file. \quit

CREATE FUNCTION test_otel_span_count()
RETURNS integer
AS 'MODULE_PATHNAME', 'test_otel_span_count'
LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE FUNCTION test_otel_pop_span()
RETURNS text
AS 'MODULE_PATHNAME', 'test_otel_pop_span'
LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE FUNCTION test_otel_pop_span_by_name(name text)
RETURNS text
AS 'MODULE_PATHNAME', 'test_otel_pop_span_by_name'
LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE FUNCTION test_otel_count_spans_by_name(name text)
RETURNS integer
AS 'MODULE_PATHNAME', 'test_otel_count_spans_by_name'
LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE FUNCTION test_otel_clear()
RETURNS void
AS 'MODULE_PATHNAME', 'test_otel_clear'
LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE FUNCTION test_otel_set_policy(policy text)
RETURNS void
AS 'MODULE_PATHNAME', 'test_otel_set_policy'
LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE FUNCTION test_otel_producer_roundtrip(name text)
RETURNS text
AS 'MODULE_PATHNAME', 'test_otel_producer_roundtrip'
LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE FUNCTION test_otel_resource_attributes()
RETURNS text
AS 'MODULE_PATHNAME', 'test_otel_resource_attributes'
LANGUAGE C VOLATILE PARALLEL SAFE;
