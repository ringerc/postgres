# Copyright (c) 2026, PostgreSQL Global Development Group
#
# InstrumentationScope round-trip test.
#
# Verifies that spans produced by contrib/otel_postgres_tracing
# carry an OTel InstrumentationScope identifying that module as the
# producer, and that spans produced via the producer-API path from
# test_otel_exporter carry that module's scope instead.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $TRACEPARENT = '00-aabbccddeeff00112233445566778899-0011223344556677-01';

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel,otel_postgres_tracing,test_otel_exporter'
log_min_messages = warning
EOCONF
$node->start;
$node->safe_psql('postgres',
	'CREATE EXTENSION otel; CREATE EXTENSION test_otel_exporter');

# ----------------------------------------------------------------------
# Test 1: spans emitted by contrib/otel_postgres_tracing for executor
# work carry scope = "contrib/otel_postgres_tracing".  We force a span
# via SET otel.traceparent with the sampled wire bit set; the
# default sampler-hook policy passes that through without consulting
# test_otel_exporter's sampler hook.
# ----------------------------------------------------------------------

my $exec_span = $node->safe_psql('postgres', qq{
	SELECT test_otel_clear();
	SET otel.traceparent = '$TRACEPARENT';
	SELECT 1;
	SELECT test_otel_pop_span();
});

like($exec_span, qr/^scope\.name=contrib\/otel_postgres_tracing$/m,
	'executor span: scope.name identifies the trace module');
like($exec_span, qr/^scope\.version=\d/m,
	'executor span: scope.version is populated (PG_VERSION)');

# ----------------------------------------------------------------------
# Test 2: spans emitted via test_otel_exporter's producer-API path
# carry scope = "test_otel_exporter", NOT contrib/otel_postgres_tracing.
# Same backend; the producer roundtrip is isolated from query
# execution.
# ----------------------------------------------------------------------

my $producer_span = $node->safe_psql('postgres', q{
	SELECT test_otel_clear();
	SELECT 'IGNORE:' || test_otel_producer_roundtrip('scope.test');
	SELECT test_otel_pop_span();
});

like($producer_span, qr/^scope\.name=test_otel_exporter$/m,
	'producer-API span: scope.name identifies the producer module');
like($producer_span, qr/^scope\.version=1\.0$/m,
	'producer-API span: scope.version matches what was registered');
like($producer_span, qr/^name=scope\.test$/m,
	'producer-API span: span name is the operation, not the scope');

done_testing();
