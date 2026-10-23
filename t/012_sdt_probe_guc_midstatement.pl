# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Changing otel.trace_sdt_probes (or setting otel_api.traceparent ahead of
# it) in an existing session must not leave bridge spans open: a statement
# that disables the probe families of its own START probes would otherwise
# never see the matching DONE probes.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $TRACEPARENT = '00-aabbccddeeff00112233445566778899-0011223344556677-01';

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'otel_api,otel_postgres_tracing,test_otel_exporter'\n"
	  . "log_min_messages = warning\n");
$node->start;

$node->safe_psql('postgres', q{
	CREATE EXTENSION otel_api;
	CREATE EXTENSION otel_postgres_tracing;
	CREATE EXTENSION test_otel_exporter;
});

# Run $sql in one psql session; return its stderr.
sub run_session
{
	my ($sql) = @_;
	my ($stdout, $stderr);

	$node->psql('postgres', $sql, stdout => \$stdout, stderr => \$stderr);
	return $stderr;
}

unlike(
	run_session(qq{
		SET otel_api.traceparent = '$TRACEPARENT';
		SET otel.trace_sdt_probes = 'txn,lock_wait';
		SELECT test_otel_clear();
		SELECT 1;
	}),
	qr/not closed|still open/,
	'disabling statement families mid-statement leaks no span');

unlike(
	run_session(qq{
		SET otel_api.traceparent = '$TRACEPARENT';
		SET otel.trace_sdt_probes = 'all';
		SET otel.trace_sdt_probes = 'none';
		SELECT 1;
	}),
	qr/not closed|still open/,
	'disabling every family leaks no span');

unlike(
	run_session(qq{
		SET otel_api.traceparent = '$TRACEPARENT';
		SET otel.trace_sdt_probes = 'all';
		BEGIN;
		SET LOCAL otel.trace_sdt_probes = 'none';
		SELECT 1;
		COMMIT;
		SELECT 2;
	}),
	qr/not closed|still open/,
	'disabling txn mid-transaction leaks no span');

# Enabling families mid-statement: DONE probes with no matching START
# must not end an unrelated open span.
unlike(
	run_session(qq{
		SET otel.trace_sdt_probes = 'none';
		SET otel_api.traceparent = '$TRACEPARENT';
		SET otel.trace_sdt_probes = 'all';
		SELECT 1;
	}),
	qr/not closed|still open|out of order/,
	'enabling families mid-statement leaks no span');

done_testing();
