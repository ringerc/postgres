# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Each parallel worker's pgsql.execute span carries its worker number and
# backend pid; the leader's carries pg.parallel.leader.  A statement that
# launches no workers is not marked as a leader.  Worker spans are
# backend-local, so they are read from the JSON log-line emitter.

use strict;
use warnings FATAL => 'all';

use JSON::PP;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $TRACE_ID    = 'aabbccddeeff00112233445566778899';
my $TRACEPARENT = "00-$TRACE_ID-0011223344556677-01";

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'otel_api,otel_postgres_tracing,test_otel_exporter'\n"
	  . "otel_api.emit_spans_to_log = on\n"
	  . "otel.trace_all_queries = on\n"
	  . "max_worker_processes = 8\n"
	  . "log_min_messages = warning\n");
$node->start;

$node->safe_psql('postgres', q{
	CREATE EXTENSION otel_api;
	CREATE EXTENSION otel_postgres_tracing;
	CREATE EXTENSION test_otel_exporter;
	CREATE TABLE big AS SELECT g AS id FROM generate_series(1, 200000) g;
	ANALYZE big;
});

# Run $sql in one session with parallelism forced; return the decoded
# pgsql.execute spans whose query text contains $marker.
sub execute_spans
{
	my ($sql, $marker) = @_;
	my $log_start = -s $node->logfile;

	$node->safe_psql('postgres', qq{
		SET otel_api.traceparent = '$TRACEPARENT';
		SET parallel_setup_cost = 0;
		SET parallel_tuple_cost = 0;
		SET min_parallel_table_scan_size = 0;
		SET max_parallel_workers_per_gather = 2;
		SET debug_parallel_query = off;
		$sql});

	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_start);
	my @spans;
	for my $line (split /\n/, $log)
	{
		next unless $line =~ /otel-span: (\{.*\})\s*$/;
		my $span = decode_json($1);
		next unless $span->{name} eq 'pgsql.execute';
		next unless ($span->{attributes}{'db.query.text'} // '') =~ /\Q$marker\E/;
		push @spans, $span;
	}
	return @spans;
}

my @spans = execute_spans(
	q{SELECT count(*) /* par_marker */ FROM big;}, 'par_marker');
my @workers = grep { defined $_->{attributes}{'pg.parallel.worker_number'} } @spans;
my @leaders = grep { $_->{attributes}{'pg.parallel.leader'} } @spans;

is(scalar(@leaders), 1, 'exactly one leader span');
ok(!defined $leaders[0]{attributes}{'pg.parallel.worker_number'},
	'leader span has no worker number');
cmp_ok(scalar(@workers), '>=', 2, 'worker spans emitted');

my %numbers = map { $_->{attributes}{'pg.parallel.worker_number'} => 1 } @workers;
is(scalar(keys %numbers), scalar(@workers), 'worker numbers are distinct');

my %pids = map { $_->{attributes}{'pg.backend.pid'} => 1 } @workers;
is(scalar(keys %pids), scalar(@workers), 'worker pids are distinct');
ok(!exists $pids{$leaders[0]{attributes}{'pg.backend.pid'} // 0},
	'worker pids differ from the leader pid');

is(scalar(grep { $_->{name} ne 'pgsql.execute' } @workers), 0,
	'worker number is not in the span name');

# No workers launched: not marked as a leader.
@spans = execute_spans(
	q{SET max_parallel_workers_per_gather = 0;
	  SELECT count(*) /* serial_marker */ FROM big;}, 'serial_marker');
is(scalar(grep { $_->{attributes}{'pg.parallel.leader'} } @spans), 0,
	'serial statement is not a leader');

done_testing();
