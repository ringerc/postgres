# Copyright (c) 2026, PostgreSQL Global Development Group
#
# SDT probes that fire inside parallel workers (pg.sort for a worker's
# Sort node) become spans nested under that worker's pgsql.execute span.
# Worker spans are backend-local, so they are read from the JSON log-line
# emitter.

use strict;
use warnings FATAL => 'all';

use JSON::PP;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $TRACEPARENT = 'aabbccddeeff00112233445566778899';
$TRACEPARENT = "00-$TRACEPARENT-0011223344556677-01";

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
	CREATE TABLE big AS SELECT g AS id, md5(g::text) AS v
		FROM generate_series(1, 200000) g;
	ANALYZE big;
});

my $log_start = -s $node->logfile;
$node->safe_psql('postgres', q{
	SET otel_api.traceparent = '} . $TRACEPARENT . q{';
	SET otel.trace_sdt_probes = 'query,execute,sort';
	SET parallel_setup_cost = 0;
	SET parallel_tuple_cost = 0;
	SET min_parallel_table_scan_size = 0;
	SET max_parallel_workers_per_gather = 2;
	SELECT count(*) FROM (SELECT * FROM big ORDER BY v) s /* sort_marker */;
});

my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_start);
my @spans;
for my $line (split /\n/, $log)
{
	push @spans, decode_json($1) if $line =~ /otel-span: (\{.*\})\s*$/;
}

# Worker execute spans: the statement's spans carrying a worker number.
my %worker_exec = map { $_->{span_id} => 1 }
  grep {
	$_->{name} eq 'pgsql.execute'
	  && defined $_->{attributes}{'pg.parallel.worker_number'}
	  && ($_->{attributes}{'db.query.text'} // '') =~ /sort_marker/
  } @spans;
cmp_ok(scalar(keys %worker_exec), '>=', 1, 'worker execute spans emitted');

my @worker_sorts =
  grep { $_->{name} eq 'pg.sort' && $worker_exec{$_->{parent_span_id}} } @spans;
cmp_ok(scalar(@worker_sorts), '>=', 1,
	'a worker\'s pg.sort span is a child of its pgsql.execute span');

done_testing();
