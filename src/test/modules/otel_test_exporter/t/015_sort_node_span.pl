# Copyright (c) 2026, PostgreSQL Global Development Group
#
# With otel.trace_plan_child_spans on, a Sort plan node gets a pg.exec.sort
# span carrying the sort method, space type and space used, and lasting as
# long as the node's own instrumented time (not the whole statement).  The
# coarse SDT pg.sort family is not in the default family list.

use strict;
use warnings FATAL => 'all';

use JSON::PP;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $TRACEPARENT = '00-aabbccddeeff00112233445566778899-0011223344556677-01';

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'otel_api,otel_postgres_tracing,test_otel_exporter'\n"
	  . "otel_api.emit_spans_to_log = on\n"
	  . "otel.trace_all_queries = on\n"
	  . "log_min_messages = warning\n");
$node->start;

$node->safe_psql('postgres', q{
	CREATE EXTENSION otel_api;
	CREATE EXTENSION otel_postgres_tracing;
	CREATE EXTENSION test_otel_exporter;
	CREATE TABLE big AS SELECT g AS id, md5(g::text) AS v
		FROM generate_series(1, 100000) g;
	ANALYZE big;
});

# Run $sql in one session; return all spans whose trace carries the marker's
# pgsql.execute span, keyed by name.
sub run_spans
{
	my ($sql, $marker) = @_;
	my $log_start = -s $node->logfile;

	$node->safe_psql('postgres', qq{
		SET otel_api.traceparent = '$TRACEPARENT';
		SET max_parallel_workers_per_gather = 0;
		$sql});

	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_start);
	my @spans;
	for my $line (split /\n/, $log)
	{
		push @spans, decode_json($1) if $line =~ /otel-span: (\{.*\})\s*$/;
	}
	my ($exec) = grep {
		$_->{name} eq 'pgsql.execute'
		  && ($_->{attributes}{'db.query.text'} // '') =~ /\Q$marker\E/
	} @spans;
	return ($exec, @spans);
}

# Span duration in seconds from the emitter's "YYYY-MM-DD HH:MM:SS.ffffff+TZ"
# timestamps (time of day only; the test statements run in well under a day).
sub tod
{
	$_[0] =~ /(\d+):(\d+):(\d+(?:\.\d+)?)/ or die "bad timestamp $_[0]";
	return $1 * 3600 + $2 * 60 + $3;
}
sub dur { tod($_[0]{end_time}) - tod($_[0]{start_time}) }

# Off by default: no node spans.
my ($exec, @spans) = run_spans(
	q{SELECT count(*) FROM (SELECT * FROM big ORDER BY v) s /* off_marker */;},
	'off_marker');
is(scalar(grep { $_->{name} eq 'pg.exec.sort' } @spans), 0,
	'no sort node span without otel.trace_plan_child_spans');
is(scalar(grep { $_->{name} eq 'pg.sort' } @spans), 0,
	'SDT pg.sort is not in the default families');

# In-memory sort.
($exec, @spans) = run_spans(
	q{SET otel.trace_plan_child_spans = on;
	  SET work_mem = '64MB';
	  SELECT count(*) FROM (SELECT * FROM big ORDER BY v) s /* mem_marker */;},
	'mem_marker');
my @sorts = grep { $_->{name} eq 'pg.exec.sort' } @spans;
is(scalar(@sorts), 1, 'one sort node span');
my $a = $sorts[0]{attributes};
is($a->{'pg.sort.method'}, 'quicksort', 'in-memory sort method');
is($a->{'pg.sort.space_type'}, 'Memory', 'memory space type');
cmp_ok($a->{'pg.sort.space_kb'}, '>', 0, 'space used reported');
is($sorts[0]{parent_span_id}, $exec->{span_id},
	'sort node span nests under pgsql.execute');
cmp_ok(dur($sorts[0]), '>', 0, 'sort node span has a duration');
cmp_ok(dur($sorts[0]), '<=', dur($exec),
	'sort node span is no longer than the statement');

# Disk sort.
($exec, @spans) = run_spans(
	q{SET otel.trace_plan_child_spans = on;
	  SET work_mem = '64kB';
	  SELECT count(*) FROM (SELECT * FROM big ORDER BY v) s /* disk_marker */;},
	'disk_marker');
@sorts = grep { $_->{name} eq 'pg.exec.sort' } @spans;
is(scalar(@sorts), 1, 'one sort node span for the spilling sort');
$a = $sorts[0]{attributes};
is($a->{'pg.sort.space_type'}, 'Disk', 'disk space type');
like($a->{'pg.sort.method'}, qr/^external/, 'external sort method');

# The coarse SDT family can still be requested explicitly.
($exec, @spans) = run_spans(
	q{SET otel.trace_sdt_probes = 'query,execute,sort';
	  SELECT count(*) FROM (SELECT * FROM big ORDER BY v) s /* sdt_marker */;},
	'sdt_marker');
cmp_ok(scalar(grep { $_->{name} eq 'pg.sort' } @spans), '>=', 1,
	'pg.sort emitted when the sort family is requested');

done_testing();
