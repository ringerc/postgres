# Copyright (c) 2026, PostgreSQL Global Development Group
#
# End-to-end test for the postgres.log_events Counter registered by
# contrib/otel_postgres_tracing.
#
# Triggers ereport events at NOTICE, WARNING, and ERROR severities
# in a single backend session, then dumps the per-backend metric
# snapshot and asserts each severity bucket holds the expected
# count.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
# log_min_messages = notice so NOTICE ereports actually reach
# emit_log_hook; default is "warning" which would suppress them.
$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel,otel_postgres_tracing,test_otel_exporter'
log_min_messages = notice
log_statement = 'none'
EOCONF
$node->start;
$node->safe_psql('postgres',
	'CREATE EXTENSION otel; CREATE EXTENSION test_otel_exporter');

# ----------------------------------------------------------------------
# All triggers + the dump must run in the same backend so the
# per-backend instrument table sees the same counter cells.  We get
# that by piping a single multi-statement script into psql with
# ON_ERROR_STOP=off so errors don't halt the session.
# ----------------------------------------------------------------------

my $combined;
my $stderr;
$node->psql(
	'postgres',
	q{
		DO $$BEGIN RAISE NOTICE 'demo notice 1'; END$$;
		DO $$BEGIN RAISE NOTICE 'demo notice 2'; END$$;
		DO $$BEGIN RAISE WARNING 'demo warning'; END$$;
		-- ERROR via 1/0: the implicit txn aborts, the next statement
		-- starts a fresh one.  emit_log_hook fires for the ERROR
		-- before the longjmp.
		SELECT 1/0;
		SELECT 'STILL_ALIVE';
		SELECT test_otel_metrics_dump();
	},
	stdout => \$combined,
	stderr => \$stderr,
	on_error_stop => 0);

# psql returned the metric dump as the last result row.
my @lines = split /\n/, $combined;
my $dump = pop @lines;
diag("metrics dump: $dump") if $ENV{TAP_DEBUG};

# ----------------------------------------------------------------------
# Parse the dump and pull out postgres.log_events buckets.
# ----------------------------------------------------------------------

my %got;
for my $r (split /\|/, $dump)
{
	if ($r =~ /instrument=postgres\.log_events.*attr_value=(\w+);.*value=(\d+)/)
	{
		$got{$1} = $2;
	}
}

# NOTICE: two RAISE NOTICEs above, but postgres also emits its own
# NOTICEs (extension creation, etc.) before the test triggers fire.
# Assert >= 2 (our two), not == 2.
cmp_ok($got{NOTICE} // 0, '>=', 2,
	'NOTICE counter saw at least the 2 RAISE NOTICEs in this session');

is($got{WARNING}, '1',
	'WARNING counter saw exactly one RAISE WARNING');

cmp_ok($got{ERROR} // 0, '>=', 1,
	'ERROR counter saw at least the 1/0 division error');

# Sanity: snapshot fields are well-formed for one of the log_events
# data points.
my $sample = (grep { /instrument=postgres\.log_events/ }
	split /\|/, $dump)[0];
like($sample, qr/meter=contrib\/otel_postgres_tracing/,
	'snapshot identifies the producer meter');
like($sample, qr/unit=1/,
	'snapshot carries the OTel "1" unit annotation');
like($sample, qr/attr_key=severity/,
	'snapshot attribute key is "severity"');

# FATAL/PANIC buckets should remain empty (we never triggered them).
ok(!exists $got{FATAL},
	'FATAL bucket stays empty for a healthy session');
ok(!exists $got{PANIC},
	'PANIC bucket stays empty for a healthy session');

done_testing();
