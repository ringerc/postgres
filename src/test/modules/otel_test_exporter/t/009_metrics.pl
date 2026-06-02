# Copyright (c) 2026, PostgreSQL Global Development Group
#
# End-to-end test for contrib/otel's metrics API + the self-metric
# "otel.spans.dropped" registered by the producer-side stack code.
#
# Forces both overflow drops (push beyond MAX_SPAN_STACK_DEPTH=64)
# and unwound drops (push with OTEL_UNWIND_DROP and let the per-call
# memory context reset them) in a single backend, then reads the
# counter back via metric_collect_self.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

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
# Step 1: baseline --- no spans have been dropped yet in this backend,
# so the otel.spans.dropped counter should not appear at all (cells
# with value 0 are excluded from collect_self output).
# ----------------------------------------------------------------------

my $before = $node->safe_psql('postgres', 'SELECT test_otel_metrics_dump()');
isnt($before, qr/otel\.spans\.dropped/,
	'baseline: otel.spans.dropped counter has no non-zero cells yet');

# ----------------------------------------------------------------------
# Step 2: push 100 spans with OTEL_UNWIND_DROP, which forces:
#   * 36 overflow drops (100 - 64)
#   * 64 unwound drops (when CurrentMemoryContext resets after the
#     function returns)
# The two SELECTs run in the same session so test_otel_force_drop_spans
# and test_otel_metrics_dump observe the same backend's counters.
# ----------------------------------------------------------------------

my $combined = $node->safe_psql('postgres', q{
	SELECT 'PUSHED:' || test_otel_force_drop_spans(100);
	SELECT test_otel_metrics_dump();
});

# First line is the pushed count.
my ($pushed_line, @rest) = split /\n/, $combined;
is($pushed_line, 'PUSHED:64',
	'force_drop_spans(100) pushed exactly MAX_SPAN_STACK_DEPTH=64');

my $dump = join("\n", @rest);

# The dump has one "meter=...;..." record per (instrument, attr) cell.
# Split into records, each delimited by "|".
my @records = split /\|/, $dump;

# Look for the overflow and unwound cells.
my %got;
for my $r (@records)
{
	if ($r =~ /instrument=otel\.spans\.dropped.*attr_value=(\w+);.*value=(\d+)/)
	{
		$got{$1} = $2;
	}
}

is($got{overflow}, '36',
	'overflow drops: pushed 100, cap is 64, so 36 were dropped at push time');
is($got{unwound}, '64',
	'unwound drops: the 64 pushed spans were dropped when the memcxt reset');
ok(!exists $got{out_of_order},
	'out_of_order cell stays empty (no out-of-order emit triggered)');

# ----------------------------------------------------------------------
# Step 3: snapshot fields are well-formed --- meter / unit / kind.
# ----------------------------------------------------------------------

my $sample = (grep { /instrument=otel\.spans\.dropped/ } @records)[0];
like($sample, qr/meter=contrib\/otel/,
	'snapshot identifies the producer meter as contrib/otel');
like($sample, qr/unit=1/,
	'snapshot carries the OTel "1" unit annotation');
like($sample, qr/kind=1/,
	'snapshot kind=1 corresponds to OTEL_INSTRUMENT_COUNTER');
like($sample, qr/attr_key=reason/,
	'snapshot attribute key is "reason"');

done_testing();
