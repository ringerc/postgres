# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Producer-side API end-to-end test.
#
# Exercises the OtelTracingApi producer surface (span_link_to_active_
# and_push, span_emit, span_current_context, span_stack_depth, plus
# the convenience helpers span_init / span_add_attribute_string) via
# a single SQL function in the test exporter that runs the full
# sequence in one call.
#
# The capture ring in test_otel_exporter is per-backend static
# memory, so push + pop must happen in the same backend.  We
# accomplish this by issuing all SQL through one safe_psql call with
# multiple statements.
#
# Verifies that:
#   1. The roundtrip SQL function returns a valid 16-hex-char span_id.
#   2. Exactly one span lands in the ring after the call.
#   3. The captured span carries the name, kind, status, span_id and
#      attributes set by the producer code.
#   4. start_time < end_time (timestamps were captured).
#   5. The stack-depth assertions inside the C function held (no
#      elog ERROR was raised).
#
# No client-supplied trace context is propagated here; the span is
# the root of a brand-new trace.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf(
	'postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel_api,otel_postgres_tracing,test_otel_exporter'
log_min_messages = warning
EOCONF
$node->start;
$node->safe_psql('postgres',
	'CREATE EXTENSION otel_api; CREATE EXTENSION test_otel_exporter');

# --------------------------------------------------------------------
# Single psql session: clear, roundtrip, count, pop.  All in one
# backend so the per-backend capture ring is consistent across the
# calls.  Statement separator is ";"; output of each statement is
# concatenated by psql's --tuples-only --no-align output.
# --------------------------------------------------------------------
my $combined = $node->safe_psql(
	'postgres', q{
	SELECT test_otel_clear();
	SELECT 'SPAN_ID:' || test_otel_producer_roundtrip('producer.roundtrip');
	SELECT 'COUNT:' || test_otel_span_count();
	SELECT test_otel_pop_span();
	SELECT 'POSTCOUNT:' || test_otel_span_count();
});

# Pull out the span_id, count, and post-pop count from the combined
# output.  The body of test_otel_pop_span (key=value lines) is the
# bulk of $combined; we match patterns against the whole thing.
my ($span_id) = $combined =~ /^SPAN_ID:([0-9a-f]{16})$/m;
my ($count)   = $combined =~ /^COUNT:(\d+)$/m;
my ($postcount) = $combined =~ /^POSTCOUNT:(\d+)$/m;

ok(defined $span_id, 'roundtrip returned a 16-hex-char span_id');
is($count,     '1', 'exactly one span captured');
is($postcount, '0', 'ring drained after pop');

like($combined, qr/^name=producer\.roundtrip$/m,
	'name set by span_init');
like($combined, qr/^kind=0$/m,                'kind = INTERNAL');
like($combined, qr/^status=1$/m,              'status = OK');
like($combined, qr/^span_id=\Q$span_id\E$/m,
	'captured span_id matches what the SQL function returned');
like($combined, qr/^parent_span_id=$/m,
	'no parent (root span on a new trace)');
like($combined, qr/^trace_id=[0-9a-f]{32}$/m,
	'trace_id is auto-generated for root span (no propagated context)');
like($combined, qr/^attr=test\.case=roundtrip$/m,
	'attribute test.case round-tripped');
like($combined, qr/^attr=test\.name=producer\.roundtrip$/m,
	'attribute test.name round-tripped');

# The MINOR-3 generic event API: a named event with attributes attached
# via api->span_add_event must round-trip through the log dump.
like($combined, qr/^event\.name=test\.event$/m,
	'generic named event round-tripped (name)');
like($combined, qr/^event\.attr=event\.kind=generic$/m,
	'generic event attribute event.kind round-tripped');
like($combined, qr/^event\.attr=event\.seq=1$/m,
	'generic event attribute event.seq round-tripped');

my ($start) = $combined =~ /^start_time=(\d+)$/m;
my ($end)   = $combined =~ /^end_time=(\d+)$/m;
cmp_ok($start, '<=', $end, 'start_time captured before or at end_time');

# --------------------------------------------------------------------
# Second invocation in a fresh backend produces a different span_id.
# --------------------------------------------------------------------
my $second_combined = $node->safe_psql(
	'postgres', q{
	SELECT 'SPAN_ID:' || test_otel_producer_roundtrip('producer.roundtrip');
});
my ($span_id_2) = $second_combined =~ /^SPAN_ID:([0-9a-f]{16})$/m;
isnt($span_id_2, $span_id,
	'second roundtrip generates a fresh span_id');

$node->stop;
done_testing();
