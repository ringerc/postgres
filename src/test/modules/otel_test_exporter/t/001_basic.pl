# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Span-lifecycle end-to-end test for contrib/otel.
#
# Loads both contrib/otel and the test-only span exporter
# (test_otel_exporter) into a cluster, attaches a W3C traceparent
# via the per-message RequestHeaders ('M') protocol mechanism, runs
# a query, and reads the captured span back through the exporter's
# SQL surface.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $TRACE_ID    = 'aabbccddeeff00112233445566778899';
my $SPAN_ID     = '0011223344556677';
my $FLAGS       = '01';
my $TRACEPARENT = "00-$TRACE_ID-$SPAN_ID-$FLAGS";

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel,otel_postgres_tracing,test_otel_exporter'
log_min_messages = warning
log_statement = 'all'
EOCONF
$node->start;

# raw_connect_works() requires a running postmaster (it tests by actually
# connecting), so this check must come after $node->start.  Run it before
# the CREATE EXTENSION below to avoid wasted setup work on the skip path.
if (!$node->raw_connect_works())
{
	plan skip_all => "this test requires working raw_connect()";
}

$node->safe_psql('postgres',
	'CREATE EXTENSION otel; CREATE EXTENSION test_otel_exporter');

# ----------------------------------------------------------------------
# Raw-protocol helpers (same shape as the existing otel TAP test).
# ----------------------------------------------------------------------

sub send_startup
{
	my ($sock, @kv) = @_;
	my $body = pack('N', 0x00030000);
	while (@kv)
	{
		my $k = shift @kv;
		my $v = shift @kv;
		$body .= $k . "\0" . $v . "\0";
	}
	$body .= "\0";
	$sock->send(pack('N', length($body) + 4) . $body)
	  or die "send_startup: $!";
}

sub send_msg
{
	my ($sock, $type, $body) = @_;
	$body = '' unless defined $body;
	$sock->send($type . pack('N', length($body) + 4) . $body)
	  or die "send_msg: $!";
}

sub recv_exact
{
	my ($sock, $n) = @_;
	my $buf = '';
	while (length($buf) < $n)
	{
		my $chunk = '';
		my $got = $sock->recv($chunk, $n - length($buf));
		die "recv_exact: $!" unless defined $got;
		return undef if length($chunk) == 0;
		$buf .= $chunk;
	}
	return $buf;
}

sub recv_msg
{
	my ($sock) = @_;
	my $hdr = recv_exact($sock, 5);
	return undef unless defined $hdr;
	my ($type, $len) = unpack('A1 N', $hdr);
	my $body = ($len > 4) ? recv_exact($sock, $len - 4) : '';
	return ($type, $body);
}

sub drain_to_rfq
{
	my ($sock) = @_;
	my @msgs;
	while (1)
	{
		my ($type, $body) = recv_msg($sock);
		die "connection closed before ReadyForQuery" unless defined $type;
		push @msgs, [ $type, $body ];
		last if $type eq 'Z';
	}
	return @msgs;
}

sub headers_body
{
	my @kv = @_;
	my $n  = scalar(@kv) / 2;
	my $body = pack('n', $n);
	while (@kv)
	{
		my $k = shift @kv;
		my $v = shift @kv;
		$body .= $k . "\0" . $v . "\0";
	}
	return $body;
}

sub first_value
{
	my (@msgs) = @_;
	for my $m (@msgs)
	{
		my ($type, $body) = @$m;
		next unless $type eq 'D';
		my $nfields = unpack('n', substr($body, 0, 2));
		return undef if $nfields == 0;
		my $len = unpack('N', substr($body, 2, 4));
		return undef if $len == 0xFFFFFFFF;
		return substr($body, 6, $len);
	}
	return undef;
}

sub run_query
{
	my ($sock, $sql) = @_;
	send_msg($sock, 'Q', "$sql\0");
	return drain_to_rfq($sock);
}

# ----------------------------------------------------------------------
# Handshake.
# ----------------------------------------------------------------------

my $superuser = getpwuid($<);
my $sock = $node->raw_connect();
send_startup(
	$sock,
	user           => $superuser,
	database       => 'postgres',
	'_pq_.headers' => '1');
drain_to_rfq($sock);

# ----------------------------------------------------------------------
# Test 1: span is emitted for a traced query.
# ----------------------------------------------------------------------

run_query($sock, 'SELECT test_otel_clear()');

send_msg($sock, 'M', headers_body('otel.traceparent' => $TRACEPARENT));
run_query($sock, 'SELECT 1');

# Wait until the exporter has the span (the ring is per-backend; we
# query it through a separate psql connection since this conn is busy
# with the raw protocol).
my $count = $node->safe_psql('postgres', 'SELECT test_otel_span_count()');
# Note: span_count is per-backend; the safe_psql call opens a new
# backend with its own (empty) ring.  So we need to fetch through the
# SAME backend that emitted the span --- which is the raw socket
# connection.  Switch back to that connection for the readout.

my @msgs = run_query($sock, 'SELECT test_otel_span_count()');
is(first_value(@msgs), '1',
	'one span captured by the test exporter');

@msgs = run_query($sock, 'SELECT test_otel_pop_span()');
my $span = first_value(@msgs);
ok(defined $span && length $span,
	'test_otel_pop_span returned a span body');

# trace_id matches what the client supplied
like($span, qr/trace_id=$TRACE_ID/,
	'span carries the propagated trace_id');

# span_id is freshly generated --- 16 lowercase hex chars, NOT equal
# to the supplied parent_span_id
like($span, qr/span_id=[0-9a-f]{16}/,
	'span_id is 16 lowercase hex chars');
unlike($span, qr/^span_id=$SPAN_ID/m,
	'span_id is not the client-supplied parent (it is freshly generated)');

# parent_span_id is what the client supplied
like($span, qr/parent_span_id=$SPAN_ID/,
	'parent_span_id is the client-supplied span_id from traceparent');

# trace_flags matches
like($span, qr/trace_flags=$FLAGS/,
	'trace_flags match the client value');

# Expected attributes are present
like($span, qr/attr=db\.system=postgresql/,
	'db.system attribute is "postgresql"');
like($span, qr/attr=db\.name=postgres/,
	'db.name attribute is the connected database');
like($span, qr/attr=db\.statement=SELECT 1/,
	'db.statement attribute is the SQL text');

# Status is UNSET (0) for a successful query
like($span, qr/status=0\n/,
	'status is UNSET on the success path');

# Timestamps are sane (start <= end)
my ($start) = $span =~ /start_time=(-?\d+)/;
my ($end)   = $span =~ /end_time=(-?\d+)/;
ok($end >= $start, 'end_time >= start_time');

# ----------------------------------------------------------------------
# Test 2: with no trace context, no span is emitted (trace_all_queries
# is off by default).
# ----------------------------------------------------------------------

run_query($sock, 'SELECT test_otel_clear()');
run_query($sock, 'SELECT 1');

@msgs = run_query($sock, 'SELECT test_otel_span_count()');
is(first_value(@msgs), '0',
	'no span emitted when no trace context is active');

# ----------------------------------------------------------------------
# Test 3: trace_all_queries=on emits parentless spans even without
# client-propagated context.
# ----------------------------------------------------------------------

run_query($sock, 'SELECT test_otel_clear()');
run_query($sock, 'SET otel.trace_all_queries = on');
run_query($sock, 'SELECT 1');
run_query($sock, 'RESET otel.trace_all_queries');

@msgs = run_query($sock, 'SELECT test_otel_span_count()');
isnt(first_value(@msgs), '0',
	'spans emitted under trace_all_queries even without propagated context');

@msgs = run_query($sock, 'SELECT test_otel_pop_span()');
$span = first_value(@msgs);
# Parentless: parent_span_id is empty
like($span, qr/parent_span_id=\n/,
	'synthesized span has no parent');
# trace_id is a synthesized 32 hex chars
like($span, qr/trace_id=[0-9a-f]{32}\n/,
	'synthesized trace_id is 32 lowercase hex chars');

# ----------------------------------------------------------------------
# Test 4: a query that raises an ERROR produces a span with
# status=ERROR and an event captured from the ereport.
# ----------------------------------------------------------------------

run_query($sock, 'SELECT test_otel_clear()');
send_msg($sock, 'M', headers_body('otel.traceparent' => $TRACEPARENT));
# Division by zero against a set-returning-function source --- the
# value isn't known until execution, so the planner cannot fold it
# and the error fires inside the executor where our hooks live.
# 22012 is ERRCODE_DIVISION_BY_ZERO.
run_query($sock, 'SELECT 1/i FROM generate_series(0,0) AS i');

@msgs = run_query($sock, 'SELECT test_otel_span_count()');
is(first_value(@msgs), '1',
	'one span captured even though the query errored');

@msgs = run_query($sock, 'SELECT test_otel_pop_span()');
$span = first_value(@msgs);
like($span, qr/status=2\n/,
	'span status is ERROR (2) for a query that raised an ereport(ERROR)');
# In current postgres (v18+), ERROR's numeric value is 21
# (WARNING_CLIENT_ONLY occupies 20).
like($span, qr/event\.elevel=21\n/,
	'event elevel matches ERROR (21)');
like($span, qr/event\.sqlstate=22012\n/,
	'event sqlstate is 22012 (division_by_zero)');
like($span, qr/event\.message=division by zero/,
	'event message captures the ereport text');
like($span, qr/event\.filename=\w+\.c/,
	'event filename is a postgres source file');

# Connection is now in a failed-transaction state; ROLLBACK to clear.
run_query($sock, 'ROLLBACK');

# ----------------------------------------------------------------------
# Test 5: utility commands (DO block, BEGIN/COMMIT) get spans via
# ProcessUtility_hook.  Span name reflects the command tag.
# ----------------------------------------------------------------------

run_query($sock, 'SELECT test_otel_clear()');
send_msg($sock, 'M', headers_body('otel.traceparent' => $TRACEPARENT));
# RAISE WARNING from PL/pgSQL --- the inline DO block emits a
# WARNING during execution and then continues to a successful
# return.  DO goes through ProcessUtility (not ExecutorStart).
run_query($sock, q{DO $$ BEGIN RAISE WARNING 'test-warn'; END $$});

@msgs = run_query($sock, 'SELECT test_otel_span_count()');
isnt(first_value(@msgs), '0',
	'at least one span captured for the DO block');

@msgs = run_query($sock, 'SELECT test_otel_pop_span()');
$span = first_value(@msgs);
like($span, qr/name=DO\n/,
	'utility span name is "DO" (the command tag)');
like($span, qr/status=0\n/,
	'utility span status remains UNSET when only WARNING fires');
like($span, qr/event\.elevel=19\n/,
	'WARNING-level event captured on the utility span (elevel 19)');
like($span, qr/event\.message=test-warn/,
	'WARNING message captured on the utility span');

# ----------------------------------------------------------------------
# Test 6: explicit BEGIN/COMMIT cycle - the BEGIN and COMMIT statements
# each produce a utility span with the matching name.
# ----------------------------------------------------------------------

run_query($sock, 'SELECT test_otel_clear()');
send_msg($sock, 'M', headers_body('otel.traceparent' => $TRACEPARENT));
run_query($sock, 'BEGIN');
run_query($sock, 'COMMIT');

# At least two spans (BEGIN and COMMIT); names should reflect.
@msgs = run_query($sock, 'SELECT test_otel_pop_span()');
$span = first_value(@msgs);
like($span, qr/name=BEGIN\n/, 'first utility span is BEGIN');

@msgs = run_query($sock, 'SELECT test_otel_pop_span()');
$span = first_value(@msgs);
like($span, qr/name=COMMIT\n/, 'second utility span is COMMIT');

# ----------------------------------------------------------------------
# Test 7: a traceparent with the sampled bit UNSET (flags = "00")
# produces no span, by default.  This is the OTel-SDK ParentBased
# default behaviour: contrib/otel honours an upstream "do not sample"
# signal unless an exporter module installs a sampler hook to override
# it.  No sampler hook is registered here.
# ----------------------------------------------------------------------

my $UNSAMPLED_TRACEPARENT = "00-$TRACE_ID-$SPAN_ID-00";

run_query($sock, 'SELECT test_otel_clear()');
send_msg($sock, 'M',
	headers_body('otel.traceparent' => $UNSAMPLED_TRACEPARENT));
run_query($sock, 'SELECT 1');

@msgs = run_query($sock, 'SELECT test_otel_span_count()');
is(first_value(@msgs), '0',
	'no span emitted for an upstream-unsampled traceparent (default policy)');

# But trace_all_queries still wins over the unsampled bit --- the
# force-on path bypasses propagated sampling state entirely.
run_query($sock, 'SELECT test_otel_clear()');
send_msg($sock, 'M',
	headers_body('otel.traceparent' => $UNSAMPLED_TRACEPARENT));
run_query($sock, 'SET otel.trace_all_queries = on');
run_query($sock, 'SELECT 1');
run_query($sock, 'RESET otel.trace_all_queries');

@msgs = run_query($sock, 'SELECT test_otel_span_count()');
isnt(first_value(@msgs), '0',
	'trace_all_queries overrides unsampled traceparent');

# ----------------------------------------------------------------------
# Tidy up.
# ----------------------------------------------------------------------

send_msg($sock, 'X');
$sock->close();
$node->stop;
done_testing();
