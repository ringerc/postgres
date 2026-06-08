# Copyright (c) 2026, PostgreSQL Global Development Group
#
# End-to-end test for the contrib/otel module: register the otel.*
# protocol-header handler, parse a W3C traceparent received from the
# client, and propagate the trace context into log emission via the
# emit_log_hook + log_line_prefix %{key}A annotation escapes.
#
# The test bashes the v3 wire protocol on a raw socket so it can send
# RequestHeaders ('M') messages, which no libpq API yet exposes.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# A representative W3C TraceContext to use throughout the test.
my $TRACE_ID	= 'aabbccddeeff00112233445566778899';
my $SPAN_ID	 = '0011223344556677';
my $FLAGS	   = '01';
my $TRACEPARENT = "00-$TRACE_ID-$SPAN_ID-$FLAGS";

# ----------------------------------------------------------------------
# Cluster setup
# ----------------------------------------------------------------------

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel,otel_postgres_tracing'
log_statement = 'all'
log_min_messages = log
log_line_prefix = 'TR[%{trace_id}A] SP[%{span_id}A] '
EOCONF
$node->start;

# Install the SQL surface (otel_current_traceparent).
$node->safe_psql('postgres', 'CREATE EXTENSION otel');

if (!$node->raw_connect_works())
{
	plan skip_all => "this test requires working raw_connect()";
}

# ----------------------------------------------------------------------
# Raw-protocol helpers (same shape as test_protocol_headers)
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

# Read and accumulate messages until ReadyForQuery; return the list of
# (type, body) pairs that arrived.
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
	my $n = scalar(@kv) / 2;
	my $body = pack('n', $n);
	while (@kv)
	{
		my $k = shift @kv;
		my $v = shift @kv;
		$body .= $k . "\0" . $v . "\0";
	}
	return $body;
}

# Extract the first column of the first DataRow from a list of messages.
# Returns the value as a string, or undef if the column was NULL or no
# DataRow was found.
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
		return undef if $len == 0xFFFFFFFF;	   # SQL NULL
		return substr($body, 6, $len);
	}
	return undef;
}

# Issue a simple Query, drain the response, return all messages.
sub run_query
{
	my ($sock, $sql) = @_;
	send_msg($sock, 'Q', "$sql\0");
	return drain_to_rfq($sock);
}

# ----------------------------------------------------------------------
# Handshake with _pq_.headers=1
# ----------------------------------------------------------------------

my $superuser = getpwuid($<);
my $sock = $node->raw_connect();
send_startup(
	$sock,
	user => $superuser,
	database => 'postgres',
	'_pq_.headers' => '1');

my @startup = drain_to_rfq($sock);
ok(!(grep { $_->[0] eq 'v' } @startup),
	'_pq_.headers=1 accepted (no NegotiateProtocolVersion)');

# ----------------------------------------------------------------------
# Test 1: an M with otel.traceparent makes the trace_id appear in
# subsequent log lines via log_line_prefix.
# ----------------------------------------------------------------------

my $log_offset = -s $node->logfile;

send_msg($sock, 'M', headers_body('otel.traceparent' => $TRACEPARENT));
run_query($sock, 'SELECT 1');

$node->wait_for_log(
	qr/TR\[$TRACE_ID\] SP\[$SPAN_ID\] LOG:\s+statement: SELECT 1/,
	$log_offset);
pass('SELECT 1 logged with trace_id and span_id from otel.traceparent');

# ----------------------------------------------------------------------
# Test 2: SQL-level introspection via otel_current_traceparent().
#
# The implicit transaction around Test 1's SELECT 1 cleared the
# transaction-scope context, so send another M before this query.
# ----------------------------------------------------------------------

send_msg($sock, 'M', headers_body('otel.traceparent' => $TRACEPARENT));
my @msgs = run_query($sock, 'SELECT otel_current_traceparent()');
is(first_value(@msgs), $TRACEPARENT,
	'otel_current_traceparent() returns the active traceparent');

# ----------------------------------------------------------------------
# Test 3: transaction-scope persistence and clear at COMMIT.
# ----------------------------------------------------------------------

run_query($sock, 'BEGIN');

$log_offset = -s $node->logfile;
send_msg($sock, 'M', headers_body('otel.traceparent' => $TRACEPARENT));

run_query($sock, 'SELECT 2');
run_query($sock, 'SELECT 3');

# Both statements within the BEGIN block should carry the trace context.
my $mid_log =
  PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);
like(
	$mid_log,
	qr/TR\[$TRACE_ID\] SP\[$SPAN_ID\] LOG:\s+statement: SELECT 2/,
	'SELECT 2 inside transaction has trace context');
like(
	$mid_log,
	qr/TR\[$TRACE_ID\] SP\[$SPAN_ID\] LOG:\s+statement: SELECT 3/,
	'SELECT 3 inside transaction has trace context');

run_query($sock, 'COMMIT');

# After COMMIT, the per-transaction effect should have cleared.  A new
# unrelated statement must NOT carry the trace context.
$log_offset = -s $node->logfile;
run_query($sock, 'SELECT 4');
my $post_commit =
  PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);
like(
	$post_commit,
	qr/TR\[\] SP\[\] LOG:\s+statement: SELECT 4/,
	'SELECT 4 after COMMIT has empty trace context (cleared at COMMIT)');

# ----------------------------------------------------------------------
# Test 4: a malformed traceparent is silently ignored; no trace context
# is installed.
# ----------------------------------------------------------------------

$log_offset = -s $node->logfile;
send_msg($sock, 'M',
	headers_body('otel.traceparent' => 'not-a-valid-traceparent'));
run_query($sock, 'SELECT 5');
my $malformed =
  PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);
like(
	$malformed,
	qr/TR\[\] SP\[\] LOG:\s+statement: SELECT 5/,
	'malformed traceparent is ignored; no trace context attached');

# ----------------------------------------------------------------------
# Test 5: empty value clears a previously-set traceparent.
# ----------------------------------------------------------------------

run_query($sock, 'BEGIN');
send_msg($sock, 'M', headers_body('otel.traceparent' => $TRACEPARENT));
$log_offset = -s $node->logfile;
run_query($sock, 'SELECT 6');
my $with_trace =
  PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);
like(
	$with_trace,
	qr/TR\[$TRACE_ID\] SP\[$SPAN_ID\] LOG:\s+statement: SELECT 6/,
	'SELECT 6 inside transaction has trace context (before explicit clear)');

send_msg($sock, 'M', headers_body('otel.traceparent' => ''));
$log_offset = -s $node->logfile;
run_query($sock, 'SELECT 7');
my $after_clear =
  PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);
like(
	$after_clear,
	qr/TR\[\] SP\[\] LOG:\s+statement: SELECT 7/,
	'SELECT 7 has no trace context after explicit empty-value clear');

run_query($sock, 'ROLLBACK');

# ----------------------------------------------------------------------
# Test 6: parallel-worker propagation.
#
# The trace context is stored in custom GUCs (otel.traceparent,
# otel.tracestate); custom GUCs propagate to parallel workers as part
# of the standard parallel-state machinery (RestoreGUCState during
# worker startup).  The worker's assign_hook fires during that
# restore, populating the worker's in-memory OtelContext.
#
# debug_parallel_query=regress forces every query through a Gather,
# so otel_current_traceparent() runs in a worker rather than the
# leader.  Without propagation the function would return NULL (the
# worker's otel_ctx would be empty); with propagation it returns the
# leader's active traceparent.
# ----------------------------------------------------------------------

run_query($sock, "SET debug_parallel_query = regress");

run_query($sock, 'BEGIN');
send_msg($sock, 'M', headers_body('otel.traceparent' => $TRACEPARENT));

my @parallel_msgs = run_query($sock, 'SELECT otel_current_traceparent()');
is(first_value(@parallel_msgs), $TRACEPARENT,
	'parallel worker sees the trace context (GUC propagated, assign_hook populated worker otel_ctx)');

run_query($sock, 'COMMIT');
run_query($sock, "SET debug_parallel_query = off");

# ----------------------------------------------------------------------
# Tidy up.
# ----------------------------------------------------------------------

send_msg($sock, 'X');
$sock->close();

$node->stop;
done_testing();
