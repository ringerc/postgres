# Copyright (c) 2026, PostgreSQL Global Development Group
#
# End-to-end test for the trace-context protocol message ('M' / TraceContext).
#
# Speaks the v3 wire protocol on a raw socket: performs a StartupMessage
# handshake at protocol 3.3, sends TraceContext ('M') messages interleaved
# with Query, and verifies:
#
#   * A single 'M' before a Query tags the whole operation (cleared at RFQ)
#   * Context is cleared at RFQ (next pipeline is untagged)
#   * GUC kill-switch (trace_context_enabled=off) -> protocol violation
#
# The handler is the test_trace_context loadable module which logs:
#   test_trace_context: apply traceparent=<tp> tracestate=<ts>
#   test_trace_context: clear

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# ----------------------------------------------------------------------
# Cluster setup
# ----------------------------------------------------------------------

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'test_trace_context'
log_min_messages = log
log_connections = 'receipt,authentication,authorization'
EOCONF
$node->start;

if (!$node->raw_connect_works())
{
	plan skip_all => "this test requires working raw_connect()";
}

my $TRACEPARENT =
  '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01';
my $TRACESTATE = 'vendor1=abc,vendor2=def';

# ----------------------------------------------------------------------
# Raw-protocol helpers
# ----------------------------------------------------------------------

# Read exactly $n bytes from socket (handles short reads).
sub recv_exact
{
	my ($sock, $n) = @_;
	my $buf = '';
	while (length($buf) < $n)
	{
		my $chunk = '';
		my $got   = $sock->recv($chunk, $n - length($buf));
		die "recv_exact: $!" unless defined $got;
		return undef if length($chunk) == 0;    # EOF
		$buf .= $chunk;
	}
	return $buf;
}

# Read one typed message from socket: returns (type, body).
sub read_msg
{
	my ($sock) = @_;
	my $hdr = recv_exact($sock, 5);
	return (undef, undef) unless defined $hdr;
	my ($type) = unpack('a', $hdr);
	my ($len)  = unpack('N', substr($hdr, 1, 4));
	my $body   = ($len > 4) ? recv_exact($sock, $len - 4) : '';
	return ($type, $body);
}

# Send a typed protocol message (1-byte type, 4-byte length-incl-self, body).
sub send_msg
{
	my ($sock, $type, $body) = @_;
	$body = '' unless defined $body;
	$sock->send($type . pack('N', length($body) + 4) . $body)
	  or die "send_msg: $!";
}

# Build a TraceContext ('M') wire body: two NUL-terminated strings.
sub trace_context_body
{
	my ($tp, $ts) = @_;
	$ts //= '';
	return "$tp\x00$ts\x00";
}

# Open a protocol 3.3 connection, drain startup until ReadyForQuery.
# Returns ($sock).
sub open_33_conn
{
	my $sock = $node->raw_connect();

	# StartupMessage with protocol 3.3
	my $startup =
	  pack('nn', 3, 3) . "user\x00" . $ENV{USER} . "\x00database\x00postgres\x00\x00";
	$sock->send(pack('N', length($startup) + 4) . $startup)
	  or die "send startup: $!";

	# Drain until ReadyForQuery ('Z')
	while (1)
	{
		my ($type, $body) = read_msg($sock);
		die "EOF during startup" unless defined $type;
		last if $type eq 'Z';
	}
	return $sock;
}

# Drain socket until ReadyForQuery, collecting ErrorResponse bodies.
# Returns list of ErrorResponse bodies seen before RFQ.
sub drain_until_rfq
{
	my ($sock) = @_;
	my @errors;
	while (1)
	{
		my ($type, $body) = read_msg($sock);
		die "EOF while draining" unless defined $type;
		push @errors, $body if $type eq 'E';
		last if $type eq 'Z';
	}
	return @errors;
}

# Count apply log lines for a specific traceparent since $offset.
sub count_apply
{
	my ($offset, $tp) = @_;
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $offset);
	my $re  = qr/test_trace_context: apply traceparent=\Q$tp\E/;
	return scalar(() = $log =~ /$re/g);
}

# Count clear log lines since $offset.
sub count_clear
{
	my ($offset) = @_;
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $offset);
	return scalar(() = $log =~ /test_trace_context: clear/g);
}

# ----------------------------------------------------------------------
# Test 1: Single M before Q -> handler fires once, cleared at RFQ
# ----------------------------------------------------------------------

{
	my $offset = -s $node->logfile;
	my $conn   = open_33_conn();

	# Send TraceContext 'M'
	my $body = trace_context_body($TRACEPARENT, $TRACESTATE);
	send_msg($conn, 'M', $body);

	# Send a simple Query
	send_msg($conn, 'Q', "SELECT 1;\x00");

	# Drain until RFQ
	drain_until_rfq($conn);

	$node->wait_for_log(
		qr/test_trace_context: apply traceparent=\Q$TRACEPARENT\E/, $offset);

	is(count_apply($offset, $TRACEPARENT), 1,
		'apply fires once for single M before Q');

	# After RFQ, clear should have fired
	$node->wait_for_log(qr/test_trace_context: clear/, $offset);
	is(count_clear($offset), 1, 'clear fires at RFQ');

	$conn->close();
}

# ----------------------------------------------------------------------
# Test 2: Context cleared at RFQ - next pipeline is untagged
# ----------------------------------------------------------------------

{
	my $offset = -s $node->logfile;
	my $conn   = open_33_conn();

	# First pipeline: send M then Q
	my $body = trace_context_body($TRACEPARENT, '');
	send_msg($conn, 'M', $body);
	send_msg($conn, 'Q', "SELECT 1;\x00");
	drain_until_rfq($conn);

	# Second pipeline: Q only (no M) - context must not carry over
	my $offset2 = -s $node->logfile;
	send_msg($conn, 'Q', "SELECT 1;\x00");
	drain_until_rfq($conn);

	$node->wait_for_log(
		qr/test_trace_context: apply traceparent=\Q$TRACEPARENT\E/, $offset);

	# apply should appear exactly once (first pipeline only)
	is(count_apply($offset, $TRACEPARENT), 1,
		'second pipeline without M is untagged (apply fires once)');

	$conn->close();
}

# ----------------------------------------------------------------------
# Test 3: Kill-switch off -> M is protocol violation (ERROR not FATAL)
# ----------------------------------------------------------------------

$node->append_conf('postgresql.conf', "trace_context_enabled = off\n");
$node->reload;

{
	my $offset = -s $node->logfile;

	# Connect at 3.3, send M -> expect ErrorResponse
	my $conn = open_33_conn();

	my $body = trace_context_body($TRACEPARENT, '');
	send_msg($conn, 'M', $body);

	# Send a Sync so we get a response; ERROR not FATAL means we get Z
	send_msg($conn, 'S', '');

	my @errors = drain_until_rfq($conn);
	ok(scalar @errors > 0, 'M with kill-switch off produces ErrorResponse');

	# Verify it's ERRCODE_PROTOCOL_VIOLATION (code '08P01' or class '08')
	my $got_protocol_violation = grep {
		/C08P01/
	} @errors;
	ok($got_protocol_violation,
		'ErrorResponse has ERRCODE_PROTOCOL_VIOLATION (08P01)');

	$conn->close();
}

# Re-enable
$node->append_conf('postgresql.conf', "trace_context_enabled = on\n");
$node->reload;

$node->stop;
done_testing();
