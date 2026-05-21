# Copyright (c) 2026, PostgreSQL Global Development Group
#
# End-to-end test for the per-message protocol headers ('M') mechanism.
#
# Speaks the v3 wire protocol on a raw socket: performs a StartupMessage
# handshake that opts into the feature via _pq_.headers=1, sends a few
# RequestHeaders ('M') messages interleaved with Query and BEGIN/COMMIT,
# and verifies the test handler's log output to confirm:
#
#   * the _pq_.headers option is accepted (no NegotiateProtocolVersion);
#   * statement-scope effects clear at the next ReadyForQuery;
#   * transaction-scope effects persist across statements within an
#	 explicit transaction and clear only at COMMIT.
#
# The test handler is provided by the loadable module
# test_protocol_headers, which registers handlers for prefixes
# "test_stmt.", "test_tx.", "test_sess." and logs every set/clear event.

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
shared_preload_libraries = 'test_protocol_headers'
log_min_messages = log
log_connections = 'receipt,authentication,authorization'
EOCONF
$node->start;

if (!$node->raw_connect_works())
{
	plan skip_all => "this test requires working raw_connect()";
}

# ----------------------------------------------------------------------
# Raw-protocol helpers
# ----------------------------------------------------------------------

# Send a v3 StartupMessage with the given key/value pairs.  The protocol
# version is fixed at 3.0.
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

# Send a typed protocol message (1-byte type, 4-byte length-incl-self,
# then $body).
sub send_msg
{
	my ($sock, $type, $body) = @_;
	$body = '' unless defined $body;
	$sock->send($type . pack('N', length($body) + 4) . $body)
	  or die "send_msg: $!";
}

# Read exactly $n bytes (handles short reads).
sub recv_exact
{
	my ($sock, $n) = @_;
	my $buf = '';
	while (length($buf) < $n)
	{
		my $chunk = '';
		my $got = $sock->recv($chunk, $n - length($buf));
		die "recv_exact: $!" unless defined $got;
		return undef if length($chunk) == 0;	  # EOF
		$buf .= $chunk;
	}
	return $buf;
}

# Read one typed protocol message; returns ($type, $body) or undef at EOF.
sub recv_msg
{
	my ($sock) = @_;
	my $hdr = recv_exact($sock, 5);
	return undef unless defined $hdr;
	my ($type, $len) = unpack('A1 N', $hdr);
	my $body = ($len > 4) ? recv_exact($sock, $len - 4) : '';
	return ($type, $body);
}

# Drain messages until ReadyForQuery ('Z') arrives.  Returns the list of
# message-type bytes seen, in order.
sub drain_to_rfq
{
	my ($sock) = @_;
	my @types;
	while (1)
	{
		my ($type, $body) = recv_msg($sock);
		die "connection closed before ReadyForQuery" unless defined $type;
		push @types, $type;
		last if $type eq 'Z';
	}
	return @types;
}

# Build the body of a RequestHeaders ('M') message from a list of
# key=>value pairs.
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

# ----------------------------------------------------------------------
# Test 1: _pq_.headers=1 negotiates cleanly; no NegotiateProtocolVersion.
# ----------------------------------------------------------------------

# The cluster's superuser is the OS user that ran initdb.
my $superuser = getpwuid($<);

my $sock = $node->raw_connect();
send_startup(
	$sock,
	user => $superuser,
	database => 'postgres',
	'_pq_.headers' => '1');

# Capture full messages so the protocol_features ParameterStatus body
# can be inspected (drain_to_rfq returned types-only previously; widen it).
my @startup_msgs;
while (1)
{
	my ($type, $body) = recv_msg($sock);
	die "connection closed before ReadyForQuery" unless defined $type;
	push @startup_msgs, [ $type, $body ];
	last if $type eq 'Z';
}
my @startup_types = map { $_->[0] } @startup_msgs;

ok(!(grep { $_ eq 'v' } @startup_types),
	'_pq_.headers=1 is accepted (no NegotiateProtocolVersion)');
ok((grep { $_ eq 'R' } @startup_types),
	'authentication message received');
ok((grep { $_ eq 'Z' } @startup_types),
	'ReadyForQuery reached');

# Affirmative acknowledgement: the server must emit a ParameterStatus
# carrying ("protocol_features", value-containing-"headers").  Without
# this, a proxy that silently strips _pq_.headers would let the absence
# of NegotiateProtocolVersion masquerade as success.
my $features_seen = 0;
for my $m (@startup_msgs)
{
	next unless $m->[0] eq 'S';
	my @parts = split /\0/, $m->[1];	  # key, value, trailing-NUL artifact
	next unless @parts >= 2 && $parts[0] eq 'protocol_features';
	$features_seen = 1
	  if grep { $_ eq 'headers' } split /,/, $parts[1];
	last;
}
ok($features_seen,
	'ParameterStatus protocol_features contains "headers"');

# ----------------------------------------------------------------------
# Test 2: statement-scope dispatch and clear.
# ----------------------------------------------------------------------

my $log_offset = -s $node->logfile;

send_msg($sock, 'M', headers_body('test_stmt.alpha' => 'one'));
send_msg($sock, 'Q', "SELECT 1\0");
drain_to_rfq($sock);

# wait_for_log polls the log file until the regex matches.
$node->wait_for_log(
	qr/test_protocol_headers: set scope=statement key=test_stmt\.alpha value=one/,
	$log_offset);
$node->wait_for_log(qr/test_protocol_headers: clear scope=statement/,
	$log_offset);
pass('statement-scope set and clear both observed');

# ----------------------------------------------------------------------
# Test 3: transaction-scope effects persist across statements; clear
# fires once at COMMIT.
# ----------------------------------------------------------------------

$log_offset = -s $node->logfile;

send_msg($sock, 'Q', "BEGIN\0");
drain_to_rfq($sock);

send_msg($sock, 'M', headers_body('test_tx.beta' => 'two'));
send_msg($sock, 'Q', "SELECT 2\0");
drain_to_rfq($sock);

send_msg($sock, 'Q', "SELECT 3\0");
drain_to_rfq($sock);

# Before COMMIT: the set should have fired, but not the transaction-scope
# clear.
my $log_mid = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);
like(
	$log_mid,
	qr/test_protocol_headers: set scope=transaction key=test_tx\.beta value=two/,
	'transaction-scope set observed after first statement');
unlike($log_mid, qr/test_protocol_headers: clear scope=transaction/,
	'transaction-scope clear has NOT yet fired before COMMIT');

send_msg($sock, 'Q', "COMMIT\0");
drain_to_rfq($sock);

$node->wait_for_log(qr/test_protocol_headers: clear scope=transaction/,
	$log_offset);
pass('transaction-scope clear fired at COMMIT');

# Set must have appeared exactly once even though three statements ran
# under the transaction --- handlers receive each entry once, not once
# per statement.
my $log_full =
  PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);
my @sets =
  ($log_full =~ /test_protocol_headers: set scope=transaction/g);
is(scalar(@sets), 1,
	'transaction-scope set fired exactly once for one M message');

# ----------------------------------------------------------------------
# Tidy up.
# ----------------------------------------------------------------------

send_msg($sock, 'X');	# Terminate
$sock->close();

$node->stop;
done_testing();
