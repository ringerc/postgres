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
#   * transaction-scope effects persist across statements within an
#	 explicit transaction and clear only at COMMIT.
#
# The test handler is provided by the loadable module
# test_protocol_headers, which registers a single handler for the "test."
# prefix and internally demultiplexes to per-key lifetimes:
#
#   test.txn_scope	-> transaction-scope clear via RegisterXactCallback
#   test.sess_scope	-> session-scope clear via on_proc_exit
#
# Statement-scope support (test.stmt_scope, cleared via
# pre_ready_for_query_hook) is added in a follow-up commit; the
# corresponding TAP coverage is added with it.
#
# Every set/clear event is logged so this test can assert on timing by
# reading the server log.

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

# Open a fresh raw connection that has negotiated _pq_.headers=1, and
# discard the startup-burst messages.  Returns the connected socket.
# Used by the limit-exceed tests below; each of those triggers a FATAL
# ereport in the backend, so each test wants its own disposable conn.
sub open_negotiated_conn
{
	my $superuser = getpwuid($<);
	my $sock = $node->raw_connect();
	send_startup(
		$sock,
		user => $superuser,
		database => 'postgres',
		'_pq_.headers' => '1');
	while (1)
	{
		my ($type, $body) = recv_msg($sock);
		die "EOF before ReadyForQuery during negotiation"
		  unless defined $type;
		last if $type eq 'Z';
	}
	return $sock;
}

# Read messages until an ErrorResponse arrives or EOF.  NoticeResponse
# ('N') and NotificationResponse ('A') are silently skipped.  Returns
# the ErrorResponse body, or undef if the connection closed without
# any ErrorResponse.
sub recv_until_error
{
	my ($sock) = @_;
	while (1)
	{
		my ($type, $body) = recv_msg($sock);
		return undef unless defined $type;
		return $body if $type eq 'E';
	}
}

# Parse an ErrorResponse body into a hash keyed by single-byte field
# tag.  Tag 'M' is the human-readable message, 'C' is the SQLSTATE,
# 'S' is the severity, etc.  See
# https://www.postgresql.org/docs/current/protocol-error-fields.html .
sub parse_error_fields
{
	my ($body) = @_;
	my %fields;
	my $pos = 0;
	while ($pos < length($body))
	{
		my $tag = substr($body, $pos, 1);
		$pos++;
		last if $tag eq "\0";
		my $end = index($body, "\0", $pos);
		last if $end < 0;
		$fields{$tag} = substr($body, $pos, $end - $pos);
		$pos = $end + 1;
	}
	return %fields;
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
# Test 2: statement-scope dispatch and clear is covered by the
# follow-up commit that wires test.stmt_scope on top of
# pre_ready_for_query_hook.
# ----------------------------------------------------------------------

# ----------------------------------------------------------------------
# Test 3: transaction-scope effects persist across statements; clear
# fires once at COMMIT.
# ----------------------------------------------------------------------

my $log_offset = -s $node->logfile;

send_msg($sock, 'Q', "BEGIN\0");
drain_to_rfq($sock);

send_msg($sock, 'M', headers_body('test.txn_scope' => 'two'));
send_msg($sock, 'Q', "SELECT 2\0");
drain_to_rfq($sock);

send_msg($sock, 'Q', "SELECT 3\0");
drain_to_rfq($sock);

# Before COMMIT: the set should have fired, but not the transaction-scope
# clear.
my $log_mid = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);
like(
	$log_mid,
	qr/test_protocol_headers: set scope=transaction key=test\.txn_scope value=two/,
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
# Test 3b: subtransactions are deliberately NOT instrumented by the
# test module.
#
# A transaction-scope header set inside a SAVEPOINT block survives a
# ROLLBACK TO that savepoint --- the handler's set callback fires
# (because the M message arrived) but no clear fires for the subxact
# abort.  The single clear arrives only when the outer transaction
# ends.  This is a property of the test_protocol_headers extension
# (which deliberately installs no SubXactCallback), not of the core
# dispatcher --- a different extension is free to choose different
# semantics.
# ----------------------------------------------------------------------

$log_offset = -s $node->logfile;

send_msg($sock, 'Q', "BEGIN\0");
drain_to_rfq($sock);

send_msg($sock, 'M', headers_body('test.txn_scope' => 'outer'));
send_msg($sock, 'Q', "SELECT 'before-savepoint'\0");
drain_to_rfq($sock);

send_msg($sock, 'Q', "SAVEPOINT s1\0");
drain_to_rfq($sock);

send_msg($sock, 'M', headers_body('test.txn_scope' => 'inner'));
send_msg($sock, 'Q', "SELECT 'inside-savepoint'\0");
drain_to_rfq($sock);

send_msg($sock, 'Q', "ROLLBACK TO s1\0");
drain_to_rfq($sock);

# After ROLLBACK TO: BOTH set events ("outer" and "inner") should
# already have been logged, and NO transaction-scope clear should
# have fired yet.
my $log_after_rollback =
  PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);
like(
	$log_after_rollback,
	qr/test_protocol_headers: set scope=transaction key=test\.txn_scope value=outer/,
	'subxact: outer set logged');
like(
	$log_after_rollback,
	qr/test_protocol_headers: set scope=transaction key=test\.txn_scope value=inner/,
	'subxact: inner set logged before ROLLBACK TO');
unlike(
	$log_after_rollback,
	qr/test_protocol_headers: clear scope=transaction/,
	'subxact: clear has NOT fired despite ROLLBACK TO --- the extension does not instrument subxacts');

send_msg($sock, 'Q', "COMMIT\0");
drain_to_rfq($sock);

$node->wait_for_log(qr/test_protocol_headers: clear scope=transaction/,
	$log_offset);
pass('subxact: transaction-scope clear fires once at outer COMMIT');

# Verify the clear fired exactly once in the whole sequence --- no
# bonus clears from subxact abort.
my $log_final =
  PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);
my @clears =
  ($log_final =~ /test_protocol_headers: clear scope=transaction/g);
is(scalar(@clears), 1,
	'subxact: exactly one transaction-scope clear in the entire flow');

# ----------------------------------------------------------------------
# Test 3c: an unknown key under the registered prefix lands at the
# extension's handler and produces a WARNING --- it is NOT silently
# dropped by the dispatcher.  Confirms that prefix-only registration
# combined with internal demux is reachable end-to-end.
# ----------------------------------------------------------------------

$log_offset = -s $node->logfile;

send_msg($sock, 'M', headers_body('test.unknown_key' => 'whatever'));
send_msg($sock, 'Q', "SELECT 1\0");
drain_to_rfq($sock);

$node->wait_for_log(
	qr/test_protocol_headers: unknown key "test\.unknown_key" under registered prefix/,
	$log_offset);
pass('unknown key under registered prefix reaches the handler as a WARNING');

# ----------------------------------------------------------------------
# Test 3d: atomic frame parse --- a malformed frame is rejected before
# any handler runs.  The server parses every entry into a temporary
# list, calls pq_getmsgend(), and only then dispatches set_cb.  Trailing
# garbage after the declared entries makes pq_getmsgend() raise ERROR,
# which must arrive WITHOUT half-applying the valid leading entry.
#
# This is a recoverable ERROR (not FATAL), so the connection survives
# and we continue using $sock.
# ----------------------------------------------------------------------

$log_offset = -s $node->logfile;

{
	# Build an M body with count=1 and one valid txn_scope entry,
	# then append a stray NUL byte so pq_getmsgend sees leftover input.
	my $body = headers_body('test.txn_scope' => 'must-not-apply');
	$body .= "\0";

	send_msg($sock, 'M', $body);

	# Server raises ERROR on the malformed frame.  Without an explicit
	# Sync (we sent a 'Q' Query message? no --- only M).  In simple-Q
	# pathways, the error propagates to ErrorResponse + ReadyForQuery
	# at the next protocol boundary.  Send a benign Query to drive the
	# server through error recovery and back to idle, then verify the
	# log slice in one go.
	send_msg($sock, 'Q', "SELECT 1\0");
	drain_to_rfq($sock);

	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);
	unlike(
		$log,
		qr/test_protocol_headers: set scope=transaction key=test\.txn_scope value=must-not-apply/,
		'atomic parse: malformed M does not half-fire set_cb on the leading entry');
}

# ----------------------------------------------------------------------
# Test 3e: deferred apply / failure-atomicity.
#
# A handler that raises ERROR from set_cb must fail the SQL operation
# the headers were intended to prefix --- not produce a standalone
# error that lets the following Query run anyway.  We use the
# test.fail_on_set key which the test module's set_cb always raises
# ERROR on.
#
# Sequence:
#   1. Send M test.fail_on_set=boom.  This does NOT raise --- dispatch
#      is deferred.  No response from the server.
#   2. Send Q "SELECT 1".  ApplyPendingRequestHeaders fires at the top
#      of the Query path, the handler raises ERROR, the SELECT does
#      not run.
#   3. Read messages until ErrorResponse + ReadyForQuery.  The error
#      message must mention the handler.
#   4. Confirm via log inspection that the SELECT did NOT run (no
#      "statement: SELECT 1" log line in the slice).
#   5. Confirm the connection survives: send a clean Q and drain to
#      RFQ.  The next M-less Query must succeed.
# ----------------------------------------------------------------------

$log_offset = -s $node->logfile;

{
	send_msg($sock, 'M', headers_body('test.fail_on_set' => 'boom'));
	send_msg($sock, 'Q', "SELECT 1\0");

	my $err = recv_until_error($sock);
	ok(defined $err,
		'deferred apply: failing handler produces ErrorResponse on the Query');
	if (defined $err)
	{
		my %f = parse_error_fields($err);
		like($f{M} // '',
			qr/handler asked to fail \(value=boom\)/,
			'error message identifies the failing handler');
	}

	# After ERROR the server still emits ReadyForQuery for this Q.
	drain_to_rfq($sock);

	# Server log: the handler logged its receipt (set scope=fail ...)
	# but the SELECT 1 must not appear (it never ran).
	my $log =
	  PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);
	like(
		$log,
		qr/test_protocol_headers: set scope=fail key=test\.fail_on_set value=boom/,
		'failing handler was reached before raising');
	unlike(
		$log,
		qr/statement: SELECT 1\b/,
		'deferred apply: SELECT 1 was NOT executed because the handler failed');

	# Connection survives.  Drive a clean Q to confirm.
	send_msg($sock, 'Q', "SELECT 'after-fail'\0");
	drain_to_rfq($sock);
	my $log_after =
	  PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);
	like(
		$log_after,
		qr/statement: SELECT 'after-fail'/,
		'connection survives a failing handler');
}

# ----------------------------------------------------------------------
# Test 3f: multiple M messages before a single op are applied in
# receipt order.  Two M frames before one Q; the test module records
# both set events in order.  No special merge or replace logic in the
# dispatcher --- handlers receive each (key, value) once in the order
# the client sent them.
# ----------------------------------------------------------------------

$log_offset = -s $node->logfile;

{
	send_msg($sock, 'Q', "BEGIN\0");
	drain_to_rfq($sock);

	send_msg($sock, 'M', headers_body('test.txn_scope' => 'first'));
	send_msg($sock, 'M', headers_body('test.txn_scope' => 'second'));
	send_msg($sock, 'Q', "SELECT 1\0");
	drain_to_rfq($sock);

	# Both set events must appear in the order they were sent.
	my $log =
	  PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);
	my $first_pos = index($log,
		"test_protocol_headers: set scope=transaction key=test.txn_scope value=first");
	my $second_pos = index($log,
		"test_protocol_headers: set scope=transaction key=test.txn_scope value=second");
	ok($first_pos >= 0,  'multi-M: first set logged');
	ok($second_pos >= 0, 'multi-M: second set logged');
	ok($first_pos < $second_pos,
		'multi-M: set events appear in receipt order');

	send_msg($sock, 'Q', "COMMIT\0");
	drain_to_rfq($sock);
}

# ----------------------------------------------------------------------
# Test 3g: an 'M' with no following operation has no observable effect.
#
# Under deferred apply, headers stashed by 'M' are dispatched only at
# the start of the next Q/P/B/E.  If the client sends M and then
# closes the connection (Terminate), nothing should reach the
# handler.  Use a fresh disposable connection so we can close without
# disturbing the long-lived $sock.
# ----------------------------------------------------------------------

{
	my $offset = -s $node->logfile;
	my $sock3g = open_negotiated_conn();

	send_msg($sock3g, 'M', headers_body('test.txn_scope' => 'dropped'));
	send_msg($sock3g, 'X');	  # Terminate
	$sock3g->close();

	# Give the backend a moment to exit and flush its log.  Use the
	# long-lived $sock to drive a marker query as a barrier.
	send_msg($sock, 'Q', "SELECT 'marker-3g'\0");
	drain_to_rfq($sock);

	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $offset);
	unlike(
		$log,
		qr/test_protocol_headers: set scope=transaction key=test\.txn_scope value=dropped/,
		'M with no following operation does not invoke set_cb');
}

# ----------------------------------------------------------------------
# Test 4: a single (key, value) entry exceeding max_protocol_header_size
# is FATAL.  The cap is per-entry, not per-message --- a message with
# many small entries is fine even if its total body is large, but one
# oversize entry is enough to be a protocol violation.
#
# Uses a fresh connection because the violation terminates the
# backend.  Verifies (a) the server sends an ErrorResponse that names
# the GUC, (b) the connection is closed afterwards, and (c) the
# server log records the rejection.
# ----------------------------------------------------------------------

{
	my $offset = -s $node->logfile;
	my $sock4 = open_negotiated_conn();

	# Default max_protocol_header_size is 4096 bytes per entry,
	# counting both NUL terminators.  Build one entry whose wire size
	# is just over the cap:  "x\0" (2) + "a"*4100 + "\0" (4101) = 4103.
	my $big_value = 'a' x 4100;
	send_msg($sock4, 'M', headers_body('x' => $big_value));

	my $err = recv_until_error($sock4);
	ok(defined $err,
		'oversize entry produces an ErrorResponse before connection close');
	if (defined $err)
	{
		my %f = parse_error_fields($err);
		like($f{M} // '',
			qr/RequestHeaders entry .* exceeds max_protocol_header_size/,
			'oversize-entry ErrorResponse names max_protocol_header_size');
	}

	# FATAL ereport closes the connection.  Next read must hit EOF.
	my ($type) = recv_msg($sock4);
	ok(!defined $type,
		'server closes the connection after oversize-entry FATAL');
	$sock4->close();

	$node->wait_for_log(
		qr/RequestHeaders entry .* exceeds max_protocol_header_size/,
		$offset);
	pass('server log records the oversize-entry rejection');
}

# ----------------------------------------------------------------------
# Test 5: M with entry count exceeding max_protocol_header_entries is
# FATAL.  Same shape as test 4.
# ----------------------------------------------------------------------

{
	my $offset = -s $node->logfile;
	my $sock5 = open_negotiated_conn();

	# Default max_protocol_header_entries is 64.  Send 65 tiny entries
	# (each well under the size cap so the entries-cap fires, not the
	# size-cap).  Total body is roughly 2 + 65 * 6 = 392 bytes.
	my @kv;
	for my $i (1 .. 65)
	{
		push @kv, "x.$i", "v";
	}
	send_msg($sock5, 'M', headers_body(@kv));

	my $err = recv_until_error($sock5);
	ok(defined $err,
		'too-many-entries M produces an ErrorResponse before connection close');
	if (defined $err)
	{
		my %f = parse_error_fields($err);
		like($f{M} // '',
			qr/RequestHeaders entry count exceeds max_protocol_header_entries/,
			'too-many-entries ErrorResponse names max_protocol_header_entries');
	}

	my ($type) = recv_msg($sock5);
	ok(!defined $type,
		'server closes the connection after too-many-entries FATAL');
	$sock5->close();

	$node->wait_for_log(
		qr/RequestHeaders entry count exceeds max_protocol_header_entries/,
		$offset);
	pass('server log records the too-many-entries rejection');
}

# ----------------------------------------------------------------------
# Tests 6 + 7: setting either cap GUC to 0 makes the server refuse to
# negotiate _pq_.headers at handshake time.  Justification: with the
# cap at zero, every 'M' message would FATAL anyway, so a no-go signal
# at handshake is friendlier than a connection-killing surprise on
# first use.  The opt-in is reported back through
# NegotiateProtocolVersion's unrecognised-option list, the same way
# the server already handles _pq_.* params it doesn't understand.
# ----------------------------------------------------------------------

# Helper: open a raw connection that *requests* _pq_.headers=1 but
# does NOT assume negotiation succeeded.  Returns the full list of
# message [type, body] pairs received during the startup burst.
sub open_and_capture_startup
{
	my $superuser = getpwuid($<);
	my $sk = $node->raw_connect();
	send_startup(
		$sk,
		user => $superuser,
		database => 'postgres',
		'_pq_.headers' => '1');
	my @msgs;
	while (1)
	{
		my ($type, $body) = recv_msg($sk);
		die "EOF before ReadyForQuery during startup capture"
		  unless defined $type;
		push @msgs, [ $type, $body ];
		last if $type eq 'Z';
	}
	return ($sk, @msgs);
}

# Helper: assert NegotiateProtocolVersion appears in @msgs and lists
# _pq_.headers among its unrecognised-options.  Returns the body of
# the NegotiateProtocolVersion message (or undef if not present).
sub find_negotiate_with_headers_rejected
{
	my (@msgs) = @_;
	for my $m (@msgs)
	{
		next unless $m->[0] eq 'v';
		my $b = $m->[1];
		# Layout: Int32 newest-supported-minor, Int32 n-unrecognised,
		# then n NUL-terminated strings.
		next if length($b) < 8;
		my ($minor, $n) = unpack('N N', $b);
		my $pos = 8;
		for (my $i = 0; $i < $n; $i++)
		{
			my $end = index($b, "\0", $pos);
			last if $end < 0;
			my $opt = substr($b, $pos, $end - $pos);
			return $b if $opt eq '_pq_.headers';
			$pos = $end + 1;
		}
	}
	return undef;
}

# Close the long-lived $sock --- the SIGHUP-reload below affects all
# backends, and the remaining tests each use a disposable connection.
send_msg($sock, 'X');
$sock->close();

# ----------------------------------------------------------------------
# Test 6: max_protocol_header_size = 0 -> negotiation refused.
# ----------------------------------------------------------------------

$node->append_conf('postgresql.conf', "max_protocol_header_size = 0\n");
$node->reload;

{
	my ($sock6, @msgs) = open_and_capture_startup();
	ok(defined find_negotiate_with_headers_rejected(@msgs),
		'max_protocol_header_size=0: server emits NegotiateProtocolVersion listing _pq_.headers as unrecognised');

	# protocol_features ParameterStatus must NOT advertise "headers"
	# either, since the feature was refused.
	my $features_advertised = 0;
	for my $m (@msgs)
	{
		next unless $m->[0] eq 'S';
		my @parts = split /\0/, $m->[1];
		next unless @parts >= 2 && $parts[0] eq 'protocol_features';
		$features_advertised = 1
		  if grep { $_ eq 'headers' } split /,/, $parts[1];
	}
	ok(!$features_advertised,
		'size=0: protocol_features ParameterStatus does not advertise "headers"');

	send_msg($sock6, 'X');
	$sock6->close();
}

$node->adjust_conf('postgresql.conf', 'max_protocol_header_size', undef);
$node->reload;

# ----------------------------------------------------------------------
# Test 7: max_protocol_header_entries = 0 -> negotiation refused.
# Same shape as Test 6.
# ----------------------------------------------------------------------

$node->append_conf('postgresql.conf', "max_protocol_header_entries = 0\n");
$node->reload;

{
	my ($sock7, @msgs) = open_and_capture_startup();
	ok(defined find_negotiate_with_headers_rejected(@msgs),
		'max_protocol_header_entries=0: server emits NegotiateProtocolVersion listing _pq_.headers as unrecognised');

	my $features_advertised = 0;
	for my $m (@msgs)
	{
		next unless $m->[0] eq 'S';
		my @parts = split /\0/, $m->[1];
		next unless @parts >= 2 && $parts[0] eq 'protocol_features';
		$features_advertised = 1
		  if grep { $_ eq 'headers' } split /,/, $parts[1];
	}
	ok(!$features_advertised,
		'entries=0: protocol_features ParameterStatus does not advertise "headers"');

	send_msg($sock7, 'X');
	$sock7->close();
}

# ----------------------------------------------------------------------
# Tidy up.
# ----------------------------------------------------------------------

$node->stop;
done_testing();
