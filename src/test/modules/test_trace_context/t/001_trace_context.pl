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
#   * 'M' mid-COPY-in -> ErrorResponse, session survives (acceptance-state guard)
#   * 'M' mid-walsender-streaming -> ErrorResponse (acceptance-state guard)
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
# allows_streaming => "logical" sets wal_level=logical, max_wal_senders=10,
# max_replication_slots=10, and adds the replication pg_hba.conf line.  We
# need this for the walsender acceptance-state test (Test 5).
$node->init(allows_streaming => "logical");
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

# ----------------------------------------------------------------------
# Test 4: 'M' mid-COPY-in -> ErrorResponse, session survives
#
# Acceptance-state guard: the backend is inside CopyGetData when COPY FROM
# STDIN is active.  CopyGetData has its own message loop that rejects any
# unexpected message type (including 'M') with ERRCODE_PROTOCOL_VIOLATION.
# The error must be ERROR (not FATAL) so the session survives.
# ----------------------------------------------------------------------

# Create a table for COPY.
$node->safe_psql('postgres', 'CREATE TABLE _tc_copy_test (v int)');

{
	my $conn = open_33_conn();

	# Start COPY FROM STDIN.
	send_msg($conn, 'Q', "COPY _tc_copy_test FROM STDIN;\x00");

	# Drain until we see the CopyInResponse ('G'), which means the backend is
	# now inside COPY-in state.  Also drain any prior CommandComplete / RFQ.
	my $saw_copy_in = 0;
	my $deadline    = time() + 10;
	while (time() < $deadline)
	{
		my ($type, $body) = read_msg($conn);
		die "EOF waiting for CopyInResponse" unless defined $type;
		if ($type eq 'G')
		{
			$saw_copy_in = 1;
			last;
		}
		# If we got ReadyForQuery, COPY errored before starting; bail.
		last if $type eq 'Z';
	}
	ok($saw_copy_in, 'server sent CopyInResponse (entered COPY-in state)');

	if ($saw_copy_in)
	{
		# Now send 'M' mid-COPY-in instead of CopyData/CopyDone.
		my $body = trace_context_body($TRACEPARENT, '');
		send_msg($conn, 'M', $body);

		# The server should reject 'M' with an ErrorResponse, then send RFQ.
		my @errors = drain_until_rfq($conn);

		ok(scalar @errors > 0,
			"'M' mid-COPY-in produces ErrorResponse (not silently accepted)");

		my $got_protocol_violation = grep { /C08P01/ } @errors;
		ok($got_protocol_violation,
			"ErrorResponse has ERRCODE_PROTOCOL_VIOLATION (08P01)");

		# Verify the session survived: send a simple query and get a result.
		send_msg($conn, 'Q', "SELECT 42;\x00");
		my $survived   = 0;
		my $inner_dead = time() + 5;
		while (time() < $inner_dead)
		{
			my ($type2, $body2) = read_msg($conn);
			last unless defined $type2;
			if ($type2 eq 'Z')
			{
				$survived = 1;
				last;
			}
		}
		ok($survived,
			"session survives after 'M' mid-COPY-in (ERROR, not FATAL)");
	}

	$conn->close();
}

# ----------------------------------------------------------------------
# Test 5: 'M' mid-walsender-streaming -> ErrorResponse
#
# Acceptance-state guard (walsender): during streaming replication, the
# backend is inside WalSndLoop/ProcessRepliesIfAny.  That loop now has an
# explicit case for PqMsg_TraceContext that rejects it with
# ERRCODE_PROTOCOL_VIOLATION and ERROR (not FATAL), allowing the TCP
# connection to survive via the sigsetjmp recovery in PostgresMain.
#
# We use physical streaming replication (START_REPLICATION 0/0) to avoid
# needing a logical decoding plugin; any 3.3 replication connection will do.
# The key is entering the CopyBoth streaming state and sending 'M' there.
# ----------------------------------------------------------------------

SKIP:
{
	skip "raw_connect not available; skipping walsender streaming test", 2
	  unless $node->raw_connect_works();

	# Open a physical replication connection at protocol 3.3.
	my $repl_sock = $node->raw_connect();

	# StartupMessage with protocol 3.3 and replication=yes (physical).
	my $startup =
	  pack('nn', 3, 3)
	  . "user\x00"
	  . $ENV{USER}
	  . "\x00database\x00postgres\x00replication\x00yes\x00\x00";
	$repl_sock->send(pack('N', length($startup) + 4) . $startup)
	  or die "send replication startup: $!";

	# Drain until ReadyForQuery.
	my $repl_ready = 0;
	while (1)
	{
		my ($type, $body) = read_msg($repl_sock);
		unless (defined $type)
		{
			diag "EOF during replication startup";
			last;
		}
		$repl_ready = 1, last if $type eq 'Z';
		# ErrorResponse during startup -> skip
		if ($type eq 'E') { diag "Error during replication startup: $body"; last; }
	}

	skip "replication connection not ready; skipping walsender test", 2
	  unless $repl_ready;

	# Identify the current WAL insert LSN so we can start streaming from there.
	# IDENTIFY_SYSTEM returns a single row with fields: systemid, timeline,
	# xlogpos, dbname.
	send_msg($repl_sock, 'Q', "IDENTIFY_SYSTEM;\x00");

	my ($xlogpos, $timeline) = (undef, undef);
	while (1)
	{
		my ($type, $body) = read_msg($repl_sock);
		last unless defined $type;
		if ($type eq 'D')
		{
			# DataRow: parse the columns.
			# Row data: Int16 numcols, then per-col: Int32 len, bytes
			my $off   = 0;
			my $ncols = unpack('n', substr($body, $off, 2));
			$off += 2;
			my @cols;
			for (1 .. $ncols)
			{
				my $collen = unpack('N', substr($body, $off, 4));
				$off += 4;
				if ($collen == 0xFFFFFFFF)
				{
					push @cols, undef;
				}
				else
				{
					push @cols, substr($body, $off, $collen);
					$off += $collen;
				}
			}
			# IDENTIFY_SYSTEM: systemid, timeline, xlogpos, dbname
			$timeline = $cols[1] if @cols >= 2;
			$xlogpos  = $cols[2] if @cols >= 3;
		}
		last if $type eq 'Z';
	}

	if (!defined $xlogpos)
	{
		diag "could not get xlogpos from IDENTIFY_SYSTEM";
		skip "IDENTIFY_SYSTEM failed; skipping walsender streaming test", 2;
	}

	$timeline //= '1';

	# Start physical streaming replication from the current WAL position.
	send_msg($repl_sock, 'Q',
		"START_REPLICATION $xlogpos;\x00");

	# Wait for CopyBothResponse ('W') which signals we are in streaming mode.
	my $saw_copyboth = 0;
	while (1)
	{
		my ($type, $body) = read_msg($repl_sock);
		unless (defined $type)
		{
			diag "EOF waiting for CopyBothResponse";
			last;
		}
		if ($type eq 'W') { $saw_copyboth = 1; last; }
		last if $type eq 'Z' || $type eq 'E';
	}

	skip "CopyBothResponse not received; skipping streaming 'M' test", 2
	  unless $saw_copyboth;

	# We are now inside WalSndLoop / streaming state.  Send 'M' — the
	# walsender's ProcessRepliesIfAny loop will see it via pq_getbyte_if_available
	# on the next iteration and reject it with ERRCODE_PROTOCOL_VIOLATION.
	# The server may also send keepalive CopyData messages; we drain
	# those after the ErrorResponse.
	my $m_body = trace_context_body($TRACEPARENT, '');
	send_msg($repl_sock, 'M', $m_body);

	# Drain until ErrorResponse or ReadyForQuery.  The server may send
	# CopyData keepalives before it sees the 'M' and errors; skip them.
	my @repl_errors;
	while (1)
	{
		my ($type, $body) = read_msg($repl_sock);
		last unless defined $type;
		push @repl_errors, $body if $type eq 'E';
		last if $type eq 'Z';
		# Skip CopyData (keepalives) while waiting for the error response.
		next if $type eq 'd';
	}

	ok(scalar @repl_errors > 0,
		"'M' mid-walsender-streaming produces ErrorResponse");

	my $got_pv = grep { /C08P01/ } @repl_errors;
	ok($got_pv, "walsender ErrorResponse has ERRCODE_PROTOCOL_VIOLATION");

	$repl_sock->close();
}

$node->stop;
done_testing();
