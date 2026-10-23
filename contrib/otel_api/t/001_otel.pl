# Copyright (c) 2026, PostgreSQL Global Development Group
#
# End-to-end test for the otel_api module: register the trace-context
# handler, parse a W3C traceparent received from the client via 'M',
# and verify until-RFQ lifecycle semantics.
#
# The test speaks the v3.3 wire protocol on a raw socket so it can send
# TraceContext ('M') messages directly.
#
# Wire format (protocol 3.3):
#   'M' | Int32 length | String traceparent | String tracestate
# (Two NUL-terminated strings; no entry count.)
#
# Trace context appears in the server log via a CONTEXT line appended
# by otel_postgres_tracing's emit_log_hook:
#   CONTEXT: trace_id=<trace_id> span_id=<span_id> trace_flags=<flags>

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# A representative W3C TraceContext to use throughout the test.
my $TRACE_ID    = 'aabbccddeeff00112233445566778899';
my $SPAN_ID     = '0011223344556677';
my $FLAGS       = '01';
my $TRACEPARENT = "00-$TRACE_ID-$SPAN_ID-$FLAGS";

# ----------------------------------------------------------------------
# Cluster setup
# ----------------------------------------------------------------------

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel_api,otel_postgres_tracing'
log_statement = 'all'
log_min_messages = log
log_line_prefix = '%m [%p] %A %q%a '
EOCONF
$node->start;

# Install the SQL surface (otel_current_traceparent).
$node->safe_psql('postgres', 'CREATE EXTENSION otel_api');

if (!$node->raw_connect_works())
{
	plan skip_all => "this test requires working raw_connect()";
}

# ----------------------------------------------------------------------
# Raw-protocol helpers
# ----------------------------------------------------------------------

sub send_startup
{
	my ($sock, @kv) = @_;
	my $body = pack('N', 0x00030003);    # protocol 3.3
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
		my $got   = $sock->recv($chunk, $n - length($buf));
		die "recv_exact: $!" unless defined $got;
		return undef if length($chunk) == 0;    # EOF
		$buf .= $chunk;
	}
	return $buf;
}

sub read_msg
{
	my ($sock) = @_;
	my $hdr = recv_exact($sock, 5);
	return (undef, undef) unless defined $hdr;
	my $type = substr($hdr, 0, 1);
	my $len  = unpack('N', substr($hdr, 1, 4));
	my $body = ($len > 4) ? recv_exact($sock, $len - 4) : '';
	return ($type, $body);
}

sub drain_to_rfq
{
	my ($sock) = @_;
	my @msgs;
	while (1)
	{
		my ($type, $body) = read_msg($sock);
		die "EOF during drain" unless defined $type;
		push @msgs, [$type, $body];
		last if $type eq 'Z';
	}
	return @msgs;
}

# Build a TraceContext ('M') wire body: two NUL-terminated strings.
sub trace_context_body
{
	my ($tp, $ts) = @_;
	$ts //= '';
	return "$tp\x00$ts\x00";
}

# Extract the first column of the first DataRow from a list of messages.
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
		return undef if $len == 0xFFFFFFFF;    # SQL NULL
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
# Handshake with protocol 3.3 (TraceContext available by default)
# ----------------------------------------------------------------------

my $superuser = getpwuid($<);
my $sock      = $node->raw_connect();
send_startup(
	$sock,
	user     => $superuser,
	database => 'postgres');

my @startup = drain_to_rfq($sock);
ok(!(grep { $_->[0] eq 'v' } @startup),
	'protocol 3.3 handshake accepted (no NegotiateProtocolVersion)');

# ----------------------------------------------------------------------
# Test 1: M before Q -> trace_id appears in CONTEXT log line.
# The emit_log_hook appends a CONTEXT line with trace_id/span_id.
# until-RFQ: context applies for that one query, cleared at its RFQ.
# ----------------------------------------------------------------------

my $log_offset = -s $node->logfile;

send_msg($sock, 'M', trace_context_body($TRACEPARENT, ''));
run_query($sock, 'SELECT 1');

# Trace context surfaces in the log either as errannot annotations
# (rendered by %A as trace_id="..." when the server has errannot) or,
# on a server without errannot, as a "CONTEXT: trace_id=..." fallback
# line.  Match either form, and check the fields independently since the
# %A and CONTEXT renderings order them differently.
$node->wait_for_log(qr/trace_id="?$TRACE_ID/, $log_offset);
my $sel1_log =
  PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);
like($sel1_log, qr/trace_id="?$TRACE_ID/,
	'SELECT 1 log has trace_id from TraceContext M');
like($sel1_log, qr/span_id="?$SPAN_ID/,
	'SELECT 1 log has span_id from TraceContext M');

# ----------------------------------------------------------------------
# Test 2: SQL-level introspection via otel_current_traceparent().
# Send a fresh M before this query (prior RFQ cleared the previous context).
# ----------------------------------------------------------------------

send_msg($sock, 'M', trace_context_body($TRACEPARENT, ''));
my @msgs = run_query($sock, 'SELECT otel_current_traceparent()');
is(first_value(@msgs), $TRACEPARENT,
	'otel_current_traceparent() returns the active traceparent');

# ----------------------------------------------------------------------
# Test 3: until-RFQ semantics: context is cleared at RFQ.
# After a query, a subsequent query without M must NOT carry the context.
# ----------------------------------------------------------------------

$log_offset = -s $node->logfile;

send_msg($sock, 'M', trace_context_body($TRACEPARENT, ''));
run_query($sock, 'SELECT 2');    # carries the context

# Now a query WITHOUT M: context should be cleared
$log_offset = -s $node->logfile;
run_query($sock, 'SELECT 3');    # must NOT carry the context
my $post_rfq = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);

# The SELECT 3 log line must NOT contain our trace_id
unlike(
	$post_rfq,
	qr/trace_id="?$TRACE_ID/,
	'SELECT 3 without preceding M has no trace context (cleared at RFQ)');

# ----------------------------------------------------------------------
# Test 4: a malformed traceparent is silently ignored (advisory);
# the operation proceeds untagged.
# ----------------------------------------------------------------------

$log_offset = -s $node->logfile;
send_msg($sock, 'M', trace_context_body('not-a-valid-traceparent', ''));
run_query($sock, 'SELECT 4');
my $malformed = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);
unlike(
	$malformed,
	qr/trace_id=/,
	'malformed traceparent is ignored; operation proceeds untagged');

# ----------------------------------------------------------------------
# Test 5: SET otel_api.traceparent survives an RFQ.
# An M-installed context is cleared at RFQ; a SET-installed context is
# NOT cleared (it lives at the GUC-machinery scope the user chose).
# ----------------------------------------------------------------------

run_query($sock,
	"SET otel_api.traceparent = '$TRACEPARENT'");    # session scope

# Verify context is present
$log_offset = -s $node->logfile;
run_query($sock, 'SELECT 5');
my $after_set = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);
like(
	$after_set,
	qr/trace_id="?$TRACE_ID/,
	'SET-installed context present after SET');

# The context should survive another RFQ (it was installed via SET, not M)
$log_offset = -s $node->logfile;
run_query($sock, 'SELECT 6');
my $still_set = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);
like(
	$still_set,
	qr/trace_id="?$TRACE_ID/,
	'SET-installed context survives an RFQ (not cleared by M-context clear)');

# Clean up the SET
run_query($sock, 'RESET otel_api.traceparent');

# ----------------------------------------------------------------------
# Test 6: parallel-worker propagation.
# Trace context is stored in GUCs; GUCs propagate to parallel workers.
# With debug_parallel_query=regress, otel_current_traceparent() runs in
# a worker rather than the leader.
# ----------------------------------------------------------------------

run_query($sock, 'SET debug_parallel_query = regress');

send_msg($sock, 'M', trace_context_body($TRACEPARENT, ''));
my @parallel_msgs = run_query($sock, 'SELECT otel_current_traceparent()');
is(first_value(@parallel_msgs), $TRACEPARENT,
	'parallel worker sees the trace context (GUC propagated, assign_hook fired)');

run_query($sock, 'SET debug_parallel_query = off');

# ----------------------------------------------------------------------
# Test 7: W3C forward-compat --- higher-version traceparents (01..fe) are
# parsed for their known 55-char prefix; "ff" is W3C-reserved invalid.
# ----------------------------------------------------------------------

# Future version with no trailing fields.
send_msg($sock, 'M',
	trace_context_body("01-$TRACE_ID-$SPAN_ID-$FLAGS", ''));
my @v01_msgs = run_query($sock, 'SELECT otel_current_traceparent()');
is(first_value(@v01_msgs), "00-$TRACE_ID-$SPAN_ID-$FLAGS",
	'v01 traceparent parsed for known prefix; version pin "00" in output');

# Future version with trailing additional fields.
send_msg($sock, 'M',
	trace_context_body("01-$TRACE_ID-$SPAN_ID-$FLAGS-future-field-data", ''));
my @v01ext_msgs = run_query($sock, 'SELECT otel_current_traceparent()');
is(first_value(@v01ext_msgs), "00-$TRACE_ID-$SPAN_ID-$FLAGS",
	'v01 traceparent with trailing fields is accepted; trailing data ignored');

# "ff" is W3C-reserved invalid --- rejected, no trace context applied.
$log_offset = -s $node->logfile;
send_msg($sock, 'M', trace_context_body("ff-$TRACE_ID-$SPAN_ID-$FLAGS", ''));
run_query($sock, 'SELECT 7');
my $ff_log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);
unlike(
	$ff_log,
	qr/trace_id=$TRACE_ID/,
	'version "ff" traceparent is rejected; no trace context applied');

# ----------------------------------------------------------------------
# Tidy up.
# ----------------------------------------------------------------------

send_msg($sock, 'X');
$sock->close();

$node->stop;
done_testing();
