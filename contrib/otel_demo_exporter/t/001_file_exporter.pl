# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Tests for the bare-minimum file exporter (contrib/otel_demo_exporter).
#
# Verifies that the rendezvous-variable API is enough to wire up an
# out-of-tree exporter and that trace context flows end-to-end from
# the client (via the otel.traceparent protocol header) through
# contrib/otel into the JSON-lines file written by this module.
# Also exercises the parallel-worker case: worker spans share the
# leader's trace_id and link back via parent_span_id, validating
# that the rendezvous-installed emit hook fires inside parallel
# workers and that contrib/otel's current_span_id GUC propagation
# reaches them.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Distinct trace ids per test section so we can grep spans apart
# from any incidental traffic written to the shared spans file.
my $TRACE_ID_SEQ    = 'aabbccddeeff00112233445566778899';
my $SPAN_ID_SEQ     = '0011223344556677';
my $TRACEPARENT_SEQ = "00-$TRACE_ID_SEQ-$SPAN_ID_SEQ-01";

my $TRACE_ID_PAR    = '1122334455667788991122334455aabb';
my $SPAN_ID_PAR     = 'cafef00ddeadbeef';
my $TRACEPARENT_PAR = "00-$TRACE_ID_PAR-$SPAN_ID_PAR-01";

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;

my $span_file = $node->data_dir . '/otel_spans.jsonl';

$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel,otel_postgres_tracing,otel_demo_exporter'
otel_demo_exporter.output_file = '$span_file'
log_min_messages = warning
log_statement = 'none'
# Ensure parallel-worker spawning is enabled for the parallel test.
max_parallel_workers_per_gather = 2
max_parallel_workers = 4
max_worker_processes = 8
EOCONF
$node->start;
$node->safe_psql('postgres',
	'CREATE EXTENSION otel; CREATE EXTENSION otel_demo_exporter');

if (!$node->raw_connect_works())
{
	plan skip_all => "this test requires working raw_connect()";
}

# ----- raw-protocol helpers -----

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
	while (1)
	{
		my ($type, $body) = recv_msg($sock);
		die "connection closed before ReadyForQuery" unless defined $type;
		last if $type eq 'Z';
	}
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

sub run_query
{
	my ($sock, $sql) = @_;
	send_msg($sock, 'Q', "$sql\0");
	drain_to_rfq($sock);
}

# ----- file-watch helpers -----

# Slurp the spans file repeatedly until at least $n lines mention
# $trace_id, or until the deadline.  Returns the contents on success;
# returns whatever was last read on timeout (caller decides whether to
# fail or proceed with partial assertions).
sub wait_for_n_spans
{
	my ($trace_id, $n) = @_;
	my $deadline = time() + 10;
	my $contents = '';
	while (time() < $deadline)
	{
		$contents = PostgreSQL::Test::Utils::slurp_file($span_file)
			if -e $span_file;
		my $count = () = $contents =~ /"trace_id":"$trace_id"/g;
		return $contents if $count >= $n;
		select(undef, undef, undef, 0.1);
	}
	return $contents;
}

# Pick out the span objects matching a trace_id.  Returns a list of
# raw JSON strings (one per matched line), in file order.
sub spans_for_trace
{
	my ($contents, $trace_id) = @_;
	my @hits;
	for my $line (split /\n/, $contents)
	{
		push @hits, $line if $line =~ /"trace_id":"$trace_id"/;
	}
	return @hits;
}

# Tiny field extractor for the well-defined fields the exporter writes.
sub field
{
	my ($json, $key) = @_;
	# All exporter values are strings or numbers; this handles both
	# string-quoted and bare numeric forms.
	return $1 if $json =~ /"$key":"([^"\\]*)"/;
	return $1 if $json =~ /"$key":(-?\d+)/;
	return undef;
}

# ----- handshake -----

my $superuser = getpwuid($<);
my $sock = $node->raw_connect();
send_startup(
	$sock,
	user           => $superuser,
	database       => 'postgres',
	'_pq_.headers' => '1');
drain_to_rfq($sock);

# ----------------------------------------------------------------------
# Test 1: end-to-end propagation, single query.
#
# Client supplies a traceparent via the 'M' protocol header; runs a
# query; the resulting span line in the file must carry exactly the
# client-supplied trace_id and parent_span_id, a freshly-generated
# (distinct) span_id, and the SQL text in db_statement.
# ----------------------------------------------------------------------

send_msg($sock, 'M', headers_body('otel.traceparent' => $TRACEPARENT_SEQ));
run_query($sock, 'SELECT 1');

my $contents = wait_for_n_spans($TRACE_ID_SEQ, 1);
ok(-e $span_file, 'span file was created');

my @seq_lines = spans_for_trace($contents, $TRACE_ID_SEQ);
is(scalar @seq_lines, 1,
	'exactly one span for the single-query trace_id');

my $seq_span = $seq_lines[0];
is(field($seq_span, 'trace_id'), $TRACE_ID_SEQ,
	'span trace_id is exactly the client-supplied value');
is(field($seq_span, 'parent_span_id'), $SPAN_ID_SEQ,
	'span parent_span_id is exactly the client-supplied span_id');

my $seq_span_id = field($seq_span, 'span_id');
like($seq_span_id, qr/^[0-9a-f]{16}$/,
	'span_id is 16 lowercase hex chars');
isnt($seq_span_id, $SPAN_ID_SEQ,
	'span_id is freshly generated, not echoed back from the client');

is(field($seq_span, 'name'), 'pgsql.execute',
	'span name is the executor command tag');
is(field($seq_span, 'status'), '0',
	'status is UNSET (0) for the success path');
is(field($seq_span, 'db_statement'), 'SELECT 1',
	'db.statement attribute carried over verbatim');

my $start = field($seq_span, 'start_time');
my $end   = field($seq_span, 'end_time');
ok(defined $start && defined $end && $end >= $start,
	'start_time and end_time present with end >= start');

# ----------------------------------------------------------------------
# Test 2: parallel query.
#
# debug_parallel_query=regress forces the SELECT through a Gather
# with parallel workers.  Each worker is a separate backend process
# that:
#   - inherits the leader's otel.traceparent + otel.current_span_id
#     GUCs via the standard parallel-state GUC restore
#   - hits contrib/otel_demo_exporter's _PG_init the same way the leader
#     did (because both modules are in shared_preload_libraries) and
#     registers its own emit hook via the rendezvous API
#   - on ExecutorStart, calls contrib/otel's start_span which sets
#     parent_span_id = otel.current_span_id (the LEADER's span id)
#     rather than echoing the client's parent
#
# Net effect: the spans file contains the leader's span (parent =
# client-supplied) plus one or more worker spans (parent = leader's
# span_id), all sharing the same trace_id.  This is the assertion
# we want, because it proves three independent things end-to-end:
#   1. the rendezvous hook is invoked inside parallel workers
#   2. trace_id flows from client through leader to workers
#   3. the current_span_id GUC linkage produces a proper parent
#      chain rather than re-rooting every worker at the client
# ----------------------------------------------------------------------

run_query($sock, 'SET debug_parallel_query = regress');
send_msg($sock, 'M', headers_body('otel.traceparent' => $TRACEPARENT_PAR));
run_query($sock, 'SELECT 42');
run_query($sock, 'RESET debug_parallel_query');

# Expect at least 2 spans (leader + >=1 worker).  Polling waits for
# both: the worker emits when it shuts down its ExecutorEnd; the
# leader emits when ITS ExecutorEnd fires.  Both happen before the
# query's ReadyForQuery comes back, so by the time we get here both
# spans should already have been fflush'd, but we still poll for
# robustness against slow CI.
$contents = wait_for_n_spans($TRACE_ID_PAR, 2);

my @par_lines = spans_for_trace($contents, $TRACE_ID_PAR);
ok(scalar(@par_lines) >= 2,
	'at least two spans recorded for the parallel-query trace_id')
  or diag("only got: " . scalar(@par_lines) . " lines\n$contents");

# Identify the leader: its parent_span_id is the client-supplied one.
my @leader = grep { field($_, 'parent_span_id') eq $SPAN_ID_PAR } @par_lines;
is(scalar @leader, 1,
	'exactly one leader span (parent_span_id == client-supplied span_id)');

my $leader_span_id;
$leader_span_id = field($leader[0], 'span_id') if @leader;
like($leader_span_id, qr/^[0-9a-f]{16}$/,
	'leader span_id is 16 hex chars');

# Worker span(s): parent_span_id == leader's span_id.
my @workers = grep {
	defined $leader_span_id
	  && field($_, 'parent_span_id') eq $leader_span_id
} @par_lines;
ok(scalar(@workers) >= 1,
	'at least one worker span links back to the leader span_id')
  or diag("worker linkage missing; parent_span_id values: "
		. join(',', map { field($_, 'parent_span_id') // '?' } @par_lines));

# All parallel spans share the trace_id (the grep already implies this,
# but this asserts it explicitly so a future regression is loud).
for my $line (@par_lines)
{
	is(field($line, 'trace_id'), $TRACE_ID_PAR,
	   'parallel span trace_id matches the client-supplied trace_id');
}

# Workers have their own distinct span_ids, not echoing the leader's.
for my $w (@workers)
{
	my $wid = field($w, 'span_id');
	like($wid, qr/^[0-9a-f]{16}$/, 'worker span_id is 16 hex chars');
	isnt($wid, $leader_span_id,
		'worker span_id is distinct from the leader');
	isnt($wid, $SPAN_ID_PAR,
		'worker span_id is not the client-supplied parent either');
}

# ----------------------------------------------------------------------
# File integrity: every non-empty line is a complete JSON object,
# unaffected by concurrent leader+worker writes (relying on O_APPEND
# atomicity for sub-PIPE_BUF writes).
# ----------------------------------------------------------------------

my $matched = 0;
my $total   = 0;
for my $line (split /\n/, $contents)
{
	next if $line eq '';
	$total++;
	$matched++ if $line =~ /^\{.*\}$/;
}
is($matched, $total,
	'every non-empty line is a complete JSON object (no interleaved writes)');

# ----------------------------------------------------------------------
# Tidy up.
# ----------------------------------------------------------------------

send_msg($sock, 'X');
$sock->close();
$node->stop;
done_testing();
