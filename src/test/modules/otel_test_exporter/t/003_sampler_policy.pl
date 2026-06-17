# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Walks the sampler-hook invocation policy matrix exposed by
# contrib/otel's v2 OtelTracingApi.set_sampler_policy.  For each
# (policy, wire-bit, sampler-decision) triple, sends a single
# query with a chosen traceparent flag and asserts the captured
# span count matches contrib/otel's documented behaviour.
#
# The control surface comes from this test exporter:
#   * SQL fn  test_otel_set_policy('hook_on_unsampled_bit' | ...)
#     plumbs through to api->set_sampler_policy.
#   * GUC test_otel_exporter.sampler_decision (drop | record_only |
#     record_and_sample) controls what our sampler hook returns
#     when contrib/otel decides to call it.
#
# The captured-span count comes from the existing
# test_otel_span_count() introspection.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $TRACE_ID = 'aabbccddeeff00112233445566778899';
my $SPAN_ID  = '0011223344556677';

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel_api,otel_postgres_tracing,test_otel_exporter'
log_min_messages = warning
log_statement = 'none'
EOCONF
$node->start;
$node->safe_psql('postgres',
	'CREATE EXTENSION otel_api; CREATE EXTENSION test_otel_exporter');

if (!$node->raw_connect_works())
{
	plan skip_all => "this test requires working raw_connect()";
}

# ----- raw-protocol helpers (same shape as 001_basic.pl) -----

sub send_startup
{
	my ($sock, @kv) = @_;
	my $body = pack('N', 0x00030003);	# protocol 3.3
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
	# TraceContext ('M') wire body: two NUL-terminated strings
	# (traceparent, tracestate).  Accepts the legacy keyed-pair calling
	# convention and maps recognised keys to wire positions.
	my %h = @_;
	my $tp = $h{'otel.traceparent'} // '';
	my $ts = $h{'otel.tracestate'} // '';
	return "$tp\0$ts\0";
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

# ----- handshake -----

my $superuser = getpwuid($<);
my $sock = $node->raw_connect();
send_startup($sock,
	user     => $superuser,
	database => 'postgres');
drain_to_rfq($sock);

# ----- helper: run one cell of the matrix -----
#
# Each cell:
#   1. test_otel_clear()  -- empty the per-backend ring
#   2. SET test_otel_exporter.sampler_decision -- what hook returns
#   3. test_otel_set_policy(...)  -- which policy gates apply
#   4. 'M' header with otel.traceparent carrying the chosen flag
#   5. SELECT 1
#   6. read test_otel_span_count()
#
# All on the same backend so per-session GUC + policy state holds.

sub run_cell
{
	my ($label, $policy, $decision, $flag, $expected) = @_;

	run_query($sock, 'SELECT test_otel_clear()');
	run_query($sock,
		"SET test_otel_exporter.sampler_decision = '$decision'");
	run_query($sock,
		"SELECT test_otel_set_policy('$policy')");

	my $tp = "00-$TRACE_ID-$SPAN_ID-$flag";
	send_msg($sock, 'M', headers_body('otel.traceparent' => $tp));
	run_query($sock, 'SELECT 1');

	my @msgs = run_query($sock, 'SELECT test_otel_span_count()');
	my $got  = first_value(@msgs);
	is($got, "$expected", $label);
}

# ----------------------------------------------------------------------
# The matrix.  Mirrors TESTING.md in the rust-demo repo; each row
# encodes (policy, wire-bit, sampler-decision) -> expected span count.
#
# Wire bit '01' means sampled=1; '00' means unsampled.  The
# test_otel_exporter sampler hook returns the configured decision
# whenever contrib/otel chooses to call it under the active policy.
# ----------------------------------------------------------------------

# Wire bit = 1 (sampled)
run_cell(
	'wire=1, hook_on_unsampled_bit, decision=drop: W3C wins (gate 4 short-circuits)',
	'hook_on_unsampled_bit', 'drop', '01', 1);
run_cell(
	'wire=1, hook_always, decision=drop: sampler overrides W3C, span dropped',
	'hook_always', 'drop', '01', 0);
run_cell(
	'wire=1, hook_always, decision=record_and_sample: sampler agrees with W3C',
	'hook_always', 'record_and_sample', '01', 1);
run_cell(
	'wire=1, never_respect_bit, decision=drop: hook ignored, bit wins',
	'never_respect_bit', 'drop', '01', 1);

# Wire bit = 0 (unsampled)
run_cell(
	'wire=0, hook_on_unsampled_bit, decision=drop: SDK ParentBased default',
	'hook_on_unsampled_bit', 'drop', '00', 0);
run_cell(
	'wire=0, hook_on_unsampled_bit, decision=record_and_sample: hook promotes',
	'hook_on_unsampled_bit', 'record_and_sample', '00', 1);
run_cell(
	'wire=0, hook_always, decision=record_and_sample: hook called, records',
	'hook_always', 'record_and_sample', '00', 1);
run_cell(
	'wire=0, never_respect_bit, decision=record_and_sample: hook ignored, bit wins (drop)',
	'never_respect_bit', 'record_and_sample', '00', 0);
run_cell(
	'wire=0, never_always_sample, decision=drop: hook ignored, force record',
	'never_always_sample', 'drop', '00', 1);
run_cell(
	'wire=0, never_always_sample, decision=record_and_sample: hook ignored, force record',
	'never_always_sample', 'record_and_sample', '00', 1);

# ----------------------------------------------------------------------
# RECORD_ONLY: contrib/otel records the span, but the sampler tells
# downstream "this is not part of a globally-sampled trace."  The
# captured-span count from test_otel_span_count goes up just like
# RECORD_AND_SAMPLE because the test exporter doesn't distinguish.
# Worth a test anyway to prove the decision is plumbed through
# without contrib/otel re-mapping it to DROP.
# ----------------------------------------------------------------------

run_cell(
	'wire=0, hook_on_unsampled_bit, decision=record_only: still recorded',
	'hook_on_unsampled_bit', 'record_only', '00', 1);

# ----------------------------------------------------------------------
# Tidy up.
# ----------------------------------------------------------------------

send_msg($sock, 'X');
$sock->close();
$node->stop;
done_testing();
