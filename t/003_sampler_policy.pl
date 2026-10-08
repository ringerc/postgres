# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Smoke test: otel_api owns the sampler policy (otel_api.sampler /
# otel_api.sampler_arg, postgres-cdq.18) rather than an exporter hook.
# For each (sampler, wire-bit) pair, sends a single query with a
# chosen traceparent flag over the 'M' header (a remote parent) and
# asserts the captured span count matches the documented behaviour.
# The exhaustive new-root / remote-sampled / remote-unsampled /
# traceidratio matrix lives in tests/otel_api_conformance (it can
# fabricate exact trace IDs and parent kinds over plain SQL); this
# file only confirms the two GUCs actually reach a real exporter
# through a real 'M' header.
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
#   2. SET otel_api.sampler (+ otel_api.sampler_arg for the ratio ones)
#   3. 'M' header with otel.traceparent carrying the chosen flag
#   4. SELECT 1
#   5. read test_otel_span_count()
#
# All on the same backend so per-session GUC state holds.

sub run_cell
{
	my ($label, $sampler, $arg, $flag, $expected) = @_;

	run_query($sock, 'SELECT test_otel_clear()');
	run_query($sock, "SET otel_api.sampler = '$sampler'");
	run_query($sock, "SET otel_api.sampler_arg = $arg");

	my $tp = "00-$TRACE_ID-$SPAN_ID-$flag";
	send_msg($sock, 'M', headers_body('otel.traceparent' => $tp));
	run_query($sock, 'SELECT 1');

	my @msgs = run_query($sock,
		"SELECT test_otel_count_spans_by_name('pgsql.execute')");
	my $got  = first_value(@msgs);
	is($got, "$expected", $label);
}

# ----------------------------------------------------------------------
# otel_api.sampler x remote-parent wire bit.  Wire bit '01' means
# sampled=1; '00' means unsampled.  sampler_arg is irrelevant except
# for the traceidratio rows.
# ----------------------------------------------------------------------

# Wire bit = 1 (sampled remote parent)
run_cell('wire=1, always_on: recorded', 'always_on', 1.0, '01', 1);
run_cell('wire=1, always_off: ignores the remote bit, dropped',
	'always_off', 1.0, '01', 0);
run_cell('wire=1, parentbased_always_on: follows the remote bit',
	'parentbased_always_on', 1.0, '01', 1);
run_cell('wire=1, parentbased_always_off: still follows the remote bit',
	'parentbased_always_off', 1.0, '01', 1);

# Wire bit = 0 (unsampled remote parent)
run_cell('wire=0, always_on: ignores the remote bit, recorded',
	'always_on', 1.0, '00', 1);
run_cell('wire=0, always_off: dropped', 'always_off', 1.0, '00', 0);
run_cell('wire=0, parentbased_always_on: follows the remote bit, dropped',
	'parentbased_always_on', 1.0, '00', 0);
run_cell('wire=0, parentbased_always_off: follows the remote bit, dropped',
	'parentbased_always_off', 1.0, '00', 0);

# traceidratio: ignores the remote bit entirely; ratio=1.0 always
# samples, ratio=0.0 never does, regardless of the wire bit.
run_cell('traceidratio ratio=1.0, wire=0: always samples',
	'traceidratio', 1.0, '00', 1);
run_cell('traceidratio ratio=0.0, wire=1: never samples',
	'traceidratio', 0.0, '01', 0);

# ----------------------------------------------------------------------
# Tidy up.
# ----------------------------------------------------------------------

send_msg($sock, 'X');
$sock->close();
$node->stop;
done_testing();
