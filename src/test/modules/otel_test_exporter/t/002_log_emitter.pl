# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Tests for the built-in JSON log-line span emitter (contrib/otel's
# zero-config fallback when no exporter module is loaded).
#
# We DO still load test_otel_exporter (so the symbol resolution
# is exercised), but the assertion is on the structured log line
# rather than on the captured span buffer.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $TRACE_ID    = 'aabbccddeeff00112233445566778899';
my $SPAN_ID     = '0011223344556677';
my $TRACEPARENT = "00-$TRACE_ID-$SPAN_ID-01";

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel_api,otel_postgres_tracing,test_otel_exporter'
otel.emit_spans_to_log = on
log_min_messages = warning
log_statement = 'none'
EOCONF
$node->start;

if (!$node->raw_connect_works())
{
	plan skip_all => "this test requires working raw_connect()";
}

$node->safe_psql('postgres', 'CREATE EXTENSION otel_api');

# ----- raw-protocol helpers (same shape as 001_basic.pl) -----

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
		die "connection closed" unless defined $type;
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

# ----- handshake -----

my $superuser = getpwuid($<);
my $sock = $node->raw_connect();
send_startup($sock,
	user           => $superuser,
	database       => 'postgres',
	'_pq_.headers' => '1');
drain_to_rfq($sock);

# ----------------------------------------------------------------------
# Test 1: a query with a propagated trace context emits an otel-span
# JSON log line.
# ----------------------------------------------------------------------

my $log_offset = -s $node->logfile;

send_msg($sock, 'M', headers_body('otel.traceparent' => $TRACEPARENT));
run_query($sock, 'SELECT 1');

# Wait for the otel-span LOG line and assert its shape.
$node->wait_for_log(qr/otel-span: \{.*"trace_id":"$TRACE_ID"/, $log_offset);
my $log =
  PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);

like(
	$log,
	qr/otel-span: \{.*"trace_id":"$TRACE_ID"/,
	'log line contains the propagated trace_id');
like(
	$log,
	qr/otel-span: \{.*"parent_span_id":"$SPAN_ID"/,
	'log line contains the propagated parent_span_id');
like(
	$log,
	qr/otel-span: \{.*"span_id":"[0-9a-f]{16}"/,
	'log line contains a freshly-generated span_id');
like(
	$log,
	qr/otel-span: \{.*"trace_flags":"01"/,
	'log line contains the propagated trace_flags');
like(
	$log,
	qr/otel-span: \{.*"db\.system":"postgresql"/,
	'log line contains the db.system attribute');
like(
	$log,
	qr/otel-span: \{.*"db\.statement":"SELECT 1"/,
	'log line contains the db.statement attribute');
like(
	$log,
	qr/otel-span: \{.*"status":0/,
	'log line shows status=0 (UNSET) for a successful query');

# ----------------------------------------------------------------------
# Test 2: a query that errors emits a log line with status=2 (ERROR)
# and an event in the events array.
# ----------------------------------------------------------------------

$log_offset = -s $node->logfile;
send_msg($sock, 'M', headers_body('otel.traceparent' => $TRACEPARENT));
run_query($sock, 'SELECT 1/i FROM generate_series(0,0) AS i');

$node->wait_for_log(qr/otel-span: \{.*"status":2/, $log_offset);
$log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);

like(
	$log,
	qr/otel-span: \{.*"status":2.*"events":\[\{.*"sqlstate":"22012"/s,
	'errored span log line shows status=ERROR with a captured event');

# ----------------------------------------------------------------------
# Tidy up.
# ----------------------------------------------------------------------

send_msg($sock, 'X');
$sock->close();
$node->stop;
done_testing();
