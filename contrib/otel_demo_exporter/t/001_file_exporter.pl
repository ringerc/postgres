# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Tests for the bare-minimum file exporter (contrib/otel_demo_exporter).
#
# Verifies that the rendezvous-variable API is enough to wire up an
# out-of-tree exporter: contrib/otel publishes the api, this module
# locates it, registers an emit callback, and writes JSON-lines
# spans to a configured file path.

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

my $span_file = $node->data_dir . '/otel_spans.jsonl';

$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel,otel_demo_exporter'
otel_demo_exporter.output_file = '$span_file'
log_min_messages = warning
log_statement = 'none'
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
# Test 1: a query with a propagated trace context writes a span line.
# ----------------------------------------------------------------------

send_msg($sock, 'M', headers_body('otel.traceparent' => $TRACEPARENT));
run_query($sock, 'SELECT 1');

# Disconnect to force the per-backend FILE* to fsync via on_proc_exit.
send_msg($sock, 'X');
$sock->close();

# wait_for_log polls until the file matches; reuse it by pointing it at
# our spans file rather than the server log.  PostgreSQL::Test::Cluster
# has no helper for arbitrary files, so spin a short polling loop.
my $deadline = time() + 10;
my $contents = '';
while (time() < $deadline)
{
	$contents = PostgreSQL::Test::Utils::slurp_file($span_file)
		if -e $span_file;
	last if $contents =~ /\Q$TRACE_ID\E/;
	select(undef, undef, undef, 0.1);
}

ok(-e $span_file, 'span file was created');
like(
	$contents,
	qr/"trace_id":"$TRACE_ID"/,
	'span line carries propagated trace_id');
like(
	$contents,
	qr/"parent_span_id":"$SPAN_ID"/,
	'span line carries the propagated parent_span_id');
like(
	$contents,
	qr/"span_id":"[0-9a-f]{16}"/,
	'span line carries a 16-hex generated span_id');
like(
	$contents,
	qr/"name":"pgsql\.execute"/,
	'span name is the executor command tag');
like(
	$contents,
	qr/"status":0/,
	'status is UNSET (0) for the success path');
like(
	$contents,
	qr/"db_statement":"SELECT 1"/,
	'db.statement attribute is carried over to db_statement');

# Exactly one JSON object per line.
my @lines = split /\n/, $contents;
my $matched = 0;
for my $line (@lines)
{
	$matched++ if $line =~ /^\{.*\}$/;
}
is($matched, scalar @lines,
   'every non-empty line is a complete JSON object');

$node->stop;
done_testing();
