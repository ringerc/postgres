# Copyright (c) 2026, PostgreSQL Global Development Group
#
# OTel span enrichment for auto_explain: asserts the "plan" event
# (carrying db.postgresql.explain_plan) gets attached to the active OTel
# statement span.
#
# This only produces anything when the in-tree build defines HAVE_OTEL_API
# (see contrib/auto_explain/meson.build / Makefile); without it,
# auto_explain.c falls back to the vendored no-op otel_producer_stub.h and
# every assertion below fails.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $TRACE_ID    = 'bb22cc33dd44ee55ff660011223344aa';
my $SPAN_ID     = '2233445566778899';
my $FLAGS       = '01';
my $TRACEPARENT = "00-$TRACE_ID-$SPAN_ID-$FLAGS";

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel_api,otel_postgres_tracing,auto_explain,test_otel_exporter'
log_min_messages = warning
auto_explain.log_min_duration = 0
EOCONF
$node->start;

# raw_connect_works() requires a running postmaster (it tests by actually
# connecting), so this check must come after $node->start.
if (!$node->raw_connect_works())
{
	plan skip_all => "this test requires working raw_connect()";
}

$node->safe_psql('postgres',
	'CREATE EXTENSION otel_api; CREATE EXTENSION test_otel_exporter');

# ----------------------------------------------------------------------
# Raw-protocol helpers (same shape as otel_test_exporter's t/001_basic.pl,
# needed to inject a known W3C traceparent via the TraceContext ('M')
# protocol message ahead of the query, so the statement span can be
# looked up deterministically).
# ----------------------------------------------------------------------

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
	# (traceparent, tracestate).
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

# ----------------------------------------------------------------------
# Handshake.
# ----------------------------------------------------------------------

my $superuser = getpwuid($<);
my $sock = $node->raw_connect();
send_startup(
	$sock,
	user     => $superuser,
	database => 'postgres');
drain_to_rfq($sock);

# ----------------------------------------------------------------------
# Test: auto_explain attaches a "plan" event, carrying
# db.postgresql.explain_plan, to the active pgsql.execute statement span.
# ----------------------------------------------------------------------

run_query($sock, 'SELECT test_otel_clear()');

send_msg($sock, 'M', headers_body('otel.traceparent' => $TRACEPARENT));
run_query($sock, 'SELECT 1');

my @msgs = run_query($sock,
	"SELECT test_otel_pop_span_by_name('pgsql.execute')");
my $span = first_value(@msgs);
ok(defined $span && length $span,
	'pgsql.execute span captured for the explained query');
like($span, qr/trace_id=$TRACE_ID/,
	'span carries the propagated trace_id');
like($span, qr/event\.name=plan\n/,
	'auto_explain attached a "plan" event to the statement span');
like(
	$span,
	qr/event\.attr=db\.postgresql\.explain_plan=.*cost=/s,
	'the plan event carries the EXPLAIN plan text');

# ----------------------------------------------------------------------
# Tidy up.
# ----------------------------------------------------------------------

send_msg($sock, 'X');
$sock->close();
$node->stop;
done_testing();
