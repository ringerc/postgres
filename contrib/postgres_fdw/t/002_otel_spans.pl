# Copyright (c) 2026, PostgreSQL Global Development Group
#
# OTel span coverage for postgres_fdw's own instrumentation.
#
# This only produces anything when the in-tree build defines HAVE_OTEL_API
# (see contrib/postgres_fdw/meson.build / Makefile); without it,
# postgres_fdw.c falls back to the vendored no-op otel_producer_stub.h and
# every assertion below fails.
#
# Exercises two things:
#  - pg.fdw.* CLIENT spans are produced locally for a foreign scan.
#  - the default "comment" trace-propagation mode (postgres_fdw.
#    trace_propagation = comment) carries the SAME trace_id to the REMOTE
#    backend that executes the pushed-down DECLARE CURSOR (a separate
#    backend process, reached over a loopback FDW connection back to this
#    same cluster), and that remote statement span is parented to our
#    local pg.fdw.cursor span specifically -- proving genuine cross-process
#    propagation, not just a locally-observed decoration.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $TRACE_ID    = 'aa11bb22cc33dd44ee55ff6600112233';
my $SPAN_ID     = '1122334455667788';
my $FLAGS       = '01';
my $TRACEPARENT = "00-$TRACE_ID-$SPAN_ID-$FLAGS";

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel_api,otel_postgres_tracing,test_otel_exporter'
log_min_messages = warning
otel_api.emit_spans_to_log = on
otel_api.parse_sqlcommenter = on
EOCONF
$node->start;

# raw_connect_works() requires a running postmaster (it tests by actually
# connecting), so this check must come after $node->start.
if (!$node->raw_connect_works())
{
	plan skip_all => "this test requires working raw_connect()";
}

$node->safe_psql('postgres',
	'CREATE EXTENSION otel_api; CREATE EXTENSION test_otel_exporter; '
	. 'CREATE EXTENSION postgres_fdw');

$node->safe_psql('postgres',
	'CREATE TABLE fdw_otel_src (a int); '
	. 'INSERT INTO fdw_otel_src VALUES (1), (2), (3);');

# Loopback FDW setup: a foreign table pointing back at this same cluster.
# $node->host is whatever this test cluster actually listens on (a Unix
# socket directory by default, or a TCP address if the harness is
# configured for TCP) -- either way, the remote query runs in a genuinely
# separate backend process reached over libpq.
$node->safe_psql('postgres',
	"CREATE SERVER fdw_otel_loopback FOREIGN DATA WRAPPER postgres_fdw "
	. "OPTIONS (host '"
	. $node->host
	. "', port '"
	. $node->port
	. "', dbname 'postgres'); "
	. "CREATE USER MAPPING FOR CURRENT_USER SERVER fdw_otel_loopback; "
	. "CREATE FOREIGN TABLE fdw_otel_f (a int) SERVER fdw_otel_loopback "
	. "OPTIONS (table_name 'fdw_otel_src');");

# ----------------------------------------------------------------------
# Raw-protocol helpers (same shape as otel_test_exporter's t/001_basic.pl,
# needed to inject a known W3C traceparent via the TraceContext ('M')
# protocol message ahead of the query).
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
# Test 1: pg.fdw.* CLIENT spans are produced locally for a foreign scan,
# and carry the propagated trace_id.
# ----------------------------------------------------------------------

run_query($sock, 'SELECT test_otel_clear()');

my $log_offset = -s $node->logfile;

#
# No ORDER BY here: postgres_fdw would push it down to the remote as
# "ORDER BY a ASC NULLS LAST", and fdw_otel_src has no index on "a", so
# the remote plan would need an actual Sort node.  That trips an
# unrelated, pre-existing assertion failure in otel_postgres_tracing's
# per-node span bookkeeping when a Sort node is fetched through a
# cursor's FETCH (a real bug, filed separately; not something this test
# is meant to exercise).  Row order doesn't matter for what's asserted
# below, so a plain Seq Scan avoids it.
send_msg($sock, 'M', headers_body('otel.traceparent' => $TRACEPARENT));
run_query($sock, 'SELECT a FROM fdw_otel_f');

my @msgs = run_query($sock,
	"SELECT test_otel_pop_span_by_name('pg.fdw.cursor')");
my $cursor_span = first_value(@msgs);
ok(defined $cursor_span && length $cursor_span,
	'pg.fdw.cursor span captured for the foreign scan');
like($cursor_span, qr/trace_id=$TRACE_ID/,
	'fdw cursor span carries the propagated trace_id');
like($cursor_span, qr/kind=2\n/, 'fdw cursor span is CLIENT kind');
like($cursor_span, qr/attr=db\.system=postgresql/,
	'fdw cursor span carries db.system=postgresql');

my ($cursor_span_id) = $cursor_span =~ /span_id=([0-9a-f]{16})/;
ok(defined $cursor_span_id, 'extracted the fdw cursor span_id');

@msgs = run_query($sock, "SELECT test_otel_pop_span_by_name('pg.fdw.fetch')");
my $fetch_span = first_value(@msgs);
ok(defined $fetch_span && length $fetch_span,
	'pg.fdw.fetch span captured for the foreign scan');
like($fetch_span, qr/trace_id=$TRACE_ID/,
	'fdw fetch span carries the propagated trace_id');

# ----------------------------------------------------------------------
# Test 2: the remote backend's own statement span (a SEPARATE process,
# handling the loopback libpq connection) carries the same trace_id and
# is parented to our local pg.fdw.cursor span specifically.  The
# pushed-down statement is itself a DECLARE CURSOR, so the remote's
# top-level statement span is named "DECLARE CURSOR" (the command tag;
# see otel_postgres_tracing's ProcessUtility hook).
# ----------------------------------------------------------------------

$node->wait_for_log(qr/"parent_span_id":"$cursor_span_id"/, $log_offset);
my $log_contents = slurp_file($node->logfile, $log_offset);

like(
	$log_contents,
	qr/"trace_id":"$TRACE_ID"[^}]*"parent_span_id":"$cursor_span_id"/,
	'remote statement span carries the same trace_id and is parented to the local pg.fdw.cursor span'
);
like(
	$log_contents,
	qr/"parent_span_id":"$cursor_span_id"[^}]*"name":"DECLARE CURSOR"/,
	"the span propagated to is the remote backend's DECLARE CURSOR statement span"
);

# ----------------------------------------------------------------------
# Tidy up.
# ----------------------------------------------------------------------

send_msg($sock, 'X');
$sock->close();
$node->stop;
done_testing();
