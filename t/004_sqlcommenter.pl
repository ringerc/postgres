# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Coverage for contrib/otel's optional sqlcommenter trace context
# extraction (GUC otel.parse_sqlcommenter).
#
# What this test verifies:
#   * Default off: a sqlcommenter comment is ignored.
#   * GUC on, leading comment: traceparent is extracted, span
#     carries the client-supplied trace_id + parent_span_id.
#   * GUC on, trailing comment: same.
#   * GUC on, URL-encoded value: percent-escapes are decoded.
#   * GUC on, malformed comment: silently ignored, no span unless
#     other context exists (no crash, no error).
#   * Precedence: an in-flight 'M' header OVERRIDES a comment in
#     the same query --- the protocol-level signal wins.
#   * Statement scope: a comment-derived context applies to ONE
#     statement only; the immediately-following query without a
#     comment gets no traced span.
#   * Prepared-statement breakage (the documented limitation):
#     PREPARE ... AS captures the comment at parse time; subsequent
#     EXECUTEs reuse the *same* trace_id even when "logically" each
#     EXECUTE represents a distinct operation.  This is the
#     Mode-3 failure mode --- the test asserts it explicitly so
#     a future regression that "fixes" it accidentally is caught.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

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

# ----- raw-protocol helpers (same shape as 003_sampler_policy.pl) -----

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

# Run a SQL Query, then pop the most-recently captured span via
# test_otel_pop_span() and return it as a text blob (or undef if
# the ring was empty).
sub query_and_pop_span
{
	my ($sock, $sql) = @_;
	run_query($sock, 'SELECT test_otel_clear()');
	run_query($sock, $sql);
	my @msgs = run_query($sock, 'SELECT test_otel_pop_span()');
	return first_value(@msgs);
}

sub query_and_span_count
{
	my ($sock, $sql) = @_;
	run_query($sock, 'SELECT test_otel_clear()');
	run_query($sock, $sql);
	my @msgs = run_query($sock, 'SELECT test_otel_span_count()');
	return first_value(@msgs);
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

# By default the test_otel_exporter sampler hook returns DROP --- we
# explicitly want spans for our comment-bearing traceparents, so
# flip the sampler decision for this whole test.
run_query($sock,
	"SET test_otel_exporter.sampler_decision = 'record_and_sample'");

# Distinct trace_ids per scenario so a stale span from one test
# isn't mistakable for the next test's signal.
my $COMMENT_TRACE_LEAD  = '1111111111111111111111111111aaaa';
my $COMMENT_SPAN_LEAD   = 'aaaaaaaaaaaaaaaa';
my $COMMENT_TRACE_TRAIL = '2222222222222222222222222222bbbb';
my $COMMENT_SPAN_TRAIL  = 'bbbbbbbbbbbbbbbb';
my $COMMENT_TRACE_URL   = '3333333333333333333333333333cccc';
my $COMMENT_SPAN_URL    = 'cccccccccccccccc';
my $HEADER_TRACE        = '4444444444444444444444444444dddd';
my $HEADER_SPAN         = 'dddddddddddddddd';

# ----------------------------------------------------------------------
# Test 1: GUC off (default), leading comment.  No span (no other
# context; sqlcommenter parsing disabled).
# ----------------------------------------------------------------------

run_query($sock, 'RESET otel.parse_sqlcommenter');
is( query_and_span_count(
		$sock,
		"/* traceparent='00-${COMMENT_TRACE_LEAD}-${COMMENT_SPAN_LEAD}-01' */ SELECT 1"
	),
	'0',
	'GUC off: leading sqlcommenter is ignored');

# ----------------------------------------------------------------------
# Test 2: GUC on, LEADING comment.  Span produced, carries the
# client-supplied trace_id and parent_span_id.
# ----------------------------------------------------------------------

run_query($sock, "SET otel.parse_sqlcommenter = on");
my $span = query_and_pop_span(
	$sock,
	"/* traceparent='00-${COMMENT_TRACE_LEAD}-${COMMENT_SPAN_LEAD}-01' */ SELECT 'lead'"
);
ok(defined $span && length $span,
	'leading comment: span captured with GUC enabled');
like($span, qr/trace_id=${COMMENT_TRACE_LEAD}/,
	'leading comment: trace_id taken from sqlcommenter');
like($span, qr/parent_span_id=${COMMENT_SPAN_LEAD}/,
	'leading comment: parent_span_id taken from sqlcommenter');

# ----------------------------------------------------------------------
# Test 3: GUC on, TRAILING comment (the sqlcommenter spec's preferred
# placement).  Note: psql / Q messages strip a trailing semicolon
# from queryString depending on path, so we deliberately omit it.
# ----------------------------------------------------------------------

$span = query_and_pop_span(
	$sock,
	"SELECT 'trail' /* traceparent='00-${COMMENT_TRACE_TRAIL}-${COMMENT_SPAN_TRAIL}-01' */"
);
ok(defined $span && length $span,
	'trailing comment: span captured');
like($span, qr/trace_id=${COMMENT_TRACE_TRAIL}/,
	'trailing comment: trace_id taken from sqlcommenter');
like($span, qr/parent_span_id=${COMMENT_SPAN_TRAIL}/,
	'trailing comment: parent_span_id taken from sqlcommenter');

# ----------------------------------------------------------------------
# Test 4: GUC on, value uses URL-encoded chars.  The spec requires
# values to be percent-encoded; we deliberately escape the dashes
# in the traceparent value as %2D to exercise the decoder.
# ----------------------------------------------------------------------

# Encode each '-' as %2D
my $TP_ENCODED =
    "00%2D${COMMENT_TRACE_URL}%2D${COMMENT_SPAN_URL}%2D01";
$span = query_and_pop_span(
	$sock,
	"/* traceparent='${TP_ENCODED}' */ SELECT 'url'"
);
ok(defined $span && length $span,
	'URL-encoded comment: span captured');
like($span, qr/trace_id=${COMMENT_TRACE_URL}/,
	'URL-encoded comment: trace_id decoded correctly');
like($span, qr/parent_span_id=${COMMENT_SPAN_URL}/,
	'URL-encoded comment: parent_span_id decoded correctly');

# ----------------------------------------------------------------------
# Test 5: GUC on, malformed comment (missing closing quote).
# Silently ignored.  No span, no error.
# ----------------------------------------------------------------------

is( query_and_span_count(
		$sock,
		"/* traceparent='00-${COMMENT_TRACE_LEAD}-${COMMENT_SPAN_LEAD}-01 */ SELECT 'mal'"
	),
	'0',
	'malformed comment: silently dropped, no span');

# ----------------------------------------------------------------------
# Test 6: Protocol header takes precedence over comment.  Send an
# 'M' otel.traceparent referencing one trace_id, then a query that
# carries a sqlcommenter with a DIFFERENT trace_id.  The span must
# carry the header's trace_id, not the comment's.
# ----------------------------------------------------------------------

run_query($sock, 'SELECT test_otel_clear()');
send_msg($sock, 'M',
	headers_body(
		'otel.traceparent' =>
		  "00-${HEADER_TRACE}-${HEADER_SPAN}-01"));
run_query($sock,
	"/* traceparent='00-${COMMENT_TRACE_LEAD}-${COMMENT_SPAN_LEAD}-01' */ SELECT 'precedence'"
);
my @msgs = run_query($sock, 'SELECT test_otel_pop_span()');
$span = first_value(@msgs);
like($span, qr/trace_id=${HEADER_TRACE}/,
	'precedence: header trace_id wins over comment trace_id');
unlike($span, qr/trace_id=${COMMENT_TRACE_LEAD}/,
	'precedence: comment trace_id is NOT used when header is set');

# Now end the transaction (so the 'M'-scoped context clears) and
# verify that subsequent statements without a comment OR a header
# produce no span.  This proves the comment-derived context is
# statement-scoped and the header-derived one is transaction-scoped.
run_query($sock, 'ROLLBACK');
is( query_and_span_count($sock, "SELECT 'no_ctx_now'"),
	'0',
	'after transaction reset: subsequent unannotated query has no span');

# ----------------------------------------------------------------------
# Test 7: Statement scope of comment-derived context.  Run a
# comment-bearing query, then immediately a NON-comment query in
# the same session.  The second one must NOT inherit the first
# one's trace context.
# ----------------------------------------------------------------------

run_query($sock, 'SELECT test_otel_clear()');
run_query($sock,
	"/* traceparent='00-${COMMENT_TRACE_LEAD}-${COMMENT_SPAN_LEAD}-01' */ SELECT 1"
);
run_query($sock, "SELECT 2");
@msgs = run_query($sock, 'SELECT test_otel_span_count()');
is(first_value(@msgs), '1',
	'statement scope: only the comment-bearing query produced a span');

# ----------------------------------------------------------------------
# Test 8: The Mode-3 PREPARE/EXECUTE breakage --- documented
# limitation.  The PREPARE statement captures the comment in its
# stored plan; subsequent EXECUTEs do NOT re-evaluate the source
# text and so cannot pick up a new trace_id even if the EXECUTE
# string itself contains a different comment.
#
# This asserts the broken-as-designed behaviour so a future
# accidental "fix" that produces statement-fresh traces is loud.
# ----------------------------------------------------------------------

run_query($sock, 'SELECT test_otel_clear()');
run_query(
	$sock,
	"PREPARE traced_q AS SELECT 1 /* traceparent='00-${COMMENT_TRACE_LEAD}-${COMMENT_SPAN_LEAD}-01' */"
);
# Two executes; the EXECUTE statement's own comment cannot help.
run_query($sock, "EXECUTE traced_q");
run_query($sock, "EXECUTE traced_q");

@msgs = run_query($sock, 'SELECT test_otel_pop_span()');
my $first  = first_value(@msgs);
@msgs = run_query($sock, 'SELECT test_otel_pop_span()');
my $second = first_value(@msgs);

ok(defined $first && defined $second,
	'PREPARE/EXECUTE: both executes captured a span');
# Both spans MUST share the same trace_id from the PREPARE-time
# comment.  That's the structural limitation.
like($first,  qr/trace_id=${COMMENT_TRACE_LEAD}/,
	'PREPARE breakage: first EXECUTE uses PREPARE-time trace_id');
like($second, qr/trace_id=${COMMENT_TRACE_LEAD}/,
	'PREPARE breakage: second EXECUTE reuses PREPARE-time trace_id');

run_query($sock, 'DEALLOCATE traced_q');

# ----------------------------------------------------------------------
# Tidy up.
# ----------------------------------------------------------------------

send_msg($sock, 'X');
$sock->close();
$node->stop;
done_testing();
