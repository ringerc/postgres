# Copyright (c) 2026, PostgreSQL Global Development Group
#
# T8: Order-independent rendezvous tests.
#
# Verifies that otel tracing works regardless of shared_preload_libraries
# ordering, including:
#   - Reverse order (provider last)
#   - Provider absent (silent no-op, no ERROR)
#   - session_preload_libraries path (per-session exporter)
#   - Late LOAD path (best-effort, current-backend-only)

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $TRACE_ID    = 'aabbccddeeff00112233445566778899';
my $SPAN_ID     = '0011223344556677';
my $FLAGS       = '01';
my $TRACEPARENT = "00-$TRACE_ID-$SPAN_ID-$FLAGS";

# -----------------------------------------------------------------------
# Raw-protocol helpers (shared across sub-tests).
# -----------------------------------------------------------------------

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
	my %h    = @_;
	my $tp   = $h{'otel.traceparent'} // '';
	my $ts   = $h{'otel.tracestate'}  // '';
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

# -----------------------------------------------------------------------
# Sub-test 1: Reverse order — provider (otel_api) loads LAST
# -----------------------------------------------------------------------
# shared_preload_libraries = 'test_otel_exporter,otel_postgres_tracing,otel_api'
# Both exporters and producers register before the provider; the provider
# drains the pending list at its _PG_init.

{
	note "Sub-test 1: reverse preload order (provider last)";

	my $node = PostgreSQL::Test::Cluster->new('reverse_order');
	$node->init;
	$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'test_otel_exporter,otel_postgres_tracing,otel_api'
log_min_messages = warning
EOCONF
	$node->start;

	if (!$node->raw_connect_works())
	{
		$node->stop;
		plan skip_all => "this test requires working raw_connect()";
	}

	$node->safe_psql('postgres',
		'CREATE EXTENSION otel_api; CREATE EXTENSION test_otel_exporter');

	my $superuser = getpwuid($<);
	my $sock = $node->raw_connect();
	send_startup($sock, user => $superuser, database => 'postgres');
	drain_to_rfq($sock);

	run_query($sock, 'SELECT test_otel_clear()');

	send_msg($sock, 'M', headers_body('otel.traceparent' => $TRACEPARENT));
	run_query($sock, 'SELECT 1');

	my @msgs = run_query($sock, 'SELECT test_otel_span_count()');
	is(first_value(@msgs), '1',
		'span captured with reverse preload order (provider last)');

	@msgs = run_query($sock, 'SELECT test_otel_pop_span()');
	my $span = first_value(@msgs);
	like($span, qr/trace_id=$TRACE_ID/,
		'span carries propagated trace_id (reverse order)');
	like($span, qr/parent_span_id=$SPAN_ID/,
		'span carries propagated parent_span_id (reverse order)');

	send_msg($sock, 'X');
	$sock->close();
	$node->stop;
}

# -----------------------------------------------------------------------
# Sub-test 2: Provider absent — only otel_postgres_tracing loaded, no
# otel_api.  Queries must run cleanly with no span and no ERROR/WARNING.
# -----------------------------------------------------------------------

{
	note "Sub-test 2: provider absent (otel_postgres_tracing only, no otel_api)";

	my $node = PostgreSQL::Test::Cluster->new('no_provider');
	$node->init;
	$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel_postgres_tracing'
log_min_messages = warning
EOCONF
	$node->start;

	# Simple queries must succeed.
	my $result = $node->safe_psql('postgres', 'SELECT 42');
	is($result, '42', 'query succeeds when provider is absent');

	# Check the log for unexpected WARNINGs or ERRORs from the otel modules.
	my $log = $node->logfile;
	my $contents = PostgreSQL::Test::Utils::slurp_file($log);
	unlike($contents, qr/FATAL|ERROR.*otel/i,
		'no FATAL/ERROR from otel modules when provider absent');
	# The absent case must be silent (no WARNING about incompatibility).
	unlike($contents, qr/WARNING.*OtelTracingApi/,
		'no WARNING about OtelTracingApi when provider is simply absent');

	$node->stop;
}

# -----------------------------------------------------------------------
# Sub-test 3: session_preload_libraries path.
# otel_api in shared_preload_libraries (cluster-wide);
# test_otel_exporter in session_preload_libraries for a specific database.
# -----------------------------------------------------------------------

{
	note "Sub-test 3: session_preload_libraries (exporter per-session)";

	my $node = PostgreSQL::Test::Cluster->new('session_preload');
	$node->init;
	$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel_api,otel_postgres_tracing'
log_min_messages = warning
EOCONF
	$node->start;

	# Set session_preload_libraries for the postgres database.
	$node->safe_psql('postgres',
		q{ALTER DATABASE postgres SET session_preload_libraries = 'test_otel_exporter'});

	# Now create the extensions.
	$node->safe_psql('postgres',
		'CREATE EXTENSION otel_api; CREATE EXTENSION test_otel_exporter');

	if (!$node->raw_connect_works())
	{
		$node->stop;
		plan skip_all => "this test requires working raw_connect()";
	}

	my $superuser = getpwuid($<);
	my $sock = $node->raw_connect();
	send_startup($sock, user => $superuser, database => 'postgres');
	drain_to_rfq($sock);

	run_query($sock, 'SELECT test_otel_clear()');

	send_msg($sock, 'M', headers_body('otel.traceparent' => $TRACEPARENT));
	run_query($sock, 'SELECT 1');

	my @msgs = run_query($sock, 'SELECT test_otel_span_count()');
	is(first_value(@msgs), '1',
		'span captured via session_preload_libraries exporter');

	@msgs = run_query($sock, 'SELECT test_otel_pop_span()');
	my $span = first_value(@msgs);
	like($span, qr/trace_id=$TRACE_ID/,
		'span carries propagated trace_id (session_preload path)');

	send_msg($sock, 'X');
	$sock->close();
	$node->stop;
}

# -----------------------------------------------------------------------
# Sub-test 4: True mid-session LOAD path (alt #2).
#
# otel_api + otel_postgres_tracing are preloaded (cluster-wide) but NO
# exporter is preloaded.  The test opens a single connection and issues
# LOAD 'test_otel_exporter' mid-session, verifying:
#
#   a) LOAD does NOT raise an ERROR (core of alt #2: late load tolerated).
#   b) A traced span emitted AFTER the LOAD is captured by the exporter.
#
# Best-effort / current-backend-only semantics: spans emitted before the
# LOAD are not asserted here (the emit hook is installed at LOAD time, so
# earlier spans are simply not seen by this exporter in this backend).
# -----------------------------------------------------------------------

{
	note "Sub-test 4: true mid-session LOAD of exporter (alt #2)";

	my $node = PostgreSQL::Test::Cluster->new('late_load');
	$node->init;
	$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel_api,otel_postgres_tracing'
log_min_messages = warning
EOCONF
	$node->start;

	# Install the extension catalog entries (DDL only, no preload).
	$node->safe_psql('postgres',
		'CREATE EXTENSION otel_api; CREATE EXTENSION test_otel_exporter');

	if (!$node->raw_connect_works())
	{
		$node->stop;
		plan skip_all => "this test requires working raw_connect()";
	}

	my $superuser = getpwuid($<);
	my $sock = $node->raw_connect();
	send_startup($sock, user => $superuser, database => 'postgres');
	drain_to_rfq($sock);

	# --- core of alt #2: LOAD mid-session must NOT raise an ERROR ---
	#
	# test_otel_exporter._PG_init accepts any load mechanism (including a
	# bare LOAD) and registers via otel_api_register_when_ready().  Since
	# the provider (otel_api) is already present in this backend, the
	# registration happens immediately.
	my @load_msgs = run_query($sock, q{LOAD 'test_otel_exporter'});
	my $load_errored = grep { $_->[0] eq 'E' } @load_msgs;
	ok(!$load_errored,
		'LOAD test_otel_exporter mid-session does not ERROR');

	# Discard any spans emitted before or during LOAD (best-effort: we
	# only assert capture from the point of registration onward).
	run_query($sock, 'SELECT test_otel_clear()');

	# Emit a traced span AFTER the LOAD.  Use a propagated traceparent so
	# otel_postgres_tracing creates a child span unconditionally.
	send_msg($sock, 'M', headers_body('otel.traceparent' => $TRACEPARENT));
	run_query($sock, 'SELECT 1');

	my @msgs = run_query($sock, 'SELECT test_otel_span_count()');
	is(first_value(@msgs), '1',
		'span captured after mid-session LOAD of exporter');

	@msgs = run_query($sock, 'SELECT test_otel_pop_span()');
	my $span = first_value(@msgs);
	like($span, qr/trace_id=$TRACE_ID/,
		'span carries propagated trace_id (mid-session LOAD path)');
	like($span, qr/parent_span_id=$SPAN_ID/,
		'span carries propagated parent_span_id (mid-session LOAD path)');

	# Belt-and-suspenders: confirm the server log is free of otel ERRORs.
	my $log_contents = PostgreSQL::Test::Utils::slurp_file($node->logfile);
	unlike($log_contents, qr/ERROR.*otel/i,
		'no ERROR from otel modules during or after mid-session LOAD');

	send_msg($sock, 'X');
	$sock->close();
	$node->stop;
}

done_testing();
