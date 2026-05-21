# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Coverage for the libpq client-side per-message protocol headers API
# (PQattachHeader / PQclearHeaders / PQheadersAvailable).  Runs the
# small libpq_headers helper binary in several modes against a cluster
# with contrib/otel loaded as the header consumer, then verifies the
# stdout and the server log.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $TRACE_ID	= 'aabbccddeeff00112233445566778899';
my $SPAN_ID	 = '0011223344556677';
my $TRACEPARENT = "00-$TRACE_ID-$SPAN_ID-01";

# ----------------------------------------------------------------------
# Cluster setup --- feature ENABLED on the server side.
# ----------------------------------------------------------------------

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel'
log_statement = 'all'
log_min_messages = log
log_line_prefix = 'TR[%T] SP[%S] '
EOCONF
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION otel');

my $conn_str = $node->connstr('postgres');

# Helper: run the libpq_headers test client with one mode and capture
# stdout, stderr, and exit status.
sub run_client
{
	my @args = ('libpq_headers', $conn_str, @_);
	my ($stdout, $stderr) = ('', '');
	IPC::Run::run(\@args, '>', \$stdout, '2>', \$stderr);
	my $rc = $? >> 8;
	return ($rc, $stdout, $stderr);
}

# ----------------------------------------------------------------------
# Group 1: PQheadersAvailable reports server support correctly.
# ----------------------------------------------------------------------

{
	my ($rc, $out, $err) = run_client('available');
	is($rc, 0, 'available mode exited 0');
	is($out, "1\n",
		'PQheadersAvailable returns 1 against a feature-enabled server');
}

# ----------------------------------------------------------------------
# Group 2: attach + SELECT delivers traceparent end-to-end.
# ----------------------------------------------------------------------

my $log_offset = -s $node->logfile;

{
	my ($rc, $out, $err) = run_client('attach', $TRACEPARENT);
	is($rc, 0, 'attach mode exited 0');
	is($out, "$TRACEPARENT\n",
		'PQattachHeader + PQexec delivers traceparent to the server');
}

# Server log carries the trace context via log_line_prefix.
$node->wait_for_log(
	qr/TR\[$TRACE_ID\] SP\[$SPAN_ID\] LOG:\s+statement: SELECT coalesce\(otel_current_traceparent/,
	$log_offset);
pass('server log carries trace_id/span_id for the libpq-driven SELECT');

# ----------------------------------------------------------------------
# Group 3: no attach -> no trace context.
# ----------------------------------------------------------------------

$log_offset = -s $node->logfile;

{
	my ($rc, $out, $err) = run_client('none');
	is($rc, 0, 'none mode exited 0');
	is($out, "\n", 'without PQattachHeader, no trace context reaches server');
}

my $log_after =
  PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);
like(
	$log_after,
	qr/TR\[\] SP\[\] LOG:\s+statement: SELECT coalesce\(otel_current_traceparent/,
	'server log shows empty trace_id/span_id when no header attached');

# ----------------------------------------------------------------------
# Group 4: PQclearHeaders cancels a queued attach.
# ----------------------------------------------------------------------

{
	my ($rc, $out, $err) = run_client('clear', $TRACEPARENT);
	is($rc, 0, 'clear mode exited 0');
	is($out, "\n",
		'PQclearHeaders prevents the queued attach from being sent');
}

# ----------------------------------------------------------------------
# Group 5: queue resets between operations (no leak from op N to op N+1).
# ----------------------------------------------------------------------

{
	my ($rc, $out, $err) = run_client('reuse', $TRACEPARENT);
	is($rc, 0, 'reuse mode exited 0');
	is($out, "$TRACEPARENT\n\n",
		'first SELECT has trace context; second SELECT does NOT (queue reset)');
}

# ----------------------------------------------------------------------
# Group 6: NULL key is refused rather than crashing.
# ----------------------------------------------------------------------

{
	my ($rc, $out, $err) = run_client('null_key');
	is($rc, 0, 'null_key mode exited 0');
	is($out, "0\n",
		'PQattachHeader(NULL key) returns 0 instead of crashing');
}

$node->stop;

# ----------------------------------------------------------------------
# Group 7: server has the feature DISABLED.
#
# Restart the same cluster with protocol_headers = off.  The server now
# treats _pq_.headers as unrecognized (it goes into
# NegotiateProtocolVersion) and does NOT emit the protocol_features
# ParameterStatus.  The libpq client must report headersAvailable=0
# and PQattachHeader must fail with a sensible error message.
# ----------------------------------------------------------------------

$node->append_conf('postgresql.conf', "protocol_headers = off\n");
$node->start;

{
	my ($rc, $out, $err) = run_client('available');
	is($rc, 0, 'available mode exited 0 with feature disabled');
	is($out, "0\n",
		'PQheadersAvailable returns 0 against a feature-disabled server');
}

{
	my ($rc, $out, $err) = run_client('not_negotiated');
	is($rc, 0, 'not_negotiated mode exited 0');
	is($out, "rejected\n",
		'PQattachHeader cleanly refuses when feature was not negotiated');
}

$node->stop;
done_testing();
