# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Exercise the libpq client-side API for per-message protocol headers
# (PQattachHeader / PQclearHeaders / PQheadersAvailable) against a
# cluster with contrib/otel loaded as the header consumer.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $TRACE_ID	= 'aabbccddeeff00112233445566778899';
my $SPAN_ID	 = '0011223344556677';
my $TRACEPARENT = "00-$TRACE_ID-$SPAN_ID-01";

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

# ----------------------------------------------------------------------
# Helper: run the libpq_headers test client and capture stdout/stderr.
# ----------------------------------------------------------------------

sub run_client
{
	my ($mode, $traceparent) = @_;

	my ($stdout, $stderr);
	IPC::Run::run([ 'libpq_headers', $conn_str, $mode, $traceparent ],
		'>', \$stdout, '2>', \$stderr)
	  or die "libpq_headers failed: $stderr";
	chomp $stdout;
	return ($stdout, $stderr);
}

# ----------------------------------------------------------------------
# Test 1: attach mode --- PQattachHeader queues the header, PQexec
# flushes it as an 'M' message, and the server-side otel handler
# stores the parsed traceparent.  The client gets it back via
# otel_current_traceparent().
# ----------------------------------------------------------------------

my $log_offset = -s $node->logfile;

my ($stdout, $stderr) = run_client('attach', $TRACEPARENT);
is($stdout, $TRACEPARENT,
	'PQattachHeader + PQexec delivers the traceparent to the server');

# Confirm the server-side log line for the SELECT carried the trace
# context via log_line_prefix.
$node->wait_for_log(
	qr/TR\[$TRACE_ID\] SP\[$SPAN_ID\] LOG:\s+statement: SELECT coalesce\(otel_current_traceparent/,
	$log_offset);
pass('server log carries trace_id/span_id for the libpq-driven SELECT');

# ----------------------------------------------------------------------
# Test 2: no-attach mode --- no PQattachHeader call, so no 'M' is sent;
# otel_current_traceparent() must return empty (NULL coalesced to '').
# ----------------------------------------------------------------------

$log_offset = -s $node->logfile;
($stdout, $stderr) = run_client('none', $TRACEPARENT);
is($stdout, '',
	'without PQattachHeader, no trace context reaches the server');

# The log line for SELECT must also have empty %T / %S.
my $log_after =
  PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_offset);
like(
	$log_after,
	qr/TR\[\] SP\[\] LOG:\s+statement: SELECT coalesce\(otel_current_traceparent/,
	'server log shows empty trace_id/span_id when no header attached');

# ----------------------------------------------------------------------
# Tidy up.
# ----------------------------------------------------------------------

$node->stop;
done_testing();
