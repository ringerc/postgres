# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Coverage for the libpq client-side trace-context API
# (PQsetTraceContext / PQattachTraceContext / PQtraceContextAvailable).
#
# The libpq_trace_context helper binary exercises each API function and
# connects to a cluster running with test_trace_context loaded so we can
# verify that trace context actually reaches the server via the log.

use strict;
use warnings FATAL => 'all';

use IPC::Run ();
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $TRACEPARENT =
  '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01';

# ----------------------------------------------------------------------
# Cluster setup
# ----------------------------------------------------------------------

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'test_trace_context'
log_min_messages = log
EOCONF
$node->start;

my $conn_str = $node->connstr('postgres');

# Helper: run the libpq_trace_context test client with one mode and capture
# stdout, stderr, and exit status.
sub run_client
{
	my @args = ('libpq_trace_context', $conn_str, @_);
	my ($stdout, $stderr) = ('', '');
	IPC::Run::run(\@args, '>', \$stdout, '2>', \$stderr,
		IPC::Run::timeout(60));
	my $rc = $? >> 8;
	return ($rc, $stdout, $stderr);
}

# Helper: count apply log lines for the test traceparent since $offset.
sub count_apply
{
	my ($offset) = @_;
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $offset);
	return scalar(()
		= $log =~
		/test_trace_context: apply traceparent=00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01/g
	);
}

# Helper: count clear log lines since $offset.
sub count_clear
{
	my ($offset) = @_;
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $offset);
	return scalar(() = $log =~ /test_trace_context: clear/g);
}

# ----------------------------------------------------------------------
# Group 1: PQtraceContextAvailable reports availability correctly.
# ----------------------------------------------------------------------

{
	my ($rc, $out, $err) = run_client('available');
	is($rc,  0,    'available mode exited 0');
	is($out, "1\n", 'PQtraceContextAvailable returns 1 for 3.3 server');
}

# ----------------------------------------------------------------------
# Group 2: PQattachTraceContext + PQexec delivers context to server.
# ----------------------------------------------------------------------

{
	my $offset = -s $node->logfile;
	my ($rc, $out, $err) = run_client('attach');
	is($rc,  0,     'attach mode exited 0');
	is($out, "ok\n", 'attach mode reported ok');

	$node->wait_for_log(
		qr/test_trace_context: apply traceparent=\Q$TRACEPARENT\E/,
		$offset);
	is(count_apply($offset), 1,
		'handler fired exactly once for PQattachTraceContext + PQexec');
}

# ----------------------------------------------------------------------
# Group 3: PQsetTraceContext (armed) re-emits on each subsequent operation.
# ----------------------------------------------------------------------

{
	my $offset = -s $node->logfile;
	my ($rc, $out, $err) = run_client('set_armed');
	is($rc,  0,     'set_armed mode exited 0');
	is($out, "ok\n", 'set_armed mode reported ok');

	$node->wait_for_log(
		qr/test_trace_context: apply traceparent=\Q$TRACEPARENT\E/,
		$offset);

	# Two SELECTs -> apply should fire twice
	is(count_apply($offset), 2,
		'PQsetTraceContext (armed) re-emits on each subsequent operation');
}

# ----------------------------------------------------------------------
# Group 4: no context -> handler never fires.
# ----------------------------------------------------------------------

{
	my $offset = -s $node->logfile;
	my ($rc, $out, $err) = run_client('none');
	is($rc,  0,     'none mode exited 0');
	is($out, "ok\n", 'none mode reported ok');

	$node->safe_psql('postgres', 'SELECT 1');
	is(count_apply($offset), 0, 'no PQattachTraceContext => no handler invocation');
}

# ----------------------------------------------------------------------
# Group 5: PQsetTraceContext(NULL) disarms -> no context on next operation.
# ----------------------------------------------------------------------

{
	my $offset = -s $node->logfile;
	my ($rc, $out, $err) = run_client('set_null');
	is($rc,  0,     'set_null mode exited 0');
	is($out, "ok\n", 'set_null mode reported ok');

	$node->safe_psql('postgres', 'SELECT 1');
	is(count_apply($offset), 0,
		'PQsetTraceContext(NULL) disarms - no context sent');
}

$node->stop;
done_testing();
