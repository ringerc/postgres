# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Coverage for the libpq client-side per-message protocol headers API
# (PQattachHeader / PQclearHeaders / PQheadersAvailable).
#
# The libpq_headers helper binary attaches a header under the
# "test_tx." prefix and runs a no-op SELECT to drive the protocol
# forward.  Server-side verification uses the test_protocol_headers
# loadable module, which registers a transaction-scoped handler for
# that prefix and logs every set/clear event.  The TAP test reads
# the log between known offsets to confirm:
#
#   * the handler was invoked exactly when the client attached;
#   * was NOT invoked when the client did not attach, or cleared the
#     queue, or used a stale (already-flushed) queue.

use strict;
use warnings FATAL => 'all';

use IPC::Run ();
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $VALUE = 'hello-from-the-test';

# ----------------------------------------------------------------------
# Cluster setup --- feature ENABLED on the server side.
# ----------------------------------------------------------------------

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'test_protocol_headers'
log_min_messages = log
EOCONF
$node->start;

my $conn_str = $node->connstr('postgres');

# Helper: run the libpq_headers test client with one mode and capture
# stdout, stderr, and exit status.
sub run_client
{
	my @args = ('libpq_headers', $conn_str, @_);
	my ($stdout, $stderr) = ('', '');
	IPC::Run::run(\@args, '>', \$stdout, '2>', \$stderr,
		IPC::Run::timeout(60));
	my $rc = $? >> 8;
	return ($rc, $stdout, $stderr);
}

# Helper: count occurrences of /set scope=transaction key=test_tx\.alpha
# value=VALUE/ in the log slice since $offset.  test_protocol_headers
# logs one such line per set event.
sub count_sets
{
	my ($offset, $value) = @_;
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $offset);
	my $re = qr/test_protocol_headers: set scope=transaction key=test_tx\.alpha value=\Q$value\E/;
	return scalar(() = $log =~ /$re/g);
}

# Helper: count occurrences of any test_protocol_headers set line under
# the test_tx prefix, ignoring the value.  Used for absence checks
# where the client never attached anything and so no value was at risk.
sub count_any_sets
{
	my ($offset) = @_;
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $offset);
	return scalar(() =
		$log =~ /test_protocol_headers: set scope=transaction key=test_tx\./g);
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
# Group 2: PQattachHeader + PQexec delivers the value to the server.
# ----------------------------------------------------------------------

{
	my $offset = -s $node->logfile;
	my ($rc, $out, $err) = run_client('attach', $VALUE);
	is($rc, 0, 'attach mode exited 0');
	is($out, "ok\n", 'attach mode reported ok');

	# wait_for_log polls until the regex matches, so we don't race the
	# log writer.
	$node->wait_for_log(
		qr/test_protocol_headers: set scope=transaction key=test_tx\.alpha value=\Q$VALUE\E/,
		$offset);
	is(count_sets($offset, $VALUE), 1,
		'handler fired exactly once for one PQattachHeader + PQexec');
}

# ----------------------------------------------------------------------
# Group 3: no attach -> handler never fires.
# ----------------------------------------------------------------------

{
	my $offset = -s $node->logfile;
	my ($rc, $out, $err) = run_client('none');
	is($rc, 0, 'none mode exited 0');
	is($out, "ok\n", 'none mode reported ok');

	# Give the server a moment to flush any pending log lines before we
	# check.  Issuing a marker query + waiting for it serves as a
	# barrier without depending on wall-clock sleep.
	$node->safe_psql('postgres', 'SELECT 1');
	is(count_any_sets($offset), 0,
		'no PQattachHeader => no handler invocation');
}

# ----------------------------------------------------------------------
# Group 4: PQclearHeaders cancels the queued attach.
# ----------------------------------------------------------------------

{
	my $offset = -s $node->logfile;
	my ($rc, $out, $err) = run_client('clear', $VALUE);
	is($rc, 0, 'clear mode exited 0');
	is($out, "ok\n", 'clear mode reported ok');

	$node->safe_psql('postgres', 'SELECT 1');
	is(count_any_sets($offset), 0,
		'PQclearHeaders prevents the queued attach from being sent');
}

# ----------------------------------------------------------------------
# Group 5: queue resets between operations (no leak from op N to op N+1).
# ----------------------------------------------------------------------

{
	my $offset = -s $node->logfile;
	my ($rc, $out, $err) = run_client('reuse', $VALUE);
	is($rc, 0, 'reuse mode exited 0');
	is($out, "ok\n", 'reuse mode reported ok');

	$node->wait_for_log(
		qr/test_protocol_headers: set scope=transaction key=test_tx\.alpha value=\Q$VALUE\E/,
		$offset);
	is(count_sets($offset, $VALUE), 1,
		'first SELECT triggers the handler; second SELECT does not (queue reset)');
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
