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

# The pipeline tests use server-side introspection to assert, per command,
# whether a trace context was active when the server processed that command.
# The test_trace_context module exports these as C functions but does not
# install SQL wrappers; create them here.  (Symbols are resolvable because
# the module is in shared_preload_libraries.)
$node->safe_psql(
	'postgres', <<'EOSQL');
CREATE FUNCTION test_tc_is_active() RETURNS bool
  AS 'test_trace_context', 'test_tc_is_active' LANGUAGE C;
CREATE FUNCTION test_tc_traceparent() RETURNS text
  AS 'test_trace_context', 'test_tc_traceparent' LANGUAGE C;
EOSQL

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

# ----------------------------------------------------------------------
# Group 6: pipeline mode + armed context.
#
# Enter pipeline mode, arm via PQsetTraceContext, queue several commands
# plus PQpipelineSync.  The client itself asserts (server-side, via
# test_tc_is_active()/test_tc_traceparent()) that EVERY command in the
# pipeline observed the trace context; it only prints "ok" if all did.
# We additionally confirm via the log that the handler fired and that the
# context was cleared exactly once, at the single RFQ that ends the pipeline.
# ----------------------------------------------------------------------

{
	my $offset = -s $node->logfile;
	my ($rc, $out, $err) = run_client('pipeline_armed');
	is($rc, 0, 'pipeline_armed mode exited 0');
	is($out, "ok\n",
		'pipeline_armed: every pipelined command observed the context');

	$node->wait_for_log(
		qr/test_trace_context: apply traceparent=\Q$TRACEPARENT\E/,
		$offset);

	# While armed, libpq emits an 'M' before each command, so apply fires
	# at least once per command.  The load-bearing assertion is the
	# per-command observation checked client-side above; here we just
	# confirm the handler actually ran in the pipeline.
	cmp_ok(count_apply($offset), '>=', 1,
		'pipeline_armed: handler fired for the armed pipeline');

	# A single pipeline is one RFQ window: the context is cleared exactly
	# once, at the Sync that ends the pipeline.
	is(count_clear($offset), 1,
		'pipeline_armed: context cleared exactly once at the pipeline RFQ');
}

# ----------------------------------------------------------------------
# Group 7: pipeline mode + one-shot context.
#
# A one-shot PQattachTraceContext emits a single 'M' before the first
# command; the server keeps it active until the pipeline's RFQ, so every
# command in that one pipeline observes it.  The client asserts per-command
# observation; we confirm the handler applied exactly once (one 'M') and
# cleared exactly once.
# ----------------------------------------------------------------------

{
	my $offset = -s $node->logfile;
	my ($rc, $out, $err) = run_client('pipeline_oneshot');
	is($rc, 0, 'pipeline_oneshot mode exited 0');
	is($out, "ok\n",
		'pipeline_oneshot: every pipelined command observed the context');

	$node->wait_for_log(
		qr/test_trace_context: apply traceparent=\Q$TRACEPARENT\E/,
		$offset);

	# One-shot => exactly one 'M' for the whole pipeline.
	is(count_apply($offset), 1,
		'pipeline_oneshot: one-shot emits exactly one M for the pipeline');
	is(count_clear($offset), 1,
		'pipeline_oneshot: context cleared exactly once at the pipeline RFQ');
}

# ----------------------------------------------------------------------
# Group 8: one-shot covers exactly its pipeline, not the next one.
#
# Attach a one-shot, run one pipeline (covered), then a second pipeline
# with no re-arming.  The client asserts the first pipeline's commands all
# saw the context and the second pipeline's commands saw NONE.  We confirm
# via the log that apply fired exactly once across both pipelines.
# ----------------------------------------------------------------------

{
	my $offset = -s $node->logfile;
	my ($rc, $out, $err) = run_client('pipeline_oneshot_2nd');
	is($rc, 0, 'pipeline_oneshot_2nd mode exited 0');
	is($out, "ok\n",
		'pipeline_oneshot_2nd: one-shot covered pipeline 1, not pipeline 2');

	$node->wait_for_log(
		qr/test_trace_context: apply traceparent=\Q$TRACEPARENT\E/,
		$offset);

	# The one-shot must NOT be re-emitted for the second pipeline: exactly
	# one apply across both pipelines.
	is(count_apply($offset), 1,
		'pipeline_oneshot_2nd: exactly one M (first pipeline only)');
}

$node->stop;
done_testing();
