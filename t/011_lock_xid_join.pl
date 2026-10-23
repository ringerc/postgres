# Copyright (c) 2026, PostgreSQL Global Development Group
#
# A blocked session's pg.lock.wait span must identify its blocker: the
# waiter's pg.lock.xid equals the holder's pg.txn.xid, and pg.backend.pid
# names each side's backend.  Speculative-insertion (ON CONFLICT) token
# waits are decoded as pg.lock.xid + pg.lock.spectoken.
#
# Spans are backend-local, so each scenario uses one background psql
# session per backend and reads each session's own capture ring.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $TRACEPARENT = '00-aabbccddeeff00112233445566778899-0011223344556677-01';

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'otel_api,otel_postgres_tracing,test_otel_exporter'\n"
	  . "log_min_messages = warning\n");
$node->start;

$node->safe_psql('postgres', q{
	CREATE EXTENSION otel_api;
	CREATE EXTENSION otel_postgres_tracing;
	CREATE EXTENSION test_otel_exporter;
	CREATE TABLE t (id int PRIMARY KEY, v int);
	INSERT INTO t VALUES (1, 0);
});

# Value of attribute $key in a span text from test_otel_pop_span*().
sub span_attr
{
	my ($span, $key) = @_;
	return undef unless defined $span;
	my ($val) = $span =~ /^attr=\Q$key\E=(.*)$/m;
	return $val;
}

# All captured pg.txn spans in a session, oldest first.  The session's
# own bookkeeping statements emit (xid-less) pg.txn spans too, so callers
# pick the one they want by attribute.
sub txn_spans
{
	my ($s) = @_;

	# One statement: the popping query's own pg.txn span is emitted only
	# after it finishes, so it is not part of the result.
	my $out = $s->query_safe(q{
		SELECT coalesce(string_agg(sp, chr(1)), '') FROM (
			SELECT test_otel_pop_span_by_name('pg.txn') AS sp
			FROM generate_series(1, 50)) x
		WHERE sp IS NOT NULL});
	return split /\x01/, $out;
}

sub txn_xids
{
	my ($s) = @_;
	return grep { defined } map { span_attr($_, 'pg.txn.xid') } txn_spans($s);
}

# Start a session with SDT txn+lock_wait spans on and an empty ring.
sub new_session
{
	my $s = $node->background_psql('postgres');
	$s->query_safe("SET otel_api.traceparent = '$TRACEPARENT'");
	$s->query_safe("SET otel.trace_sdt_probes = 'txn,lock_wait'");
	$s->query_safe(q{SELECT test_otel_clear()});
	return $s;
}

sub wait_for_ungranted
{
	my ($locktype) = @_;
	$node->poll_query_until('postgres',
		"SELECT count(*) = 1 FROM pg_locks WHERE NOT granted AND locktype = '$locktype'"
	) or die "no waiter on a $locktype lock";
}

# Hold a lock in A, block B with $b_sql, commit A, then check the join.
sub check_blocked_on_xid
{
	my ($label, $a_sql, $b_sql) = @_;

	my $a = new_session();
	my $b = new_session();
	my $a_pid = $a->query_safe('SELECT pg_backend_pid()');
	my $b_pid = $b->query_safe('SELECT pg_backend_pid()');
	my $a_xid = $a->query_safe("BEGIN; $a_sql; SELECT pg_current_xact_id()::xid::text");

	$b->query_until(qr/b_started/, "\\echo b_started\n$b_sql;\n");
	wait_for_ungranted('transactionid');
	$a->query_safe('COMMIT');
	$b->query_safe('SELECT 1');	# returns once B's statement has finished
	$b->query_safe('COMMIT') if $b_sql =~ /^BEGIN/;

	my ($txn) = grep { defined span_attr($_, 'pg.txn.xid') } txn_spans($a);
	my $wait = $b->query_safe(
		q{SELECT test_otel_pop_span_by_name('pg.lock.wait')});

	is(span_attr($txn, 'pg.txn.xid'), $a_xid, "$label: holder pg.txn.xid is its xid");
	is(span_attr($wait, 'pg.lock.type'), 'transactionid', "$label: waiter lock type");
	is(span_attr($wait, 'pg.lock.xid'), $a_xid, "$label: waiter pg.lock.xid == holder pg.txn.xid");
	is(span_attr($txn, 'pg.backend.pid'), $a_pid, "$label: holder pg.backend.pid");
	is(span_attr($wait, 'pg.backend.pid'), $b_pid, "$label: waiter pg.backend.pid");

	$a->quit;
	$b->quit;
}

check_blocked_on_xid('row update', 'UPDATE t SET v = 1 WHERE id = 1',
	'UPDATE t SET v = 2 WHERE id = 1');

check_blocked_on_xid('ON CONFLICT on uncommitted duplicate',
	'INSERT INTO t VALUES (2, 1)',
	'INSERT INTO t VALUES (2, 2) ON CONFLICT (id) DO UPDATE SET v = excluded.v');

# Aborted holder: xid still recorded, pg.txn carries the error status.
{
	my $a = new_session();
	my $a_xid = $a->query_safe(
		'BEGIN; UPDATE t SET v = 9 WHERE id = 1; SELECT pg_current_xact_id()::xid::text');
	$a->query_safe('ROLLBACK');
	is_deeply([txn_xids($a)], [$a_xid], 'aborted txn: pg.txn.xid recorded');
	$a->quit;
}

# A transaction that never assigned an xid has no pg.txn.xid.
{
	my $a = new_session();
	$a->query_safe('BEGIN; SELECT 1; COMMIT');
	my @spans = txn_spans($a);
	ok(scalar(@spans) > 0, 'read-only txn: pg.txn span present');
	is_deeply([grep { defined span_attr($_, 'pg.txn.xid') } @spans], [],
		'read-only txn: no pg.txn.xid');
	$a->quit;
}

# Speculative-insertion token wait: decoded, not raw field1..4.
{
	my $a = new_session();
	my $b = new_session();
	$a->query_safe(q{BEGIN; SELECT test_otel_spectoken_lock(4242, 7, 'exclusive')});
	$b->query_until(qr/b_started/,
		"\\echo b_started\nBEGIN; SELECT test_otel_spectoken_lock(4242, 7, 'share');\n");
	wait_for_ungranted('spectoken');
	$a->query_safe('COMMIT');
	$b->query_safe('COMMIT');

	my $wait = $b->query_safe(
		q{SELECT test_otel_pop_span_by_name('pg.lock.wait')});
	is(span_attr($wait, 'pg.lock.type'), 'spectoken', 'spectoken: lock type');
	is(span_attr($wait, 'pg.lock.xid'), '4242', 'spectoken: pg.lock.xid');
	is(span_attr($wait, 'pg.lock.spectoken'), '7', 'spectoken: pg.lock.spectoken');
	is(span_attr($wait, 'pg.lock.field1'), undef, 'spectoken: no raw field1');
	$a->quit;
	$b->quit;
}

done_testing();
