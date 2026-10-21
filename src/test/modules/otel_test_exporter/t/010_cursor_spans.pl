# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Cross-statement cursor spans (DECLARE .. FETCH .. CLOSE).  otel_api
# P2 edge-case plan, postgres-cdq.10 / postgres-cdq.9.7 / postgres-cdq.9.16.
#
# A cursor's underlying "pgsql.execute" span is started by ExecutorStart
# at DECLARE time and only ended by ExecutorEnd at CLOSE time, but every
# statement in between (the DECLARE's own utility span, each FETCH's
# utility span, ...) pushes and pops its own span on otel_api's active
# stack.  Before postgres-cdq.10, that cursor span was not .detached,
# so it sat on the active stack across the statement boundary: as soon
# as the DECLARE statement's own wrapping span tried to end (it is
# BELOW the still-open cursor span on the stack, since the cursor
# executor span was pushed inside it and never popped), otel_api's
# "ended with spans still open above it" non-LIFO path fired --
# unwinding (exporting with ERROR status) the cursor's own span right
# after DECLARE, before any FETCH ever runs.  In a cassert build that
# path is not just a WARNING, it is Assert(false) in
# nonlifo_warning(): DECLARE CURSOR alone crashed the backend (see
# otel_producer.c's nonlifo_warning()).
#
# postgres-cdq.10 fixes this: the cursor's executor span is now
# .detached (otel_declaring_cursor_portal in otel_trace.c), and
# otel_ExecutorRun() calls otel_span_activate()/otel_span_deactivate()
# around just the one ExecutorRun call that belongs to each FETCH, so
# work done while materialising rows parents to the cursor span, and
# the span itself stays open from DECLARE to CLOSE.
#
# Spans/counters are backend-local (see t/001's header comment): each
# scenario below is one psql invocation/connection so capture and
# readback see the same backend.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'otel_api,otel_postgres_tracing,test_otel_exporter'\n"
	  . "restart_after_crash = on\n"
	  . "log_min_messages = warning\n"
	  . "otel_api.emit_spans_to_log = on\n");
$node->start;

$node->safe_psql('postgres',
	'CREATE EXTENSION otel_api; CREATE EXTENSION otel_postgres_tracing; CREATE EXTENSION test_otel_exporter'
);

my $cassert = $node->safe_psql('postgres', 'SHOW debug_assertions');
note("debug_assertions = $cassert");

# ----------------------------------------------------------------------
# Scenario 1: plain DECLARE CURSOR, no FETCH yet.  Pre-fix this alone
# crashed a cassert backend (and logged the non-LIFO WARNING on every
# build); post-fix, neither happens.
# ----------------------------------------------------------------------

{
	my $log_start = -s $node->logfile;
	my ($ret, $stdout, $stderr) = $node->psql(
		'postgres', q{
			SET otel.trace_all_queries = on;
			BEGIN;
			DECLARE c CURSOR FOR SELECT i FROM generate_series(1,5) i;
			CLOSE c;
			COMMIT;
		},
		on_error_stop => 0);
	is($ret, 0, 'DECLARE CURSOR no longer crashes/errors (fixed)');
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_start);
	unlike(
		$log,
		qr/otel_api: span ended with spans still open above it/,
		'no non-LIFO WARNING for a plain DECLARE CURSOR .. CLOSE');
	unlike($log, qr/TRAP:|Assert/, 'no Assert/crash in the log');
}

# ----------------------------------------------------------------------
# Scenario 2: the cursor's span stays open across FETCHes, and ends
# normally (status UNSET, not ERROR) only at CLOSE.  Also: a WARNING
# raised while materialising a FETCH's rows attaches to the cursor
# span, not the FETCH utility span or nothing -- proof that the cursor
# span is the active parent during the FETCH's ExecutorRun.
# ----------------------------------------------------------------------

{
	$node->safe_psql(
		'postgres', q{
			CREATE OR REPLACE FUNCTION cursor_test_warn(i int) RETURNS int AS $f$
			BEGIN
				RAISE WARNING 'from fetch %', i;
				RETURN i;
			END $f$ LANGUAGE plpgsql;
		});

	my $log_start = -s $node->logfile;
	my $out = $node->safe_psql(
		'postgres', q{
			SET otel.trace_all_queries = on;
			SELECT test_otel_clear();
			BEGIN;
			DECLARE c CURSOR FOR SELECT cursor_test_warn(i) FROM generate_series(1,2) i;
			FETCH 1 FROM c;
			FETCH 1 FROM c;
			CLOSE c;
			COMMIT;
			SELECT test_otel_pop_span_by_name('pgsql.execute')
			  FROM generate_series(1, 20);
		});
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_start);
	unlike($log, qr/otel_api: span ended with spans still open above it/,
		'no non-LIFO WARNING while fetching from the cursor');

	# Find the span whose db.query.text is the DECLARE (the ring also
	# holds "SELECT test_otel_clear()" and the SELECT
	# test_otel_pop_span_by_name() calls' own spans, all also named
	# "pgsql.execute").
	my ($cursor_span) = grep { /db\.query\.text=DECLARE c CURSOR/ }
	  split /(?=scope\.name=)/, $out;
	ok(defined $cursor_span, 'found the cursor\'s own span in the ring');

	like($cursor_span, qr/status=0\n/,
		'cursor span ends normally (UNSET), not unwound with ERROR');
	like(
		$cursor_span,
		qr/event\.name=exception\n/,
		'a WARNING raised during FETCH is captured as an event');
	# slot_record_error() (otel_producer.c) keeps the latest message on
	# a tie (both are WARNING-level), so "from fetch 2" (the second
	# FETCH's warning) is what is captured -- on the cursor span
	# itself, not on either FETCH's own utility span or nowhere: proof
	# that the cursor span is the active parent throughout the
	# FETCHes, not just the first one.
	like(
		$cursor_span,
		qr/event\.attr=exception\.message=from fetch 2/,
		'...the WARNING from a FETCH lands on the cursor span itself (parentage proof)'
	);
}

# ----------------------------------------------------------------------
# Scenario 3: two cursors, opened and closed out of order.
# ----------------------------------------------------------------------

{
	my $log_start = -s $node->logfile;
	my $out = $node->safe_psql(
		'postgres', q{
			SET otel.trace_all_queries = on;
			BEGIN;
			DECLARE a CURSOR FOR SELECT i FROM generate_series(1,3) i;
			DECLARE b CURSOR FOR SELECT i FROM generate_series(10,13) i;
			FETCH 1 FROM a;
			FETCH 1 FROM b;
			FETCH 1 FROM a;
			CLOSE b;
			FETCH 1 FROM a;
			CLOSE a;
			COMMIT;
		});
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_start);
	unlike($log, qr/otel_api: span ended with spans still open above it/,
		'two interleaved cursors, closed out of order: no non-LIFO WARNING'
	);
	unlike($log, qr/TRAP:|Assert/, 'no crash');
}

# ----------------------------------------------------------------------
# Scenario 4: a WITH HOLD cursor survives COMMIT and is still fetchable
# afterwards (PersistHoldablePortal() re-runs the executor via
# ExecutorRun, exercised through the same activate/deactivate path).
# ----------------------------------------------------------------------

{
	my $log_start = -s $node->logfile;
	my $out = $node->safe_psql(
		'postgres', q{
			SET otel.trace_all_queries = on;
			BEGIN;
			DECLARE h CURSOR WITH HOLD FOR SELECT i FROM generate_series(1,5) i;
			FETCH 1 FROM h;
			COMMIT;
			FETCH 1 FROM h;
			CLOSE h;
		});
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_start);
	unlike($log, qr/otel_api: span ended with spans still open above it/,
		'WITH HOLD cursor across COMMIT: no non-LIFO WARNING');
	unlike($log, qr/TRAP:|Assert/, 'no crash');
}

# ----------------------------------------------------------------------
# Scenario 5: a FETCH that errors deactivates the cursor span cleanly
# (PG_FINALLY in otel_ExecutorRun()) rather than crashing or leaving
# the active stack imbalanced. Recovering the session afterwards with
# ROLLBACK no longer crashes (postgres-cdq.21: start_stmt_span() used
# to call get_database_name(), which asserts IsTransactionState() and
# crashed when a utility statement's span -- ROLLBACK's own -- started
# while the transaction was in aborted-block state).
# ----------------------------------------------------------------------

{
	my $log_start = -s $node->logfile;
	my ($ret, $stdout, $stderr) = $node->psql(
		'postgres', q{
			SET otel.trace_all_queries = on;
			BEGIN;
			DECLARE e CURSOR FOR SELECT 1/(i-2) FROM generate_series(1,3) i;
			FETCH 1 FROM e;
			FETCH 1 FROM e;
			ROLLBACK;
		},
		on_error_stop => 0);
	like($stderr, qr/division by zero/,
		'the second FETCH reports the division-by-zero error');
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_start);
	unlike($log, qr/TRAP:|Assert/,
		'no crash from the erroring FETCH, or from the ROLLBACK that recovers'
	);
	is($ret, 0, 'the ROLLBACK itself succeeds (recovers the session)');

	# The postmaster, and other backends, are unaffected.
	is($node->safe_psql('postgres', 'SELECT 1'), '1',
		'the postmaster is healthy after the erroring FETCH');
}

# ----------------------------------------------------------------------
# Scenario 7 (postgres-cdq.21): ROLLBACK, and ROLLBACK TO SAVEPOINT,
# after an error inside an explicit BEGIN, with otel.trace_all_queries
# on, no longer crash a cassert backend -- and the recovering
# statement's own span still carries db.namespace and pg.database.oid,
# now set from MyProcPort->database_name / MyDatabaseId instead of
# get_database_name().
# ----------------------------------------------------------------------

{
	my $log_start = -s $node->logfile;
	my ($ret, $stdout, $stderr) = $node->psql(
		'postgres', q{
			SET otel.trace_all_queries = on;
			BEGIN;
			SELECT 1/0;
			ROLLBACK;
		},
		on_error_stop => 0);
	like($stderr, qr/division by zero/, 'ROLLBACK scenario: the induced error');
	is($ret, 0, 'ROLLBACK after an error completes without crashing');
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_start);
	unlike($log, qr/TRAP:|Assert/, 'ROLLBACK scenario: no crash in the log');

	$log_start = -s $node->logfile;
	($ret, $stdout, $stderr) = $node->psql(
		'postgres', q{
			SET otel.trace_all_queries = on;
			BEGIN;
			SAVEPOINT s1;
			SELECT 1/0;
			ROLLBACK TO SAVEPOINT s1;
			COMMIT;
		},
		on_error_stop => 0);
	like($stderr, qr/division by zero/,
		'ROLLBACK TO SAVEPOINT scenario: the induced error');
	is($ret, 0, 'ROLLBACK TO SAVEPOINT after an error completes without crashing'
	);
	$log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_start);
	unlike($log, qr/TRAP:|Assert/,
		'ROLLBACK TO SAVEPOINT scenario: no crash in the log');

	# db.namespace and pg.database.oid are present on an ordinary
	# client-backend span (BEGIN's own utility span, captured via the
	# JSON log emitter, already on for this whole file).
	$log_start = -s $node->logfile;
	$node->safe_psql(
		'postgres', q{
			SET otel.trace_all_queries = on;
			BEGIN;
			COMMIT;
		});
	$log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_start);
	my ($span_line) = grep { /"name":"BEGIN"/ } split /\n/, $log;
	ok(defined $span_line, 'found the BEGIN span in the log emitter output');
	SKIP:
	{
		skip 'BEGIN span not found', 2 unless defined $span_line;
		like($span_line, qr/"db\.namespace":"postgres"/,
			'db.namespace is set (from MyProcPort->database_name)');
		like($span_line, qr/"pg\.database\.oid":\d+/,
			'pg.database.oid is set (from MyDatabaseId)');
	}
}

# ----------------------------------------------------------------------
# Scenario 6 (postgres-cdq.10 review follow-up, defect 3): DECLARE
# CURSOR's query planning can run arbitrary nested statements before
# the cursor's own ExecutorStart ever runs -- e.g. constant-folding an
# IMMUTABLE function that itself executes SQL (PerformCursorOpen calls
# pg_plan_query(), which can invoke such a function via
# eval_const_expressions(), *before* it creates the portal and calls
# PortalStart() for the cursor itself). A flag that is merely "are we
# somewhere inside DECLARE CURSOR's ProcessUtility call" (the
# original otel_declaring_cursor_portal) is set across all of that
# too, wrongly marking every such nested statement's own span
# .detached -- not just the cursor's.
#
# Fixed by checking core's ActivePortal (tcop/pquery.h) against the
# cursor's own portal name: PortalStart() sets ActivePortal to the new
# cursor portal only immediately before calling ExecutorStart() for
# it, strictly after planning (and hence after any nested statements
# planning might run) has already finished.
#
# This was confirmed against the pre-fix code with a temporary
# diagnostic (not committed): logging otel_ExecutorStart()'s
# "is_cursor" decision showed it wrongly true for both nested
# statements below, and ActivePortal->name empty (the top-level
# unnamed portal) rather than "c" at that point -- proving the flag
# leaked. Fixed, it is only true for the cursor's own ExecutorStart.
#
# Note: once otel_declaring_cursor_name no longer leaks, both the
# nested statement and the cursor's query correctly compute their own
# parent from the active stack regardless -- otel_ExecutorRun()'s
# activate/deactivate (the postgres-cdq.10 fix itself) happens to
# mask the pre-fix mislabelling's effect on simple parentage checks
# for a *single* ExecutorRun per nested statement, since it still
# temporarily activates a wrongly-.detached span for the one
# ExecutorRun it belongs to. So this scenario is a regression/sanity
# check (no crash, correct results, correct nesting) rather than one
# that fails pre-fix; the fix's necessity is established by the
# diagnostic above and by code inspection (a wrongly-.detached span's
# bookkeeping entry is, for the wrong reason, treated as a cursor by
# otel_ExecutorRun()'s peek_cursor_span(), which does not generalise
# safely, e.g. to a nested statement run more than once).
# ----------------------------------------------------------------------

{
	$node->safe_psql(
		'postgres', q{
			CREATE OR REPLACE FUNCTION cursor_test_leaf() RETURNS int AS $f$
			DECLARE r int;
			BEGIN
				SELECT count(*) INTO r FROM generate_series(1,3);
				RETURN r;
			END $f$ LANGUAGE plpgsql;

			CREATE OR REPLACE FUNCTION cursor_test_folded(i int) RETURNS int AS $f$
			BEGIN
				RETURN i + (SELECT cursor_test_leaf());
			END $f$ LANGUAGE plpgsql IMMUTABLE;
		});

	my $log_start = -s $node->logfile;
	my $out = $node->safe_psql(
		'postgres', q{
			SET otel.trace_all_queries = on;
			SELECT test_otel_clear();
			BEGIN;
			DECLARE c CURSOR FOR SELECT cursor_test_folded(1);
			FETCH 1 FROM c;
			CLOSE c;
			COMMIT;
			SELECT test_otel_pop_span() FROM generate_series(1, 30);
		});
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_start);
	unlike($log, qr/TRAP:|Assert/,
		'constant-folded nested statement during DECLARE CURSOR planning: no crash'
	);
	unlike($log, qr/otel_api: span .* still open above it/,
		'...and no non-LIFO WARNING');

	my ($folded_span) = grep {
		/name=pgsql\.execute\n/ && /db\.query\.text=i \+ \(SELECT cursor_test_leaf/
	} split /(?=scope\.name=)/, $out;
	my ($leaf_span) = grep {
		/name=pgsql\.execute\n/
		  && /db\.query\.text=SELECT count\(\*\)\s+FROM generate_series/
	} split /(?=scope\.name=)/, $out;
	ok(defined $folded_span && defined $leaf_span,
		'both the folded expression and the leaf statement were captured');
	SKIP:
	{
		skip 'spans not found', 1 unless $folded_span && $leaf_span;
		my ($folded_id) = $folded_span =~ /span_id=([0-9a-f]+)/;
		my ($leaf_parent) = $leaf_span =~ /parent_span_id=([0-9a-f]+)/;
		is($leaf_parent, $folded_id,
			'the leaf statement correctly nests under the folded expression\'s own span'
		);
	}
}

# ----------------------------------------------------------------------
# Scenario 8 (postgres-cdq.39): a Sort node whose plan is driven across
# more than one top-level statement -- a cursor's FETCH, here -- used to
# crash a cassert backend / WARN on a non-assert one.
#
# Root cause: core fires PG_SDT_SORT_START lazily, from the Sort node's
# first ExecProcNode call (nodeSort.c ExecSort(), via
# tuplesort_begin_heap()) -- which, for a cursor, is the FIRST FETCH, not
# DECLARE. PG_SDT_SORT_DONE only fires from tuplesort_end() at node
# shutdown (ExecEndSort()), which for a cursor is CLOSE (or the implicit
# ExecutorEnd a WITH HOLD cursor's PersistHoldablePortal() runs at
# COMMIT) -- many statements, and many otel_trace.c
# activate()/deactivate() cycles around the cursor's own detached
# "pgsql.execute" span, after the START. Pushed onto the producer's
# active stack like every other SDT span, pg.sort would still be sitting
# there when the first FETCH's otel_span_deactivate() tried to pop the
# cursor span out from under it: "ended with spans still open above it"
# (Assert(false) in a cassert build).
#
# Fixed in two parts (otel_sdt_bridge.c): pg.sort is now .detached (like
# pg.txn), so it is never on the producer's active stack for a
# deactivate/end elsewhere to trip over; and it is tracked in its own
# sort_stack[], separate from the shared sdt_stack[] used for every
# other probe family, so a later, unrelated probe's DONE (e.g. the next
# FETCH's own pg.query) can no longer blindly pop the still-open pg.sort
# entry instead of its own matching START.
#
# This is also the minimal, non-postgres_fdw repro for the bug found via
# contrib/postgres_fdw/t/002_otel_spans.pl (a foreign scan with ORDER BY
# pushed down to a remote with no index, fetched via DECLARE CURSOR +
# FETCH): no FDW is involved, just a plain SQL cursor over a Sort.
# ----------------------------------------------------------------------

{
	$node->safe_psql(
		'postgres', q{
			CREATE TABLE sort_cursor_src AS
				SELECT i, (i * 7919) % 997 AS k FROM generate_series(1, 500) i;
		});

	my $log_start = -s $node->logfile;
	my $out = $node->safe_psql(
		'postgres', q{
			SET otel.trace_all_queries = on;
			SET otel.trace_sdt_probes = 'query,sort';
			SELECT test_otel_clear();
			BEGIN;
			DECLARE s CURSOR FOR SELECT i, k FROM sort_cursor_src ORDER BY k;
			FETCH 10 FROM s;
			FETCH 10 FROM s;
			FETCH 10 FROM s;
			CLOSE s;
			COMMIT;
			SELECT test_otel_pop_span() FROM generate_series(1, 60);
		});
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_start);
	unlike(
		$log,
		qr/otel_api: span .* still open above it/,
		'postgres-cdq.39: a Sort node crossing a cursor FETCH boundary: no non-LIFO WARNING'
	);
	unlike($log, qr/TRAP:|Assert/,
		'postgres-cdq.39: ...and no crash (cassert build)');

	# The postmaster, and other backends, are unaffected (this is the
	# strongest evidence on a non-assert build, where the bug otherwise
	# manifests as only a WARNING plus a dropped sibling span, not a
	# crash).
	is($node->safe_psql('postgres', 'SELECT 1'), '1',
		'postgres-cdq.39: the postmaster is healthy afterwards');

	my ($cursor_span) = grep {
		/name=pgsql\.execute\n/ && /db\.query\.text=DECLARE s CURSOR/
	} split /(?=scope\.name=)/, $out;
	ok(defined $cursor_span,
		'postgres-cdq.39: found the cursor\'s own span (pgsql.execute, not the '
		  . 'DECLARE CURSOR utility span) in the ring');

	my ($sort_span) = grep { /name=pg\.sort\n/ } split /(?=scope\.name=)/,
	  $out;
	ok(defined $sort_span, 'postgres-cdq.39: found the pg.sort span in the ring');

	like($sort_span, qr/status=0\n/,
		'postgres-cdq.39: pg.sort ends normally (UNSET), not unwound with ERROR'
	) if defined $sort_span;

	SKIP:
	{
		skip 'spans not found', 1 unless $cursor_span && $sort_span;
		my ($cursor_id) = $cursor_span =~ /span_id=([0-9a-f]+)/;
		my ($sort_parent) = $sort_span =~ /parent_span_id=([0-9a-f]+)/;
		is($sort_parent, $cursor_id,
			'postgres-cdq.39: pg.sort correctly nests under the cursor\'s own span, '
			  . 'not some later/unrelated span');
	}
}

# ----------------------------------------------------------------------
# Scenario 9 (postgres-cdq.39): the same Sort-across-FETCH crash, but for
# a WITH HOLD cursor whose Sort node is still open (tuplesort_end() not
# yet called) when COMMIT runs PersistHoldablePortal() -- the related
# case flagged in the bug report alongside the plain-cursor one above.
# ----------------------------------------------------------------------

{
	my $log_start = -s $node->logfile;
	my ($ret, $stdout, $stderr) = $node->psql(
		'postgres', q{
			SET otel.trace_all_queries = on;
			SET otel.trace_sdt_probes = 'query,sort';
			BEGIN;
			DECLARE h CURSOR WITH HOLD FOR
				SELECT i, k FROM sort_cursor_src ORDER BY k;
			FETCH 10 FROM h;
			COMMIT;
			FETCH 10 FROM h;
			CLOSE h;
		},
		on_error_stop => 0);
	is($ret, 0,
		'postgres-cdq.39: WITH HOLD cursor over a Sort, fetched across COMMIT, succeeds'
	);
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_start);
	unlike(
		$log,
		qr/otel_api: span .* still open above it/,
		'postgres-cdq.39: WITH HOLD cursor over a Sort: no non-LIFO WARNING');
	unlike($log, qr/TRAP:|Assert/,
		'postgres-cdq.39: WITH HOLD cursor over a Sort: no crash');
}

done_testing();
