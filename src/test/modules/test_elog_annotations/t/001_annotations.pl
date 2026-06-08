# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Exercise errannot() / errannotf() across all three log destinations and
# the new log_line_prefix escapes %A and %{key}A.
#
# logging_collector is enabled so the server writes csvlog and jsonlog;
# that also routes stderr lines into a collected file separate from
# $node->logfile, so we use current_logfiles to locate all three.

use strict;
use warnings FATAL => 'all';
use JSON::PP;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $node = PostgreSQL::Test::Cluster->new('annot');
$node->init;

# Pipe-delimited prefix lets us pluck specific escape outputs without
# fighting the surrounding LOG line.  Each named slot has a fixed shape:
#   |trace=<value>|   from %{trace_id}A
#   |span=<value>|    from %{span_id}A
#   |missing=|        from %{nope}A    (unset)
#   |brace=|          from %{bad       (unterminated -> format error)
#   |wrongletter=|    from %{trace_id}Q (wrong terminator letter)
# %A renders the whole annotation set as a quoted block elsewhere on
# the line.
$node->append_conf(
	'postgresql.conf', q!
log_destination = 'stderr,csvlog,jsonlog'
logging_collector = on
log_min_messages = log
log_statement = 'none'
log_line_prefix = '|PROBE|%A|trace=%{trace_id}A|span=%{span_id}A|missing=%{nope}A|brace=%{bad|wrongletter=%{trace_id}Q|'
!);
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_elog_annotations');

# current_logfiles lives in the data directory; each line names a
# destination and a path (relative to the data dir, or absolute).
sub current_logfiles
{
	my $datadir = $node->data_dir;
	my $current = "$datadir/current_logfiles";
	my %paths;
	open(my $fh, '<', $current) or return %paths;
	while (my $line = <$fh>)
	{
		chomp $line;
		next unless $line =~ /^(\S+)\s+(.+)$/;
		my ($dest, $path) = ($1, $2);
		$path = "$datadir/$path" unless $path =~ m{^/};
		$paths{$dest} = $path;
	}
	close $fh;
	return %paths;
}

# Poll the supplied collected log for $needle, starting at $offset.
# Returns the captured file slice on success, dies on timeout.
sub wait_for_collected
{
	my ($dest, $needle, $offset, $timeout) = @_;
	$offset  //= 0;
	$timeout //= 30;
	my $deadline = time() + $timeout;
	while (time() < $deadline)
	{
		my %paths = current_logfiles();
		if (my $path = $paths{$dest})
		{
			if (-e $path)
			{
				my $size = -s $path;
				if ($size > $offset)
				{
					my $slice =
					  PostgreSQL::Test::Utils::slurp_file($path, $offset);
					if (index($slice, $needle) >= 0)
					{
						return $slice;
					}
				}
			}
		}
		usleep(100_000);
	}
	die "timed out waiting for '$needle' in $dest log";
}

# Snapshot offsets into the three collected files before each probe so
# the wait helpers only see lines emitted by that probe.  current_logfiles
# may not yet exist before the first message; in that case the offset is
# zero and the slice covers the full file (which is fine).
sub snapshot_offsets
{
	my %paths = current_logfiles();
	my %offsets;
	for my $dest (qw(stderr csvlog jsonlog))
	{
		$offsets{$dest} = $paths{$dest} ? -s $paths{$dest} : 0;
	}
	return %offsets;
}

#---------------------------------------------------------------------
# Test 1: plain annotations land in stderr (%A and %{key}A),
#         jsonlog (top-level keys), and csvlog (trailing JSON column).
#---------------------------------------------------------------------
my %offs = snapshot_offsets();
$node->safe_psql(
	'postgres',
	"SELECT pg_test_errannot_emit('LOG', 'annotation-probe-1', "
	  . "ARRAY['trace_id','span_id'], "
	  . "ARRAY['0123456789abcdef0123456789abcdef','aaaabbbbccccdddd'], "
	  . "ARRAY['ext.dur'], ARRAY['hello']);");

my $stderr_slice =
  wait_for_collected('stderr', 'annotation-probe-1', $offs{stderr});
my ($probe_line) = grep { /annotation-probe-1/ } split(/\n/, $stderr_slice);
ok(defined $probe_line, 'probe 1 reached collected stderr');

like(
	$probe_line,
	qr/trace_id="0123456789abcdef0123456789abcdef"/,
	'%A includes trace_id');
like(
	$probe_line,
	qr/span_id="aaaabbbbccccdddd"/,
	'%A includes span_id');
like(
	$probe_line,
	qr{ext\.dur="fmt:hello/5"},
	'%A includes ext.dur via errannotf()');

like(
	$probe_line,
	qr{\|trace=0123456789abcdef0123456789abcdef\|},
	'%{trace_id}A renders bare value');
like(
	$probe_line,
	qr{\|span=aaaabbbbccccdddd\|},
	'%{span_id}A renders bare value');
like(
	$probe_line,
	qr{\|missing=\|},
	'%{key}A for unset key renders empty');
like(
	$probe_line,
	qr{\|brace=\|},
	'unterminated %{key is a format error and emits nothing');
like(
	$probe_line,
	qr{\|wrongletter=\|},
	'%{key}X (wrong terminator letter) is a format error');

my $json_slice =
  wait_for_collected('jsonlog', 'annotation-probe-1', $offs{jsonlog});
my ($json_line) = grep { /annotation-probe-1/ } split(/\n/, $json_slice);
ok(defined $json_line, 'probe 1 reached jsonlog');
my $rec = decode_json($json_line);
is($rec->{trace_id}, '0123456789abcdef0123456789abcdef',
	'jsonlog: trace_id top-level key');
is($rec->{span_id}, 'aaaabbbbccccdddd', 'jsonlog: span_id top-level key');
is($rec->{'ext.dur'}, 'fmt:hello/5',
	'jsonlog: errannotf value top-level key');
ok(!exists $rec->{pg_rejected_annotations},
	'jsonlog: no rejection aggregator for legal keys');

my $csv_slice =
  wait_for_collected('csvlog', 'annotation-probe-1', $offs{csvlog});
my ($csv_line) = grep { /annotation-probe-1/ } split(/\n/, $csv_slice);
ok(defined $csv_line, 'probe 1 reached csvlog');
my $ann_col;
if ($csv_line =~ /,"(\{.*\})"\s*$/)
{
	$ann_col = $1;
	$ann_col =~ s/""/"/g;
}
ok(defined $ann_col, 'csvlog: trailing column is a JSON object');
my $cdec = decode_json($ann_col);
is($cdec->{trace_id}, '0123456789abcdef0123456789abcdef',
	'csvlog: trace_id in annotations column');
is($cdec->{'ext.dur'}, 'fmt:hello/5',
	'csvlog: ext.dur in annotations column');

#---------------------------------------------------------------------
# Test 2: reserved-key collisions land in pg_rejected_annotations and
#         the offending value is NOT promoted into the log record.
#---------------------------------------------------------------------
%offs = snapshot_offsets();
$node->safe_psql(
	'postgres',
	"SELECT pg_test_errannot_emit('LOG', 'annotation-probe-2', "
	  . "ARRAY['pid','message','myext.ok'], "
	  . "ARRAY['rejected-value-pid','rejected-value-msg','kept'], "
	  . "ARRAY[]::text[], ARRAY[]::text[]);");
$json_slice = wait_for_collected('jsonlog', 'annotation-probe-2',
	$offs{jsonlog});
($json_line) = grep { /annotation-probe-2/ } split(/\n/, $json_slice);
ok(defined $json_line, 'probe 2 reached jsonlog');
$rec = decode_json($json_line);

ok($rec->{pid} =~ /^\d+$/,
	'jsonlog: built-in pid key is the integer pid, not the rejected value');
isnt($rec->{message}, 'rejected-value-msg',
	'jsonlog: built-in message key is not the rejected value');
is($rec->{'myext.ok'}, 'kept',
	'jsonlog: namespaced key is attached normally');
ok(exists $rec->{pg_rejected_annotations},
	'jsonlog: rejection aggregator is present');

my %rejected = map { $_ => 1 } split(/,/, $rec->{pg_rejected_annotations});
ok($rejected{pid},     'rejection aggregator records "pid"');
ok($rejected{message}, 'rejection aggregator records "message"');
ok(!$rejected{'myext.ok'},
	'rejection aggregator does not record legal keys');

#---------------------------------------------------------------------
# Test 3: annotations attached inside PG_TRY survive CopyErrorData() /
#         FlushErrorState().  pg_test_errannot_rethrow swallows an inner
#         ERROR, saves the ErrorData, then re-emits a LOG with the saved
#         annotations re-attached.  We inspect that LOG record.
#---------------------------------------------------------------------
%offs = snapshot_offsets();
$node->safe_psql(
	'postgres',
	"SELECT pg_test_errannot_rethrow("
	  . "ARRAY['trace_id','myext.tag'], "
	  . "ARRAY['fedcba98765432100123456789abcdef','rethrow-tag']);");

$json_slice =
  wait_for_collected('jsonlog', 'rethrow probe', $offs{jsonlog});
($json_line) = grep { /rethrow probe/ } split(/\n/, $json_slice);
ok(defined $json_line, 'rethrown error reached jsonlog');
$rec = decode_json($json_line);
is($rec->{trace_id}, 'fedcba98765432100123456789abcdef',
	'jsonlog: trace_id survives CopyErrorData()');
is($rec->{'myext.tag'}, 'rethrow-tag',
	'jsonlog: extension annotation survives CopyErrorData()');

#---------------------------------------------------------------------
# Test 4: value-quoting in %A.  A value containing double-quote and
# backslash must be backslash-escaped; nothing else needs escaping.
# Build the value via chr() in SQL to avoid wrestling with three layers
# of escaping (Perl -> psql -> Postgres string literal).
#
# Value is 5 chars: a " b \ c
# %A is expected to render: a\"b\\c
#---------------------------------------------------------------------
%offs = snapshot_offsets();
$node->safe_psql(
	'postgres',
	"SELECT pg_test_errannot_emit('LOG', 'annotation-probe-4', "
	  . "ARRAY['ext.quoted'], "
	  . "ARRAY[chr(97)||chr(34)||chr(98)||chr(92)||chr(99)], "
	  . "ARRAY[]::text[], ARRAY[]::text[]);");
$stderr_slice = wait_for_collected('stderr', 'annotation-probe-4',
	$offs{stderr});
($probe_line) = grep { /annotation-probe-4/ } split(/\n/, $stderr_slice);
like(
	$probe_line,
	qr!ext\.quoted="a\\"b\\\\c"!,
	'%A escapes embedded " and \\ in values');

$node->stop;

done_testing();
