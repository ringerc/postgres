use strict;
use warnings;
use bigint qw(hex);
use Cwd;
use Config;
use File::Basename qw(basename);
use List::Util qw(minstr maxstr);
use PostgresNode;
use TestLib;
use Test::More;
use Data::Dumper;

my $verbose = 1;

sub slurp_xlogdump_lines
{
	my ($node, $timeline, $firstwal, $lastwal) = @_;
	my ($stdout, $stderr) = ('', '');

	if (defined($firstwal) && $firstwal =~ /^[[:xdigit:]]{24}$/)
	{
		$firstwal = $node->data_dir . "/pg_xlog/" . $firstwal;
	}

	if (defined($lastwal) && $lastwal !~ /^[[:xdigit:]]{24}$/)
	{
		die("pg_xlogdump expects the last WAL seg to be a bare filename, not '$lastwal'");
	}

	if (!defined($firstwal) || !defined($lastwal))
	{
		my $wal_pattern = sprintf("%s/pg_xlog/%08X" . ("?" x 16), $node->data_dir, $timeline);
		my @wal = glob $wal_pattern;
		$firstwal = List::Util::minstr(@wal) if !defined($firstwal);
		$lastwal = basename(List::Util::maxstr(@wal)) if !defined($lastwal);
	}

	diag("decoding from " . $firstwal . " to " . $lastwal)
		if $verbose;

	IPC::Run::run ['pg_xlogdump', $firstwal, $lastwal], '>', \$stdout, '2>', \$stderr;

	like($stderr, qr/(?:invalid record length at [0-9A-F]+\/[0-9A-F]+: wanted 24, got 0|^\s*$)/,
		'pg_xlogdump exits with expected error or silence');

	diag "xlogdump exited with: '$stderr'"
		if $verbose;

	my $lineno = 1;

	my @xld_lines = split("\n", $stdout);

	return \@xld_lines;
}

sub match_xlogdump_lines
{
	my ($node, $timeline, $firstwal, $lastwal) = @_;

	my $xld_lines = slurp_xlogdump_lines($node, $timeline, $firstwal, $lastwal);

	my @records;
	my $lineno = 1;

	for my $xld_line (@$xld_lines)
	{
		chomp $xld_line;

		# We don't use Test::More's 'like' here because it'd run a variable number
		# of tests. Instead do our own diagnostics and fail.
		if ($xld_line =~ /^rmgr: (\w+)\s+len \(rec\/tot\):\s*([[:digit:]]+)\/\s*([[:digit:]]+), tx:\s*([[:digit:]]*), lsn: ([[:xdigit:]]{1,8})\/([[:xdigit:]]{1,8}), prev ([[:xdigit:]]{1,8})\/([[:xdigit:]]{1,8}), desc: (.*)$/)
		{
			my %record = (
				'_raw' => length($xld_line) >= 100 ? substr($xld_line,0,100) . "..." : $xld_line,
				'rmgr' => $1,
				'len_rec' => int($2),
				'len_tot' => int($3),
				'tx' => defined($4) ? int($4) : undef,
				'lsn' => (hex($5)<<32) + hex($6),
				'prev' => (hex($7)<<32) + hex($8),
				'lsn_str' => "$5/$6",
				'prev_str' => "$7/$8",
				'desc' => length($9) >= 100 ? substr($9,0,100) . "..." : $9,
			);

			if ($record{'prev'} >= $record{'lsn'})
			{
				diag("in xlog record on line $lineno:\n    $xld_line\n   ... prev ptr $record{prev_str} is greater than or equal to lsn ptr $record{lsn_str}");
				cmp_ok($record{prev}, 'lt', $record{lsn}, 'previous pointer less than lsn ptr');
			}

			push @records, \%record;
		}
		else
		{
			diag("xlog record on line $lineno:\n    $xld_line\n   ...does not match the test pattern");
			fail("an xlog record didn't match the expected pattern");
			die();
		}

		$lineno ++;
	}

	return \@records;
}

sub xlog_seg_next
{
	my ($segno) = @_;

	my ($timeline, $ptr) = ($segno =~ /^([[:xdigit:]]{8})([[:xdigit:]]{16})$/);

	die("invalid xlog segment name $segno")
	 unless (defined($timeline) && defined($ptr));

	return sprintf('%08X%016X', hex($timeline), hex($ptr) + 1);

}

sub rmgrs_for_records
{
	my ($xld_records) = @_;

	my %rmgrs;
	for my $record (@$xld_records)
	{
		$rmgrs{$record->{rmgr}} ++;
	}
	diag "Record counts for each resource manager were: " . Dumper(\%rmgrs)
		if $verbose;

	return \%rmgrs;
}

program_help_ok('pg_xlogdump');
SKIP:
{
	skip 'xlogdump version and cmdline curently nonstandard';

	program_version_ok('pg_xlogdump');
	program_options_handling_ok('pg_xlogdump');
}

my $tempdir = TestLib::tempdir;

my $node = get_new_node('main');

$node->init();

my $pgdata = $node->data_dir;
open my $CONF, ">>$pgdata/postgresql.conf";
print $CONF "wal_keep_segments = 5000\n";
close $CONF;
$node->start;

my $first_xlog = '000000010000000000000001';

my $startup_end_xlog = $node->safe_psql('postgres', "SELECT pg_xlogfile_name(pg_switch_xlog());");

$node->safe_psql('postgres', "CREATE TABLE dummy(id serial primary key, blah text);");
$node->safe_psql('postgres', "INSERT INTO dummy(blah) SELECT repeat('dummytext', x) FROM generate_series(1, 10) x;");
$node->safe_psql('postgres', "CREATE INDEX dummy_blah ON dummy(blah COLLATE \"C\");");
$node->safe_psql('postgres', "CHECKPOINT");
my $simpletest_end_xlog = $node->safe_psql('postgres', "SELECT pg_xlogfile_name(pg_switch_xlog());");

my $start_xid = $node->safe_psql('postgres', "SELECT txid_current()");

$node->safe_psql('postgres', q{
CREATE TABLE trivial(padding text);

CREATE UNLOGGED TABLE trivial_ul(padding text);

DO
LANGUAGE plpgsql
$$
DECLARE
  wal_seg_size integer;
  remaining_size integer;
BEGIN
  wal_seg_size := (select setting from pg_settings where name = 'wal_segment_size')::integer
  				* (select setting from pg_settings where name = 'wal_block_size')::integer;

  -- Write WAL until we're near the end of a segment, and in the process
  -- generate a bunch of subxacts.
  LOOP
    SELECT wal_seg_size - file_offset FROM pg_xlogfile_name_offset(pg_current_xlog_insert_location()) INTO remaining_size;
	RAISE NOTICE 'remaining: %',remaining_size;
    IF remaining_size < 4096
	THEN
		EXIT;
	ELSE
		BEGIN
		  INSERT INTO trivial(padding) VALUES (repeat('0123456789abcdef', 64));
		EXCEPTION
		  WHEN division_by_zero THEN
		    RAISE EXCEPTION 'unreachable';
		END;
	END IF;
  END LOOP;

  -- OK, we're near the end of an xlog segment. Time to do some more writes,
  -- but this time into an unlogged table so we don't generate xlog but we
  -- do burn xids. We need to make a big enough record to spill over.
  FOR i IN 1 .. 10000000
  LOOP
    BEGIN
      INSERT INTO trivial_ul(padding) VALUES ('x');
    EXCEPTION
      WHEN division_by_zero THEN
        RAISE EXCEPTION 'unreachable';
    END;
  END LOOP;

END;
$$;
});

my $end_xid = $node->safe_psql('postgres', "SELECT txid_current()");
my $bigrecord_end_xlog = $node->safe_psql('postgres', "SELECT pg_xlogfile_name(pg_switch_xlog());");

$node->stop('fast');

my $xld_records = match_xlogdump_lines($node, 1, $first_xlog, $startup_end_xlog);

diag "xlogdump emitted " . scalar(@$xld_records) . " entries up to end of startup";

my $rmgrs = rmgrs_for_records($xld_records);

# Must be lexically sorted:
my @expected_rmgr_names = qw(
	Btree
	CLOG
	Database
	Heap
	Heap2
	Storage
	Transaction
	XLOG
);
my @rmgr_names = sort(keys(%$rmgrs));
is_deeply(\@rmgr_names, \@expected_rmgr_names, 'Got expected set of rmgrs in startup');

$xld_records = match_xlogdump_lines($node, 1, xlog_seg_next($startup_end_xlog), $simpletest_end_xlog);
pass('decoded simple test segment');

$xld_records = match_xlogdump_lines($node, 1, xlog_seg_next($simpletest_end_xlog), $bigrecord_end_xlog);
pass('decoded segment with ridiculous commit record');

done_testing();
