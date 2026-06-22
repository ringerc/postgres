#-------------------------------------------------------------------------
# Perl script to create dummy probes.h file when dtrace is not available
#
# Copyright (c) 2008-2025, PostgreSQL Global Development Group
#
# src/backend/utils/Gen_dummy_probes.pl
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

# Curated probe table: maps double-underscore probe name to
# [enum_id, [tag, ...]]  where tag is 'i' (int64) or 's' (const char *).
my %curated = (
	'transaction__start'      => ['PG_SDT_TRANSACTION_START',    ['i']],
	'transaction__commit'     => ['PG_SDT_TRANSACTION_COMMIT',   ['i']],
	'transaction__abort'      => ['PG_SDT_TRANSACTION_ABORT',    ['i']],
	'query__start'            => ['PG_SDT_QUERY_START',          ['s']],
	'query__done'             => ['PG_SDT_QUERY_DONE',           ['s']],
	'query__parse__start'     => ['PG_SDT_QUERY_PARSE_START',    ['s']],
	'query__parse__done'      => ['PG_SDT_QUERY_PARSE_DONE',     ['s']],
	'query__rewrite__start'   => ['PG_SDT_QUERY_REWRITE_START',  ['s']],
	'query__rewrite__done'    => ['PG_SDT_QUERY_REWRITE_DONE',   ['s']],
	'query__plan__start'      => ['PG_SDT_QUERY_PLAN_START',     []],
	'query__plan__done'       => ['PG_SDT_QUERY_PLAN_DONE',      []],
	'query__execute__start'   => ['PG_SDT_QUERY_EXECUTE_START',  []],
	'query__execute__done'    => ['PG_SDT_QUERY_EXECUTE_DONE',   []],
	'sort__start'             => ['PG_SDT_SORT_START',           ['i','i','i','i','i','i']],
	'sort__done'              => ['PG_SDT_SORT_DONE',            ['i','i']],
	'smgr__md__read__start'   => ['PG_SDT_SMGR_MD_READ_START',  ['i','i','i','i','i','i']],
	'smgr__md__read__done'    => ['PG_SDT_SMGR_MD_READ_DONE',   ['i','i','i','i','i','i','i','i']],
	'smgr__md__write__start'  => ['PG_SDT_SMGR_MD_WRITE_START', ['i','i','i','i','i','i']],
	'smgr__md__write__done'   => ['PG_SDT_SMGR_MD_WRITE_DONE',  ['i','i','i','i','i','i','i','i']],
	'syncrep__wait__start'    => ['PG_SDT_SYNCREP_WAIT_START',  ['i']],
	'syncrep__wait__done'     => ['PG_SDT_SYNCREP_WAIT_DONE',   ['i']],
	'recovery__xact__commit'  => ['PG_SDT_RECOVERY_XACT_COMMIT',['s','i']],
);

BEGIN { print "#include \"utils/pg_sdt_probe.h\"\n"; }


m/^\s*probe / || next;
s/^\s*probe ([^(]*)(.*);/$1$2/;
s/__/_/g;
y/abcdefghijklmnopqrstuvwxyz/ABCDEFGHIJKLMNOPQRSTUVWXYZ/;
s/^/#define TRACE_POSTGRESQL_/;
s/\([^,)]{1,}\)/(INT1)/;
s/\([^,)]{1,}, [^,)]{1,}\)/(INT1, INT2)/;
s/\([^,)]{1,}, [^,)]{1,}, [^,)]{1,}\)/(INT1, INT2, INT3)/;
s/\([^,)]{1,}, [^,)]{1,}, [^,)]{1,}, [^,)]{1,}\)/(INT1, INT2, INT3, INT4)/;
s/\([^,)]{1,}, [^,)]{1,}, [^,)]{1,}, [^,)]{1,}, [^,)]{1,}\)/(INT1, INT2, INT3, INT4, INT5)/;
s/\([^,)]{1,}, [^,)]{1,}, [^,)]{1,}, [^,)]{1,}, [^,)]{1,}, [^,)]{1,}\)/(INT1, INT2, INT3, INT4, INT5, INT6)/;
s/\([^,)]{1,}, [^,)]{1,}, [^,)]{1,}, [^,)]{1,}, [^,)]{1,}, [^,)]{1,}, [^,)]{1,}\)/(INT1, INT2, INT3, INT4, INT5, INT6, INT7)/;
s/\([^,)]{1,}, [^,)]{1,}, [^,)]{1,}, [^,)]{1,}, [^,)]{1,}, [^,)]{1,}, [^,)]{1,}, [^,)]{1,}\)/(INT1, INT2, INT3, INT4, INT5, INT6, INT7, INT8)/;
s/$/ do {} while (0)/;
print;
s/\(.*$/_ENABLED() (0)/;
print;
