#-------------------------------------------------------------------------
# Perl script to create dummy probes.h file when dtrace is not available
#
# Copyright (c) 2008-2026, PostgreSQL Global Development Group
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
);

BEGIN { print "#include \"utils/pg_sdt_probe.h\"\n"; }

m/^\s*probe / || next;

# Extract the probe name (double-underscore form, e.g. "query__parse__start")
(my $probe_name) = /^\s*probe ([^(]+)/;

if (exists $curated{$probe_name})
{
	my ($enum_id, $tags) = @{$curated{$probe_name}};
	my $nargs = scalar @$tags;

	# Build the uppercase single-underscore macro name
	(my $macro_name = $probe_name) =~ s/__/_/g;
	$macro_name =~ y/abcdefghijklmnopqrstuvwxyz/ABCDEFGHIJKLMNOPQRSTUVWXYZ/;
	$macro_name = "TRACE_POSTGRESQL_$macro_name";

	# Build the parameter list: INT1, INT2, ...
	my @params = map { "INT$_" } 1 .. $nargs;
	my $param_list = join(', ', @params);

	# Build the PgSdtArg array initializer elements
	my @elems;
	for my $k (1 .. $nargs)
	{
		my $tag  = $tags->[$k - 1];
		my $parm = "INT$k";
		if ($tag eq 's')
		{
			push @elems, "{ 's', { .s = (const char *) ($parm) } }";
		}
		else
		{
			push @elems, "{ 'i', { .i = (int64) ($parm) } }";
		}
	}

	# Emit the macro definition
	my $macro_params = $nargs > 0 ? "($param_list)" : "()";
	my $hook_call;
	if ($nargs == 0)
	{
		$hook_call = "pg_sdt_probe_hook($enum_id, ((void *) 0), 0)";
		print "#define ${macro_name}${macro_params} do { if (pg_sdt_probe_hook) $hook_call; } while (0)\n";
	}
	else
	{
		my $elems_str = join(', ', @elems);
		$hook_call = "pg_sdt_probe_hook($enum_id, _pg_sdt_a, $nargs)";
		print "#define ${macro_name}${macro_params} do { if (pg_sdt_probe_hook) { PgSdtArg _pg_sdt_a[] = { $elems_str }; $hook_call; } } while (0)\n";
	}
	print "#define ${macro_name}_ENABLED() (0)\n";
	next;
}

# Non-curated probe: emit the existing dummy no-op macros.
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
