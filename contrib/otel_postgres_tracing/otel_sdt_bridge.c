/*-------------------------------------------------------------------------
 *
 * otel_sdt_bridge.c
 *	  Bridge from PostgreSQL SDT/DTrace probe hook to OTel spans.
 *
 * Installs pg_sdt_probe_hook so that curated TRACE_POSTGRESQL_* probes
 * become OTel spans.
 *
 * Two span shapes:
 *
 *   1. Per-statement spans (pg.query / pg.parse / pg.rewrite / pg.plan /
 *      pg.execute / pg.sort / pg.smgr.*).  Each START/DONE pair becomes a
 *      span.  otel_api owns the span storage; this file keeps only an
 *      8-byte OtelSpanRef per nested probe depth, in a LIFO stack
 *      (sdt_top).  Spans are pushed onto the producer's active stack (the
 *      default) so they nest under the propagated query trace, or under
 *      whatever else the active stack currently holds (e.g. otel_trace.c's
 *      own statement span) --- otel_span_start() resolves the parent, so
 *      this file no longer has to reason about trace-id continuity itself.
 *        - START probe: otel_span_start, attributes, push onto sdt_stack.
 *        - DONE probe:  pop sdt_stack, otel_span_end.
 *
 *   2. The transaction span (pg.txn).  It spans the whole transaction and
 *      is a root of its own trace (kind INTERNAL, parent = ROOT), detached
 *      (not pushed onto the active stack, so it can never interleave with
 *      the per-statement spans) and owned by TopTransactionResourceOwner
 *      so it is force-ended --- as ERROR --- if a transaction aborts
 *      without going through the TRANSACTION_ABORT probe.  The
 *      per-statement query traces are tied to it with bidirectional span
 *      links (pg.txn <-> pg.query).
 *        - transaction__start:  otel_span_start, detached.
 *        - transaction__commit: otel_span_end (status UNSET); realign sdt_top.
 *        - transaction__abort:  status ERROR, otel_span_end; realign sdt_top.
 *
 * Design notes / constraints
 * --------------------------
 *   * Per-statement START/DONE pairing assumes LIFO nesting.  A few
 *	   probe pairs are not strictly nested (interleaved sorts, utility-
 *	   with-executor statements); an out-of-order end there is handled by
 *	   otel_api itself (it unwinds the spans above the one actually being
 *	   ended, each exported with ERROR status, with a WARNING).  So an
 *	   out-of-order emit among this bridge's per-statement spans exports
 *	   the out-of-order sibling(s) with ERROR status and never corrupts
 *	   the enclosing statement span from otel_trace.c.
 *
 * This is DEMO-quality code.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/otel_postgres_tracing/otel_sdt_bridge.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "otel_postgres_tracing.h"

/*
 * PG_HAVE_SDT_PROBE_HOOK is advertised by the core SDT-bridge patch in
 * <pg_config_manual.h> (pulled in transitively by postgres.h above), so it is
 * defined exactly when the server we are building against exposes
 * utils/pg_sdt_probe.h and the pg_sdt_probe_hook global.  No build-system
 * feature detection is needed --- this single compile-time test drives
 * everything.  On a stock PostgreSQL the macro is absent, the whole bridge is
 * compiled out, and otel_sdt_install() is the no-op stub at the bottom of this
 * file, so the module still builds and loads (the executor / utility / log
 * hooks in the rest of the extension are unaffected).
 */
#ifdef PG_HAVE_SDT_PROBE_HOOK

#include <string.h>

#include "access/xact.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "port.h"				/* pg_strong_random */
#include "storage/lock.h"		/* LOCKTAG_* / LockTagType / lock modes (pulls
								 * storage/locktag.h on PG19 where it was split
								 * out; PG18 keeps them in lock.h directly) */
#include "utils/guc.h"
#include "utils/timestamp.h"
#include "utils/pg_sdt_probe.h"
#include "utils/varlena.h"		/* SplitIdentifierString */

#include <otel_api/otel.h>
#include "otel_pg_attrs.h"


/* -----------------------------------------------------------------------
 * Module-static state
 * ----------------------------------------------------------------------- */

/*
 * LIFO stack of in-flight per-statement span handles, one per nested SDT
 * probe depth.  64 mirrors the historical MAX_SPAN_STACK_DEPTH.
 */
#define SDT_STACK_SIZE	64

static OtelSpanRef		sdt_stack[SDT_STACK_SIZE];
static int				sdt_top = 0;	/* next free slot; 0 == empty */

/*
 * Common attribute stamped on every span this bridge emits (pg.txn,
 * pg.replica.apply, and every per-statement span: pg.query, pg.parse,
 * pg.rewrite, pg.plan, pg.execute, pg.sort, pg.smgr.read, pg.smgr.write,
 * pg.syncrep.wait, pg.lock.wait).
 * Lets downstream consumers (ClickHouse, Grafana) cleanly filter spans
 * this bridge produced from the hook-based spans emitted by otel_trace.c
 * (pgsql.execute / command-tag utility spans), which the exporter can't
 * distinguish via ScopeName (the Rust exporter collapses ScopeName to
 * the crate name).
 */
#define SDT_SPAN_SOURCE_ATTR_VAL	"sdt_probe"

/*
 * Transaction-lifetime span.  Unlike the per-statement spans, this one
 * is the root of its OWN trace and is NOT pushed onto the producer's
 * active stack, so it can never cause out-of-order emits.  It is
 * associated with the per-statement query traces via span links (added
 * in the START path for the query-root probe).  detached + owned by
 * TopTransactionResourceOwner: if the transaction aborts without the
 * TRANSACTION_ABORT probe firing, the resource owner release ends it as
 * ERROR instead of leaving it open forever.
 */
static OtelSpanRef		txn_span = OTEL_SPAN_NONE;

/*
 * GUC: which SDT probe FAMILIES are enabled.  This is a GUC_LIST_INPUT
 * string GUC (otel.trace_sdt_probes); its check/assign hooks translate the
 * token list into the core symbol pg_sdt_probe_enabled_mask, which is what
 * actually gates each TRACE_POSTGRESQL_* macro at the call site.  Until the
 * mask is set, NOTHING calls this hook.  The backing string only exists so
 * SHOW / pg_settings can echo the configured value.
 */
static char			   *otel_trace_sdt_probes_str = NULL;


/* -----------------------------------------------------------------------
 * GUC list -> probe-mask mapping
 *
 * Tokens name probe FAMILIES.  Each family sets BOTH the START and DONE bits
 * together so a push can never be left without its matching pop (and the
 * span stack can never be corrupted by a half-enabled pair).  "all" / "none"
 * are convenience aliases.
 * ----------------------------------------------------------------------- */

#define SDT_BIT(id)		(UINT64CONST(1) << (id))

typedef struct SdtFamilyMap
{
	const char *name;
	uint64		bits;
} SdtFamilyMap;

#define SDT_BITS_SMGR_READ \
	(SDT_BIT(PG_SDT_SMGR_MD_READ_START) | SDT_BIT(PG_SDT_SMGR_MD_READ_DONE))
#define SDT_BITS_SMGR_WRITE \
	(SDT_BIT(PG_SDT_SMGR_MD_WRITE_START) | SDT_BIT(PG_SDT_SMGR_MD_WRITE_DONE))

#define SDT_BITS_ALL \
	(SDT_BIT(PG_SDT_TRANSACTION_START) | SDT_BIT(PG_SDT_TRANSACTION_COMMIT) | \
	 SDT_BIT(PG_SDT_TRANSACTION_ABORT) | \
	 SDT_BIT(PG_SDT_QUERY_START) | SDT_BIT(PG_SDT_QUERY_DONE) | \
	 SDT_BIT(PG_SDT_QUERY_PARSE_START) | SDT_BIT(PG_SDT_QUERY_PARSE_DONE) | \
	 SDT_BIT(PG_SDT_QUERY_REWRITE_START) | SDT_BIT(PG_SDT_QUERY_REWRITE_DONE) | \
	 SDT_BIT(PG_SDT_QUERY_PLAN_START) | SDT_BIT(PG_SDT_QUERY_PLAN_DONE) | \
	 SDT_BIT(PG_SDT_QUERY_EXECUTE_START) | SDT_BIT(PG_SDT_QUERY_EXECUTE_DONE) | \
	 SDT_BIT(PG_SDT_SORT_START) | SDT_BIT(PG_SDT_SORT_DONE) | \
	 SDT_BITS_SMGR_READ | SDT_BITS_SMGR_WRITE | \
	 SDT_BIT(PG_SDT_SYNCREP_WAIT_START) | SDT_BIT(PG_SDT_SYNCREP_WAIT_DONE) | \
	 SDT_BIT(PG_SDT_RECOVERY_XACT_COMMIT) | \
	 SDT_BIT(PG_SDT_LOCK_WAIT_START) | SDT_BIT(PG_SDT_LOCK_WAIT_DONE))

static const SdtFamilyMap sdt_family_map[] = {
	{"txn",		   SDT_BIT(PG_SDT_TRANSACTION_START) |
				   SDT_BIT(PG_SDT_TRANSACTION_COMMIT) |
				   SDT_BIT(PG_SDT_TRANSACTION_ABORT)},
	{"query",	   SDT_BIT(PG_SDT_QUERY_START) | SDT_BIT(PG_SDT_QUERY_DONE)},
	{"parse",	   SDT_BIT(PG_SDT_QUERY_PARSE_START) |
				   SDT_BIT(PG_SDT_QUERY_PARSE_DONE)},
	{"rewrite",	   SDT_BIT(PG_SDT_QUERY_REWRITE_START) |
				   SDT_BIT(PG_SDT_QUERY_REWRITE_DONE)},
	{"plan",	   SDT_BIT(PG_SDT_QUERY_PLAN_START) |
				   SDT_BIT(PG_SDT_QUERY_PLAN_DONE)},
	{"execute",	   SDT_BIT(PG_SDT_QUERY_EXECUTE_START) |
				   SDT_BIT(PG_SDT_QUERY_EXECUTE_DONE)},
	{"sort",	   SDT_BIT(PG_SDT_SORT_START) | SDT_BIT(PG_SDT_SORT_DONE)},
	{"smgr_read",  SDT_BITS_SMGR_READ},
	{"smgr_write", SDT_BITS_SMGR_WRITE},
	{"smgr",	   SDT_BITS_SMGR_READ | SDT_BITS_SMGR_WRITE},
	{"syncrep",	   SDT_BIT(PG_SDT_SYNCREP_WAIT_START) |
				   SDT_BIT(PG_SDT_SYNCREP_WAIT_DONE)},
	{"replica",	   SDT_BIT(PG_SDT_RECOVERY_XACT_COMMIT)},
	{"lock_wait",  SDT_BIT(PG_SDT_LOCK_WAIT_START) |
				   SDT_BIT(PG_SDT_LOCK_WAIT_DONE)},
	{"all",		   SDT_BITS_ALL},
	{"none",	   0},
};


/* -----------------------------------------------------------------------
 * Forward declarations
 * ----------------------------------------------------------------------- */

static void otel_sdt_hook(int id, const PgSdtArg *args, int nargs);
static void otel_sdt_xact_cb(XactEvent event, void *arg);
static void sdt_discard_open_spans(void);
static bool sdt_probes_parse(const char *value, uint64 *mask_out);
static bool sdt_probes_check_hook(char **newval, void **extra, GucSource source);
static void sdt_probes_assign_hook(const char *newval, void *extra);


/* -----------------------------------------------------------------------
 * GUC list parsing: token list -> probe-enable bitmask
 *
 * Parses a comma-separated family-token list into a uint64 bitmask.  Used by
 * both the check hook (to validate + precompute the mask) at runtime.  The
 * input is duplicated before SplitIdentifierString because that routine
 * modifies its argument in place.  Returns true on success; on an unknown
 * token it reports via GUC_check_errdetail and returns false.
 * ----------------------------------------------------------------------- */

static bool
sdt_probes_parse(const char *value, uint64 *mask_out)
{
	char	   *rawstring;
	List	   *elemlist;
	ListCell   *lc;
	uint64		mask = 0;

	/* SplitIdentifierString scribbles on its input; work on a copy. */
	rawstring = pstrdup(value);

	if (!SplitIdentifierString(rawstring, ',', &elemlist))
	{
		GUC_check_errdetail("List syntax is invalid.");
		pfree(rawstring);
		list_free(elemlist);
		return false;
	}

	foreach(lc, elemlist)
	{
		const char *tok = (const char *) lfirst(lc);
		bool		found = false;
		size_t		i;

		for (i = 0; i < lengthof(sdt_family_map); i++)
		{
			if (pg_strcasecmp(tok, sdt_family_map[i].name) == 0)
			{
				mask |= sdt_family_map[i].bits;
				found = true;
				break;
			}
		}

		if (!found)
		{
			GUC_check_errdetail("Unrecognized SDT probe family \"%s\".", tok);
			pfree(rawstring);
			list_free(elemlist);
			return false;
		}
	}

	pfree(rawstring);
	list_free(elemlist);
	*mask_out = mask;
	return true;
}

/*
 * check_hook for otel.trace_sdt_probes.  Validates the token list and stashes
 * the precomputed mask in *extra so the assign hook need not re-parse.
 */
static bool
sdt_probes_check_hook(char **newval, void **extra, GucSource source)
{
	uint64		mask = 0;
	uint64	   *extra_mask;

	/* An empty / NULL value means "none". */
	if (*newval != NULL && (*newval)[0] != '\0')
	{
		if (!sdt_probes_parse(*newval, &mask))
			return false;
	}

	extra_mask = (uint64 *) guc_malloc(LOG, sizeof(uint64));
	if (extra_mask == NULL)
		return false;
	*extra_mask = mask;
	*extra = (void *) extra_mask;
	return true;
}

/*
 * assign_hook for otel.trace_sdt_probes.  Writes the precomputed mask from
 * *extra into the core symbol that gates the TRACE_POSTGRESQL_* macros.
 */
static void
sdt_probes_assign_hook(const char *newval, void *extra)
{
	uint64	   *extra_mask = (uint64 *) extra;

	pg_sdt_probe_enabled_mask = extra_mask ? *extra_mask : 0;
}


/* -----------------------------------------------------------------------
 * otel_sdt_install
 *
 * Called once from _PG_init in otel_postgres_tracing.c.  Registers
 * GUCs, the xact callback, and (last) the probe hook.
 * ----------------------------------------------------------------------- */

void
otel_sdt_install(void)
{
	DefineCustomStringVariable("otel.trace_sdt_probes",
							   "SDT probe families to emit OTel spans for.",
							   "Comma-separated list of probe families; each "
							   "family enables a matched START/DONE probe pair "
							   "(or the standby replica-apply probe).  Recognized "
							   "tokens: txn, query, parse, rewrite, plan, execute, "
							   "sort, smgr_read, smgr_write, smgr, syncrep, "
							   "replica, lock_wait, plus all and none.  smgr "
							   "families are high-volume and off by default.  "
							   "This list drives pg_sdt_probe_enabled_mask, which "
							   "gates the probes at the call site; families not "
							   "listed never call into this extension.",
							   &otel_trace_sdt_probes_str,
							   "query,parse,rewrite,plan,execute,sort,syncrep,replica,lock_wait",
							   PGC_USERSET,
							   GUC_LIST_INPUT,
							   sdt_probes_check_hook,
							   sdt_probes_assign_hook,
							   NULL);

	RegisterXactCallback(otel_sdt_xact_cb, NULL);

	/* Install the hook last so all state is ready. */
	pg_sdt_probe_hook = otel_sdt_hook;
}


/* -----------------------------------------------------------------------
 * Lock decode helpers (for the pg.lock.wait span)
 * ----------------------------------------------------------------------- */

/*
 * Heavyweight lock mode names, indexed by lock mode 1..8.  Index 0 (NoLock)
 * is unused by the lock-wait probe but kept for natural indexing.  Out-of-
 * range modes fall back to the integer in the caller.
 */
static const char *const sdt_lockmode_names[] = {
	"NoLock",					/* 0 */
	"AccessShareLock",			/* 1 */
	"RowShareLock",				/* 2 */
	"RowExclusiveLock",			/* 3 */
	"ShareUpdateExclusiveLock", /* 4 */
	"ShareLock",				/* 5 */
	"ShareRowExclusiveLock",	/* 6 */
	"ExclusiveLock",			/* 7 */
	"AccessExclusiveLock",		/* 8 */
};

/*
 * LockTagType name decode.  Returns a static string for in-range types, or
 * NULL when out of range (caller emits the integer instead).  The core
 * LockTagTypeNames[] array carries these strings, but we keep a private copy
 * so the bridge does not depend on that symbol's exact element count.
 */
static const char *
sdt_locktag_type_name(int t)
{
	static const char *const names[] = {
		"relation",				/* LOCKTAG_RELATION */
		"extend",				/* LOCKTAG_RELATION_EXTEND */
		"frozenid",				/* LOCKTAG_DATABASE_FROZEN_IDS */
		"page",					/* LOCKTAG_PAGE */
		"tuple",				/* LOCKTAG_TUPLE */
		"transactionid",		/* LOCKTAG_TRANSACTION */
		"virtualxid",			/* LOCKTAG_VIRTUALTRANSACTION */
		"spectoken",			/* LOCKTAG_SPECULATIVE_TOKEN */
		"object",				/* LOCKTAG_OBJECT */
		"userlock",				/* LOCKTAG_USERLOCK */
		"advisory",				/* LOCKTAG_ADVISORY */
		"applytransaction",		/* LOCKTAG_APPLY_TRANSACTION */
	};

	if (t >= 0 && t < (int) lengthof(names))
		return names[t];
	return NULL;
}


/* -----------------------------------------------------------------------
 * otel_sdt_hook
 *
 * Main dispatch: decide whether to start or finish a span for each probe.
 * ----------------------------------------------------------------------- */

static void
otel_sdt_hook(int id, const PgSdtArg *args, int nargs)
{
	OtelSpanRef s;
	const char *span_name;
	bool		is_start;

	/*
	 * No master enable gate here any more: pg_sdt_probe_enabled_mask gates
	 * each TRACE_POSTGRESQL_* macro at the call site, so this hook is only
	 * reached for probe families that the otel.trace_sdt_probes GUC turned
	 * on.  The per-family in-hook gates below are likewise gone.
	 */

	/* Only trace client-connected backends — UNLESS this is the replica
	 * apply probe, which fires in the startup/recovery process where
	 * MyProcPort is always NULL. */
	if (MyProcPort == NULL && (PgSdtProbeId) id != PG_SDT_RECOVERY_XACT_COMMIT)
		return;

	/* otel_api refuses every call inside a critical section. */
	if (CritSectionCount > 0)
		return;

	/* ---- Classify the probe ---- */
	is_start = false;
	span_name = NULL;

	switch ((PgSdtProbeId) id)
	{
		/* --- Transaction ---
		 *
		 * The transaction span is the root of its OWN trace and spans the
		 * whole transaction lifetime.  It is detached (never pushed onto
		 * the producer's active stack), so it cannot interleave with the
		 * per-statement query spans and cause out-of-order emits.  The
		 * per-statement query traces are associated with it via span links
		 * (added in the START path for the query-root probe).
		 *
		 * Both commit and abort are also realign points for the
		 * per-statement stack: by the time either fires, every
		 * per-statement SDT span has paired its DONE, so a nonzero sdt_top
		 * means an asymmetric probe (e.g. a utility statement such as
		 * CREATE TABLE AS that runs an executor underneath) left an entry
		 * dangling.  The producer already released its slot via its own
		 * resource-owner / stack-unwind machinery; we just reset our own
		 * index so the next transaction starts from a clean baseline.
		 */
		case PG_SDT_TRANSACTION_START:
			if (txn_span.v == 0)
			{
				txn_span = otel_span_start(.tracer = &otel_pg_tracer,
										   .name = "pg.txn",
										   .kind = OTEL_SPAN_KIND_INTERNAL,
										   .parent = OTEL_PARENT_ROOT,
										   .owner = TopTransactionResourceOwner,
										   .detached = true);
				otel_span_set_str(txn_span, OTEL_PG_SPAN_SOURCE,
								  SDT_SPAN_SOURCE_ATTR_VAL);
			}
			return;
		case PG_SDT_TRANSACTION_COMMIT:
			if (txn_span.v != 0)
			{
				otel_span_end(txn_span);
				txn_span = OTEL_SPAN_NONE;
			}
			sdt_top = 0;
			return;
		case PG_SDT_TRANSACTION_ABORT:
			if (txn_span.v != 0)
			{
				otel_span_set_status(txn_span, OTEL_STATUS_ERROR,
									 "transaction aborted");
				otel_span_end(txn_span);
				txn_span = OTEL_SPAN_NONE;
			}
			sdt_discard_open_spans();
			return;

		/* --- Query --- */
		case PG_SDT_QUERY_START:
			span_name = "pg.query";
			is_start = true;
			break;
		case PG_SDT_QUERY_DONE:
			span_name = "pg.query";
			break;

		/* --- Parse --- */
		case PG_SDT_QUERY_PARSE_START:
			span_name = "pg.parse";
			is_start = true;
			break;
		case PG_SDT_QUERY_PARSE_DONE:
			span_name = "pg.parse";
			break;

		/* --- Rewrite --- */
		case PG_SDT_QUERY_REWRITE_START:
			span_name = "pg.rewrite";
			is_start = true;
			break;
		case PG_SDT_QUERY_REWRITE_DONE:
			span_name = "pg.rewrite";
			break;

		/* --- Plan --- */
		case PG_SDT_QUERY_PLAN_START:
			span_name = "pg.plan";
			is_start = true;
			break;
		case PG_SDT_QUERY_PLAN_DONE:
			span_name = "pg.plan";
			break;

		/* --- Execute --- */
		case PG_SDT_QUERY_EXECUTE_START:
			span_name = "pg.execute";
			is_start = true;
			break;
		case PG_SDT_QUERY_EXECUTE_DONE:
			span_name = "pg.execute";
			break;

		/* --- Sort --- */
		case PG_SDT_SORT_START:
			span_name = "pg.sort";
			is_start = true;
			break;
		case PG_SDT_SORT_DONE:
			span_name = "pg.sort";
			break;

		/* --- Storage manager (smgr) --- */
		case PG_SDT_SMGR_MD_READ_START:
			span_name = "pg.smgr.read";
			is_start = true;
			break;
		case PG_SDT_SMGR_MD_READ_DONE:
			span_name = "pg.smgr.read";
			break;
		case PG_SDT_SMGR_MD_WRITE_START:
			span_name = "pg.smgr.write";
			is_start = true;
			break;
		case PG_SDT_SMGR_MD_WRITE_DONE:
			span_name = "pg.smgr.write";
			break;

		/* --- Syncrep wait (primary side) --- */
		case PG_SDT_SYNCREP_WAIT_START:
			span_name = "pg.syncrep.wait";
			is_start = true;
			break;
		case PG_SDT_SYNCREP_WAIT_DONE:
			span_name = "pg.syncrep.wait";
			break;

		/* --- Lock wait (heavyweight lock manager) --- */
		case PG_SDT_LOCK_WAIT_START:
			span_name = "pg.lock.wait";
			is_start = true;
			break;
		case PG_SDT_LOCK_WAIT_DONE:
			span_name = "pg.lock.wait";
			break;

		/* --- Replica apply (standby side) ---
		 *
		 * This probe fires in the startup/recovery process when it replays
		 * a commit record that carries a W3C trace context embedded by the
		 * primary.  There is NO active span stack here (recovery runs in a
		 * single long-lived backend with no statement context), so we
		 * start a detached, point-in-time span directly parented on the
		 * parsed traceparent and emit it immediately.
		 */
		case PG_SDT_RECOVERY_XACT_COMMIT:
		{
			OtelSpanContext parent_ctx;
			const char *traceparent;
			long		commit_lsn_long;
			char		lsn_str[32];
			TimestampTz now;
			OtelSpanRef replica_span;

			if (nargs < 2 || args[0].tag != 's' || args[1].tag != 'i')
				return;

			traceparent = args[0].v.s;
			commit_lsn_long = (long) args[1].v.i;

			if (traceparent == NULL ||
				!otel_traceparent_parse(traceparent, &parent_ctx))
				return;

			now = GetCurrentTimestamp();

			replica_span = otel_span_start(.tracer = &otel_pg_tracer,
										   .name = "pg.replica.apply",
										   .kind = OTEL_SPAN_KIND_CONSUMER,
										   .parent = OTEL_PARENT_CONTEXT,
										   .parent_ctx = &parent_ctx,
										   .detached = true,
										   .start_time = now);
			otel_span_set_str(replica_span, OTEL_PG_SPAN_SOURCE,
							  SDT_SPAN_SOURCE_ATTR_VAL);

			/* Attribute: commit LSN formatted as %X/%08X */
			snprintf(lsn_str, sizeof(lsn_str), "%lX/%08lX",
					 (unsigned long) ((unsigned long long) commit_lsn_long >> 32),
					 (unsigned long) ((unsigned long long) commit_lsn_long & 0xFFFFFFFF));
			otel_span_set_str(replica_span, OTEL_ATTR_PG_COMMIT_LSN, lsn_str);

			/* Point-in-time span: end == start. */
			otel_span_end_at(replica_span, now);
			return;
		}

		default:
			return;				/* unknown probe — ignore */
	}

	/* ---- START path ---- */
	if (is_start)
	{
		OtelSpanContext ctx;

		/*
		 * Only build SDT spans when a child would inherit a context: the
		 * top of our own active stack (nested under a span this file
		 * already pushed, or under otel_trace.c's statement span), or the
		 * backend's root/parallel-leader context.  Without either, this
		 * would start a brand-new, disconnected root trace, which is not
		 * what a producer that shouldn't originate traces on its own
		 * should do.  Since this check is stable for a statement's
		 * duration and every nested START inherits its parent's context
		 * (or lack of one), a single check per START suffices --- there is
		 * no need to cache a once-per-query decision separately.
		 */
		if (!otel_span_context_of(OTEL_SPAN_NONE, &ctx))
			return;

		if (sdt_top >= SDT_STACK_SIZE)
			return;				/* stack full; drop this probe */

		s = otel_span_start(.tracer = &otel_pg_tracer,
							.name = span_name,
							.kind = OTEL_SPAN_KIND_INTERNAL);
		otel_span_set_str(s, OTEL_PG_SPAN_SOURCE, SDT_SPAN_SOURCE_ATTR_VAL);

		/*
		 * Add useful attributes for query-level probes.  nargs and arg
		 * layout are probe-specific; guard carefully.
		 */
		if ((PgSdtProbeId) id == PG_SDT_QUERY_START ||
			(PgSdtProbeId) id == PG_SDT_QUERY_PARSE_START ||
			(PgSdtProbeId) id == PG_SDT_QUERY_REWRITE_START)
		{
			/* First arg is the query string ('s') for these probes. */
			if (nargs >= 1 && args[0].tag == 's' && args[0].v.s != NULL)
				OTEL_SPAN_SET_STR_IF_RECORDING(s, OTEL_SC_DB_QUERY_TEXT,
											   args[0].v.s);
		}

		if ((PgSdtProbeId) id == PG_SDT_SYNCREP_WAIT_START)
		{
			/* First arg is the commit LSN ('i') for this probe. */
			if (nargs >= 1 && args[0].tag == 'i')
			{
				char		lsn_str[32];
				long		lsn_long = (long) args[0].v.i;

				snprintf(lsn_str, sizeof(lsn_str), "%lX/%08lX",
						 (unsigned long) ((unsigned long long) lsn_long >> 32),
						 (unsigned long) ((unsigned long long) lsn_long & 0xFFFFFFFF));
				otel_span_set_str(s, OTEL_ATTR_PG_COMMIT_LSN, lsn_str);
			}
		}

		if ((PgSdtProbeId) id == PG_SDT_LOCK_WAIT_START)
		{
			/* Six int64 args: locktag field1..4, locktag_type, lock mode. */
			if (nargs >= 6 &&
				args[0].tag == 'i' && args[1].tag == 'i' &&
				args[2].tag == 'i' && args[3].tag == 'i' &&
				args[4].tag == 'i' && args[5].tag == 'i')
			{
				int64		field1 = args[0].v.i;
				int64		field2 = args[1].v.i;
				int64		field3 = args[2].v.i;
				int64		field4 = args[3].v.i;
				int			locktag_type = (int) args[4].v.i;
				int			mode = (int) args[5].v.i;
				const char *tname = sdt_locktag_type_name(locktag_type);

				if (tname != NULL)
					otel_span_set_str(s, OTEL_ATTR_PG_LOCK_TYPE, tname);
				else
					otel_span_set_int(s, OTEL_ATTR_PG_LOCK_TYPE, locktag_type);

				if (mode >= 1 && mode < (int) lengthof(sdt_lockmode_names))
					otel_span_set_str(s, OTEL_ATTR_PG_LOCK_MODE,
									  sdt_lockmode_names[mode]);
				else
					otel_span_set_int(s, OTEL_ATTR_PG_LOCK_MODE, mode);

				switch (locktag_type)
				{
					case LOCKTAG_RELATION:
					case LOCKTAG_RELATION_EXTEND:
					case LOCKTAG_PAGE:
					case LOCKTAG_TUPLE:
						otel_span_set_int(s, OTEL_ATTR_PG_LOCK_DBOID, field1);
						otel_span_set_int(s, OTEL_ATTR_PG_LOCK_RELID, field2);
						if (locktag_type == LOCKTAG_PAGE ||
							locktag_type == LOCKTAG_TUPLE)
							otel_span_set_int(s, OTEL_ATTR_PG_LOCK_BLOCK, field3);
						if (locktag_type == LOCKTAG_TUPLE)
							otel_span_set_int(s, OTEL_ATTR_PG_LOCK_OFFSET, field4);
						break;

					case LOCKTAG_TRANSACTION:
						otel_span_set_int(s, OTEL_ATTR_PG_LOCK_XID, field1);
						break;

					case LOCKTAG_VIRTUALTRANSACTION:
						otel_span_set_int(s, OTEL_ATTR_PG_LOCK_PROCNO, field1);
						otel_span_set_int(s, OTEL_ATTR_PG_LOCK_LOCALXID, field2);
						break;

					default:
						/* Generic: emit field1..4 as-is. */
						otel_span_set_int(s, OTEL_ATTR_PG_LOCK_FIELD1, field1);
						otel_span_set_int(s, OTEL_ATTR_PG_LOCK_FIELD2, field2);
						otel_span_set_int(s, OTEL_ATTR_PG_LOCK_FIELD3, field3);
						otel_span_set_int(s, OTEL_ATTR_PG_LOCK_FIELD4, field4);
						break;
				}
			}
		}

		sdt_stack[sdt_top++] = s;

		/*
		 * Associate the per-statement query trace with the
		 * transaction-lifetime span (a separate trace).  pg.query is the
		 * root of the per-statement SDT subtree, so we add bidirectional
		 * span links: the query span links to the transaction, and the
		 * transaction span accumulates a link to each query that ran in
		 * it.
		 */
		if ((PgSdtProbeId) id == PG_SDT_QUERY_START && txn_span.v != 0)
		{
			OtelSpanContext txn_ctx;
			OtelSpanContext query_ctx;

			if (otel_span_context_of(txn_span, &txn_ctx))
				otel_span_add_link(s, &txn_ctx);
			if (otel_span_context_of(s, &query_ctx))
				otel_span_add_link(txn_span, &query_ctx);
		}

		return;
	}

	/* ---- DONE path ---- */
	if (sdt_top <= 0)
		return;					/* no matching START on our stack */

	s = sdt_stack[--sdt_top];

	/*
	 * SDT start/done pairs are LIFO in the common case so our span is the
	 * top; for the few that are not strictly nested (interleaved sorts, or
	 * a utility statement such as CREATE TABLE AS that runs an executor
	 * underneath) otel_api unwinds the entries above ours, each exported
	 * with ERROR status (plus a benign WARNING).  The out-of-order
	 * sibling bridge span(s) are exported that way and the lower
	 * statement span from otel_trace.c is undisturbed.  The trace stays
	 * coherent.
	 */
	otel_span_end(s);
}


/*
 * Discard the per-statement spans still open on our stack.  On abort their
 * DONE probes never fire.  pg.query starts before the statement's
 * transaction, with no resource owner, so it is a session span that
 * otel_api would otherwise keep on its active stack, and every later span
 * in the backend would be parented under it.  They have no resource
 * owner to unwind them on abort, so this bridge discards them itself,
 * explicitly, rather than exporting them with ERROR status.
 */
static void
sdt_discard_open_spans(void)
{
	while (sdt_top > 0)
		otel_span_discard(sdt_stack[--sdt_top]);
}

/* -----------------------------------------------------------------------
 * otel_sdt_xact_cb
 *
 * Transaction event callback.  On abort, reset sdt_top so our stack
 * index stays in sync (spans still open are unwound by otel_api's own
 * resource-owner release).  On commit we do nothing --- the
 * TRANSACTION_COMMIT probe fires before the xact callback, so the
 * pg.txn span has already been ended by otel_sdt_hook.
 * ----------------------------------------------------------------------- */

static void
otel_sdt_xact_cb(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			/*
			 * The TRANSACTION_ABORT probe path in otel_sdt_hook has
			 * already handled the reset for the hook-initiated abort
			 * path.  This callback catches aborts that do NOT fire the
			 * probe (e.g. errors before the probe site, DDL command
			 * rollbacks, ROLLBACK TO SAVEPOINT at the transaction
			 * level, parallel-worker failures).  txn_span itself, if
			 * still open, is force-ended by its TopTransactionResourceOwner
			 * being released.
			 */
			sdt_discard_open_spans();
			txn_span = OTEL_SPAN_NONE;
			break;

		default:
			break;
	}
}

/*
 * otel_sdt_get_txn_context
 *		Snapshot the identity of the currently-active pg.txn span.
 *
 * Returns false when no transaction span is active, in which case *out is
 * left untouched.  Used by otel_trace.c to link a statement span to the
 * enclosing transaction even when no traceparent was propagated (the SDT
 * pg.query path is gated on there being *some* context to nest under, but
 * pg.txn is always live once a transaction has started).
 */
bool
otel_sdt_get_txn_context(OtelSpanContext *out)
{
	if (txn_span.v == 0)
		return false;
	return otel_span_context_of(txn_span, out);
}

/*
 * otel_sdt_link_stmt_to_txn
 *		Add a link from the active pg.txn span back to a statement span.
 *
 * No-op when no transaction span is active.  Completes the bidirectional
 * link begun by otel_sdt_get_txn_context (the statement span links to the
 * txn; this links the txn back to the statement).
 */
void
otel_sdt_link_stmt_to_txn(const OtelSpanContext *stmt_ctx)
{
	if (txn_span.v == 0)
		return;
	otel_span_add_link(txn_span, stmt_ctx);
}

#else							/* !PG_HAVE_SDT_PROBE_HOOK */

/*
 * Stock PostgreSQL without the SDT-bridge core patch (no
 * PG_HAVE_SDT_PROBE_HOOK, no utils/pg_sdt_probe.h, no pg_sdt_probe_hook
 * global).  The probe -> span bridge is compiled out entirely so the
 * shared library has no unresolved reference to pg_sdt_probe_hook and
 * loads cleanly.  otel_sdt_install() is a no-op; the otel.trace_sdt_probes
 * GUC is simply not defined (SET on it reports "unrecognized configuration
 * parameter", as expected when the feature is unavailable).
 */
void
otel_sdt_install(void)
{
	/* nothing to install: core has no SDT probe hook */
}

#endif							/* PG_HAVE_SDT_PROBE_HOOK */
