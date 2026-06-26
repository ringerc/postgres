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
 *      span stored in a fixed static pool (sdt_pool) used as a LIFO stack
 *      (sdt_top) and pushed onto the producer's active stack so it nests
 *      under the propagated query trace.
 *        - START probe: span_init, optional attributes, link+push.
 *        - DONE probe:  pop pool entry, set end_time, emit.
 *
 *   2. The transaction span (pg.txn).  It lives in its OWN trace and
 *      spans the whole transaction; it is emitted directly and never
 *      pushed onto the active stack (so it cannot interleave with the
 *      per-statement spans).  The per-statement query traces are tied to
 *      it with bidirectional span links (pg.txn <-> pg.query).
 *        - transaction__start:  span_init + fresh trace_id; mark active.
 *        - transaction__commit: emit (status UNSET); realign sdt_top.
 *        - transaction__abort:  emit (status ERROR);  realign sdt_top.
 *
 * Design notes / constraints
 * --------------------------
 *   * sdt_pool / txn_span are static slabs.  Per-statement spans use
 *	   OTEL_UNWIND_DROP so the unwind callback never dereferences the
 *	   OtelSpan pointer (it stores NULL for DROP entries) --- no
 *	   dangling-pointer hazard despite the slab not being per-context.
 *   * commit/abort also realign sdt_top: per-statement DONEs have all
 *	   fired by then, so a nonzero sdt_top means an asymmetric probe
 *	   (e.g. a utility statement that runs an executor) left a pool entry
 *	   dangling; the producer already dropped the active-stack entry.
 *   * Per-statement START/DONE pairing assumes LIFO nesting.  A few
 *	   probe pairs are not strictly nested (interleaved sorts, utility-
 *	   with-executor statements); an out-of-order emit there logs a
 *	   benign producer WARNING and drops a sibling DROP span.  The trace
 *	   stays coherent and the OTEL_UNWIND_ERROR statement span is never
 *	   corrupted.  Acceptable for DEMO-quality instrumentation.
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
#include "storage/lock.h"		/* LOCKTAG_* / lock mode names */
#include "storage/locktag.h"
#include "utils/guc.h"
#include "utils/timestamp.h"
#include "utils/pg_sdt_probe.h"
#include "utils/varlena.h"		/* SplitIdentifierString */

#include <otel_api/otel.h>


/* -----------------------------------------------------------------------
 * Module-static state
 * ----------------------------------------------------------------------- */

/*
 * Fixed pool of OtelSpan slabs, one per nested SDT probe depth.
 * Accessed as a LIFO stack via sdt_top.  64 entries mirrors the
 * MAX_SPAN_STACK_DEPTH in otel_producer.c.
 */
#define SDT_POOL_SIZE	64

static OtelSpan			sdt_pool[SDT_POOL_SIZE];
static int				sdt_top = 0;	/* next free slot; 0 == empty */

/*
 * Per-slot scratch storage for integer-valued attributes.
 *
 * span_add_attribute_string does NOT copy its value: the pointer must stay
 * valid until the span is emitted at the matching DONE probe, which happens
 * after the START call frame has returned.  A stack-local format buffer would
 * therefore dangle.  Each pool slot gets a fixed set of small buffers here
 * (lifetime == the slab's), one per integer attribute a START might add.
 * SDT_ATTR_BUFS bounds the number of formatted-int attributes per span (the
 * lock-wait span uses at most 6: type-fallback, mode-fallback, plus up to 4
 * field/target values).
 */
#define SDT_ATTR_BUFS	6
#define SDT_ATTR_BUFLEN	32
static char				sdt_attr_scratch[SDT_POOL_SIZE][SDT_ATTR_BUFS][SDT_ATTR_BUFLEN];

/* InstrumentationScope for this bridge's spans. */
/*
 * TODO: tag every span this bridge emits with an explicit
 * pg.otel.span_source = "sdt_probe" attribute so consumers can cleanly
 * separate these intercepted-tracepoint spans (pg.*) from the hook-based
 * spans in otel_trace.c (pgsql.execute / command-tag utility spans). The
 * sdt_scope below is meant to distinguish them, but the Rust exporter
 * collapses ScopeName to the crate name on export, so scope can't be used
 * downstream. See the fuller TODO in otel_trace.c.
 */
static const OtelInstrumentationScope *sdt_scope = NULL;

/*
 * Transaction-lifetime span.  Unlike the per-statement spans, this one
 * lives in its OWN trace (fresh trace_id at transaction__start, emitted
 * at commit/abort) and is NOT pushed onto the producer's active stack,
 * so it can never cause out-of-order emits.  It is associated with the
 * per-statement query traces via span links (added in the START path
 * for the query-root probe).  Stored in its own slab because its
 * lifetime spans many statements.
 */
static OtelSpan			txn_span;
static bool				txn_active = false;

/*
 * GUC: which SDT probe FAMILIES are enabled.  This is a GUC_LIST_INPUT
 * string GUC (otel.trace_sdt_probes); its check/assign hooks translate the
 * token list into the core symbol pg_sdt_probe_enabled_mask, which is what
 * actually gates each TRACE_POSTGRESQL_* macro at the call site.  Until the
 * mask is set, NOTHING calls this hook.  The backing string only exists so
 * SHOW / pg_settings can echo the configured value.
 */
static char			   *otel_trace_sdt_probes_str = NULL;

/* smgr (storage manager) spans are off by default because smgr probes fire
 * on every buffer read/write and can be very high-volume; they are enabled
 * by including the smgr / smgr_read / smgr_write token in the list GUC. */
/*
 * TODO: investigate collapsing rapid sequences of similar spans (e.g. the
 * thousands of per-block pg.smgr.write spans a single CTAS emits -- one trace
 * was observed with 5770 of them) into a single aggregated span carrying a
 * count (and perhaps min/max/total duration, block range). This would make
 * smgr/buffer-level tracing affordable enough to leave on. Evaluate where to do
 * it and the trade-offs:
 *   - span producer (here): cheapest, keeps the exported volume down at source;
 *     coalesce consecutive same-kind probe pairs within a parent into one span
 *     with an attribute like pg.smgr.write.count. Loses per-op timing detail.
 *   - exporter (postgres_otel_tracing_demo): could batch/fold before OTLP send;
 *     more context than the collector but still per-process.
 *   - otel-collector: a transform/groupby or a custom processor downstream;
 *     keeps producers simple and is reconfigurable without a Postgres restart,
 *     but the full span volume still crosses the wire to the collector.
 * Note tracing semantics: an aggregated "span" with a count is closer to a
 * metric/event than a true span -- consider whether a span event or a counter
 * metric is the better representation.
 * Prior art: Elastic APM / EDOT (Elastic Distribution of OpenTelemetry) has
 * "span compression", which folds repeated similar/exact-match spans into one
 * composite span with a count + aggregate duration. Study its model (exact-match
 * vs same-kind compression, the configurable duration threshold) before designing.
 */


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
static void sdt_bytes_to_hex(const unsigned char *src, size_t n, char *dst);
static bool sdt_probes_parse(const char *value, uint64 *mask_out);
static bool sdt_probes_check_hook(char **newval, void **extra, GucSource source);
static void sdt_probes_assign_hook(const char *newval, void *extra);


/* -----------------------------------------------------------------------
 * Helper: bytes to lowercase hex (for synthesizing a fresh trace_id)
 * ----------------------------------------------------------------------- */

static void
sdt_bytes_to_hex(const unsigned char *src, size_t n, char *dst)
{
	static const char hex[] = "0123456789abcdef";
	size_t		i;

	for (i = 0; i < n; i++)
	{
		dst[i * 2]     = hex[(src[i] >> 4) & 0xf];
		dst[i * 2 + 1] = hex[src[i] & 0xf];
	}
	dst[n * 2] = '\0';
}


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
	const OtelTracingApi *api;
	OtelSpan   *s;
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

	api = otel_pg_ensure();
	if (api == NULL)
		return;

	if (!api->any_emit_consumer_present())
		return;

	/* ---- Classify the probe ---- */
	is_start = false;
	span_name = NULL;

	switch ((PgSdtProbeId) id)
	{
		/* --- Transaction ---
		 *
		 * The transaction span lives in its OWN trace and spans the whole
		 * transaction lifetime.  It is emitted directly (never pushed onto
		 * the producer's active stack), so it cannot interleave with the
		 * per-statement query spans and cause out-of-order emits.  The
		 * per-statement query traces are associated with it via span links
		 * (added in the START path for the query-root probe), which is the
		 * correct OTel model for a long-lived span related to many shorter
		 * traces.
		 *
		 * Both commit and abort are also realign points for the
		 * per-statement pool: by the time either fires, every per-statement
		 * SDT span has paired its DONE, so a nonzero sdt_top means an
		 * asymmetric probe (e.g. a utility statement such as CREATE TABLE AS
		 * that runs an executor underneath) left a pool entry dangling.  The
		 * producer already dropped the matching active-stack entry via its
		 * MemoryContext unwind under OTEL_UNWIND_DROP; we reset our own pool
		 * index so the next transaction starts from a clean baseline.
		 */
		case PG_SDT_TRANSACTION_START:
			if (!txn_active)
			{
				unsigned char buf[16];

				/* span_init zeroes the struct, generates a span_id, and
				 * sets start_time.  We give it a fresh, independent
				 * trace_id so the transaction is its own trace. */
				api->span_init(&txn_span, sdt_scope, "pg.txn",
							   OTEL_SPAN_KIND_INTERNAL);
				if (!pg_strong_random(buf, sizeof(buf)))
					memset(buf, 0xa5, sizeof(buf));
				sdt_bytes_to_hex(buf, sizeof(buf), txn_span.trace_id);
				strcpy(txn_span.trace_flags, "01");
				/* parent_span_id stays empty: root of its own trace. */
				txn_active = true;
			}
			return;
		case PG_SDT_TRANSACTION_COMMIT:
			if (txn_active)
			{
				txn_span.end_time = GetCurrentTimestamp();
				txn_span.status = OTEL_STATUS_UNSET;
				/* Not on the active stack: span_emit just dispatches it. */
				api->span_emit(&txn_span);
				txn_active = false;
			}
			sdt_top = 0;
			return;
		case PG_SDT_TRANSACTION_ABORT:
			if (txn_active)
			{
				txn_span.end_time = GetCurrentTimestamp();
				otel_span_set_status(&txn_span, OTEL_STATUS_ERROR,
									 "transaction aborted");
				api->span_emit(&txn_span);
				txn_active = false;
			}
			/*
			 * TODO: on abort, emit the still-open per-statement spans on the
			 * stack (sdt_top > 0) with ERROR status instead of discarding them
			 * via the reset below. When a statement errors mid-execution the
			 * QUERY_EXECUTE_DONE / QUERY_DONE (and on a plan-time error,
			 * QUERY_PLAN_DONE) probes never fire, so pg.execute / pg.query /
			 * pg.plan stay open and are dropped here -- an error trace then
			 * shows only the completed-pair spans plus the executor-hook
			 * pgsql.execute (which captures the error via the elog hook).
			 * Observed: trace 2cfc6415... (runtime 1/(i-i)) lost its
			 * pg.execute/pg.query subtree; trace 01da50ab... (plan-time 1/0)
			 * captured no server-side error at all because no executor span
			 * opened and the open pg.plan was discarded. Fix: unwind the stack
			 * top-down, set each span's end_time + ERROR status, and span_emit
			 * in LIFO order so the producer's stack contract holds (otherwise
			 * otel_producer.c logs "span emitted out of stack order; N span(s)
			 * above will be unwound"). This would also make plan/parse-phase
			 * errors visible, not just execute-phase ones.
			 */
			sdt_top = 0;
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
		 * single long-lived backend with no statement context), so we build
		 * a self-contained span directly, set its trace identity from the
		 * parsed traceparent arg, and emit it immediately.
		 */
		case PG_SDT_RECOVERY_XACT_COMMIT:
		{
			OtelSpan	replica_span;
			const char *traceparent;
			long		commit_lsn_long;
			char		lsn_str[32];

			if (nargs < 2 || args[0].tag != 's' || args[1].tag != 'i')
				return;

			traceparent = args[0].v.s;
			commit_lsn_long = (long) args[1].v.i;

			/*
			 * Validate traceparent format: "00-<32hex>-<16hex>-<2hex>".
			 * Check minimum length and the mandatory hyphen positions.
			 */
			if (traceparent == NULL ||
				strlen(traceparent) < 55 ||
				traceparent[2] != '-' ||
				traceparent[35] != '-' ||
				traceparent[52] != '-')
				return;

			if (sdt_scope == NULL && otel_pg_tracer != NULL)
				sdt_scope = otel_pg_tracer;

			/*
			 * span_init zeroes the struct, generates a fresh span_id, and
			 * sets start_time.  We then overwrite trace_id, parent_span_id,
			 * and trace_flags from the propagated traceparent.
			 */
			api->span_init(&replica_span, sdt_scope, "pg.replica.apply",
						   OTEL_SPAN_KIND_CONSUMER);

			/* trace_id: 32 hex chars at offset 3 */
			memcpy(replica_span.trace_id, traceparent + 3, 32);
			replica_span.trace_id[32] = '\0';

			/* parent_span_id: the primary's span_id at offset 36, 16 hex chars */
			memcpy(replica_span.parent_span_id, traceparent + 36, 16);
			replica_span.parent_span_id[16] = '\0';

			/* trace_flags: 2 hex chars at offset 53 */
			memcpy(replica_span.trace_flags, traceparent + 53, 2);
			replica_span.trace_flags[2] = '\0';

			/* Point-in-time span: end == start (apply is instantaneous here). */
			replica_span.end_time = replica_span.start_time;
			replica_span.status = OTEL_STATUS_UNSET;

			/* Attribute: commit LSN formatted as %X/%08X */
			snprintf(lsn_str, sizeof(lsn_str), "%lX/%08lX",
					 (unsigned long) ((unsigned long long) commit_lsn_long >> 32),
					 (unsigned long) ((unsigned long long) commit_lsn_long & 0xFFFFFFFF));
			api->span_add_attribute_string(&replica_span, "pg.commit_lsn", lsn_str);

			/* Emit directly — no stack push; recovery has no active span stack. */
			api->span_emit(&replica_span);
			return;
		}

		default:
			return;				/* unknown probe — ignore */
	}

	/* ---- START path ---- */
	if (is_start)
	{
		OtelRootContextSnapshot rc;

		/*
		 * Only build SDT spans when a propagated/sampled root trace
		 * context is present.  The producer keeps a single trace_id
		 * across the active stack "by construction": on every push it
		 * fills span->trace_id from the root context (otel_ctx) and does
		 * NOT copy it from the parent stack entry (entries store only
		 * span_id + trace_flags, see otel_producer.c).  So nesting is only
		 * coherent when a root context is set.  Without one, the existing
		 * statement-span path (otel_trace.c) synthesizes its own per-span
		 * trace and never nests; a bridge span pushed in that state would
		 * either be an orphan root or, worse, inherit a zeroed trace_id.
		 *
		 * Requiring rc.is_set keeps every SDT span on the SAME trace as
		 * the statement span and the propagated parent.  Consequence:
		 * otel.trace_all_queries without a propagated context yields only
		 * the statement span, not the SDT subtree --- to see the SDT tree,
		 * propagate a context (SET otel.traceparent, the 'M' protocol
		 * header, or sqlcommenter).
		 */
		api->get_root_context_snapshot(&rc);
		if (!rc.is_set)
			return;

		if (sdt_top >= SDT_POOL_SIZE)
			return;				/* pool full; drop this probe */

		s = &sdt_pool[sdt_top];

		/*
		 * Ensure the scope handle is registered.  otel_pg_ensure() above
		 * may have populated otel_pg_tracer for us; lazily set sdt_scope
		 * from it on first use.
		 */
		if (sdt_scope == NULL && otel_pg_tracer != NULL)
			sdt_scope = otel_pg_tracer;

		/*
		 * span_init zeroes the struct, generates a fresh span_id, and sets
		 * start_time.  span_link_to_active_and_push then fills trace_id,
		 * parent_span_id and trace_flags from the top-of-stack span (or the
		 * root context when the stack is empty).
		 */
		api->span_init(s, sdt_scope, span_name, OTEL_SPAN_KIND_INTERNAL);

		/*
		 * Add useful attributes for query-level probes.  nargs and arg
		 * layout are probe-specific; guard carefully.  The query string is
		 * a borrowed pointer that outlives the span (valid through the
		 * matching DONE in the same processing phase).
		 */
		if ((PgSdtProbeId) id == PG_SDT_QUERY_START ||
			(PgSdtProbeId) id == PG_SDT_QUERY_PARSE_START ||
			(PgSdtProbeId) id == PG_SDT_QUERY_REWRITE_START)
		{
			/* First arg is the query string ('s') for these probes. */
			if (nargs >= 1 && args[0].tag == 's' && args[0].v.s != NULL)
				api->span_add_attribute_string(s, "db.statement", args[0].v.s);
		}

		if ((PgSdtProbeId) id == PG_SDT_SYNCREP_WAIT_START)
		{
			/* First arg is the commit LSN ('i') for this probe. */
			if (nargs >= 1 && args[0].tag == 'i')
			{
				/*
				 * Format into this slot's persistent scratch (NOT a stack
				 * local): span_add_attribute_string stores the pointer, and the
				 * span is not emitted until the matching DONE after this frame
				 * returns.
				 */
				char	   *lsn_str = sdt_attr_scratch[sdt_top][0];
				long		lsn_long = (long) args[0].v.i;

				snprintf(lsn_str, SDT_ATTR_BUFLEN, "%lX/%08lX",
						 (unsigned long) ((unsigned long long) lsn_long >> 32),
						 (unsigned long) ((unsigned long long) lsn_long & 0xFFFFFFFF));
				api->span_add_attribute_string(s, "pg.commit_lsn", lsn_str);
			}
		}

		if ((PgSdtProbeId) id == PG_SDT_LOCK_WAIT_START)
		{
			/*
			 * Six int64 args: locktag field1..4, locktag_type, lock mode.
			 * Decode the type/mode to human-readable names and emit the
			 * type-specific lock target.  Integer values are formatted into
			 * this slot's persistent scratch buffers (NOT stack locals):
			 * span_add_attribute_string stores the pointer, and the span is
			 * not emitted until the matching DONE after this frame returns.
			 */
			if (nargs >= 6 &&
				args[0].tag == 'i' && args[1].tag == 'i' &&
				args[2].tag == 'i' && args[3].tag == 'i' &&
				args[4].tag == 'i' && args[5].tag == 'i')
			{
				char	  (*buf)[SDT_ATTR_BUFLEN] = sdt_attr_scratch[sdt_top];
				int			nb = 0;		/* next free scratch buffer */
				int64		field1 = args[0].v.i;
				int64		field2 = args[1].v.i;
				int64		field3 = args[2].v.i;
				int64		field4 = args[3].v.i;
				int			locktag_type = (int) args[4].v.i;
				int			mode = (int) args[5].v.i;
				const char *tname = sdt_locktag_type_name(locktag_type);

				/* pg.lock.type */
				if (tname != NULL)
					api->span_add_attribute_string(s, "pg.lock.type", tname);
				else if (nb < SDT_ATTR_BUFS)
				{
					snprintf(buf[nb], SDT_ATTR_BUFLEN, INT64_FORMAT,
							 (int64) locktag_type);
					api->span_add_attribute_string(s, "pg.lock.type", buf[nb]);
					nb++;
				}

				/* pg.lock.mode */
				if (mode >= 1 && mode < (int) lengthof(sdt_lockmode_names))
					api->span_add_attribute_string(s, "pg.lock.mode",
												   sdt_lockmode_names[mode]);
				else if (nb < SDT_ATTR_BUFS)
				{
					snprintf(buf[nb], SDT_ATTR_BUFLEN, INT64_FORMAT, (int64) mode);
					api->span_add_attribute_string(s, "pg.lock.mode", buf[nb]);
					nb++;
				}

				/* Type-specific targets. */
				switch (locktag_type)
				{
					case LOCKTAG_RELATION:
					case LOCKTAG_RELATION_EXTEND:
					case LOCKTAG_PAGE:
					case LOCKTAG_TUPLE:
						if (nb < SDT_ATTR_BUFS)
						{
							snprintf(buf[nb], SDT_ATTR_BUFLEN, INT64_FORMAT, field1);
							api->span_add_attribute_string(s, "pg.lock.dboid", buf[nb]);
							nb++;
						}
						if (nb < SDT_ATTR_BUFS)
						{
							snprintf(buf[nb], SDT_ATTR_BUFLEN, INT64_FORMAT, field2);
							api->span_add_attribute_string(s, "pg.lock.relid", buf[nb]);
							nb++;
						}
						if (locktag_type == LOCKTAG_PAGE ||
							locktag_type == LOCKTAG_TUPLE)
						{
							if (nb < SDT_ATTR_BUFS)
							{
								snprintf(buf[nb], SDT_ATTR_BUFLEN, INT64_FORMAT, field3);
								api->span_add_attribute_string(s, "pg.lock.block", buf[nb]);
								nb++;
							}
						}
						if (locktag_type == LOCKTAG_TUPLE)
						{
							if (nb < SDT_ATTR_BUFS)
							{
								snprintf(buf[nb], SDT_ATTR_BUFLEN, INT64_FORMAT, field4);
								api->span_add_attribute_string(s, "pg.lock.offset", buf[nb]);
								nb++;
							}
						}
						break;

					case LOCKTAG_TRANSACTION:
						if (nb < SDT_ATTR_BUFS)
						{
							snprintf(buf[nb], SDT_ATTR_BUFLEN, INT64_FORMAT, field1);
							api->span_add_attribute_string(s, "pg.lock.xid", buf[nb]);
							nb++;
						}
						break;

					case LOCKTAG_VIRTUALTRANSACTION:
						if (nb < SDT_ATTR_BUFS)
						{
							snprintf(buf[nb], SDT_ATTR_BUFLEN, INT64_FORMAT, field1);
							api->span_add_attribute_string(s, "pg.lock.procno", buf[nb]);
							nb++;
						}
						if (nb < SDT_ATTR_BUFS)
						{
							snprintf(buf[nb], SDT_ATTR_BUFLEN, INT64_FORMAT, field2);
							api->span_add_attribute_string(s, "pg.lock.localxid", buf[nb]);
							nb++;
						}
						break;

					default:
						/* Generic: emit field1..4 as-is. */
						if (nb < SDT_ATTR_BUFS)
						{
							snprintf(buf[nb], SDT_ATTR_BUFLEN, INT64_FORMAT, field1);
							api->span_add_attribute_string(s, "pg.lock.field1", buf[nb]);
							nb++;
						}
						if (nb < SDT_ATTR_BUFS)
						{
							snprintf(buf[nb], SDT_ATTR_BUFLEN, INT64_FORMAT, field2);
							api->span_add_attribute_string(s, "pg.lock.field2", buf[nb]);
							nb++;
						}
						if (nb < SDT_ATTR_BUFS)
						{
							snprintf(buf[nb], SDT_ATTR_BUFLEN, INT64_FORMAT, field3);
							api->span_add_attribute_string(s, "pg.lock.field3", buf[nb]);
							nb++;
						}
						if (nb < SDT_ATTR_BUFS)
						{
							snprintf(buf[nb], SDT_ATTR_BUFLEN, INT64_FORMAT, field4);
							api->span_add_attribute_string(s, "pg.lock.field4", buf[nb]);
							nb++;
						}
						break;
				}
			}
		}

		/*
		 * OTEL_UNWIND_DROP: slab storage is permanent; the unwind
		 * callback will silently remove the stack entry without
		 * dereferencing the span pointer.  sdt_top is reset separately
		 * in the xact callback on ABORT.
		 */
		s->unwind_policy = OTEL_UNWIND_DROP;

		api->span_link_to_active_and_push(s);
		sdt_top++;

		/*
		 * Associate the per-statement query trace with the
		 * transaction-lifetime span (a separate trace).  pg.query is the
		 * root of the per-statement SDT subtree, so we add bidirectional
		 * span links: the query span links to the transaction, and the
		 * transaction span accumulates a link to each query that ran in
		 * it (emitted at commit/abort).  s->trace_id / s->span_id are now
		 * populated by the push above.
		 */
		if ((PgSdtProbeId) id == PG_SDT_QUERY_START && txn_active)
		{
			otel_span_add_link(s, txn_span.trace_id, txn_span.span_id,
							   txn_span.trace_flags);
			otel_span_add_link(&txn_span, s->trace_id, s->span_id,
							   s->trace_flags);
		}

		return;
	}

	/* ---- DONE path ---- */
	if (sdt_top <= 0)
		return;					/* no matching START on our stack */

	s = &sdt_pool[--sdt_top];
	s->end_time = GetCurrentTimestamp();
	s->status = OTEL_STATUS_UNSET;

	/*
	 * span_emit locates our span by span_id and pops it.  SDT start/done
	 * pairs are LIFO in the common case so our span is the top; for the
	 * few that are not strictly nested (interleaved sorts, or a utility
	 * statement such as CREATE TABLE AS that runs an executor underneath)
	 * the producer unwinds the entries above ours, each honouring its own
	 * unwind_policy.  All bridge spans are OTEL_UNWIND_DROP, so an
	 * out-of-order emit only drops sibling bridge spans (plus a benign
	 * WARNING) and never disturbs the lower OTEL_UNWIND_ERROR statement
	 * span.  The trace stays coherent.
	 */
	api->span_emit(s);
}


/* -----------------------------------------------------------------------
 * otel_sdt_xact_cb
 *
 * Transaction event callback.  On abort, reset sdt_top so our pool
 * index stays in sync with the producer's active stack (which the
 * MemoryContext callbacks have already drained via OTEL_UNWIND_DROP).
 * On commit we do nothing --- the TRANSACTION_COMMIT probe fires
 * before the xact callback, so the pg.txn span has already been
 * emitted by otel_sdt_hook.
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
			 * level, parallel-worker failures).
			 */
			sdt_top = 0;
			break;

		default:
			break;
	}
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
