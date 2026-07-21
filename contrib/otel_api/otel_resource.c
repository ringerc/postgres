/*-------------------------------------------------------------------------
 *
 * otel_resource.c
 *	  OTel Resource attributes and InstrumentationScope registry for
 *	  the postmaster process.
 *
 * The OTel data model identifies a metric stream / span batch by
 * (Resource, InstrumentationScope, name, attributes).  Resource
 * describes the *process* that emitted the telemetry --- canonical
 * keys: service.name, service.instance.id, host.name.  A single
 * Resource therefore applies to every signal (traces, metrics) a
 * given postmaster emits.
 *
 * InstrumentationScope identifies the *library* within that process
 * that created the signal --- e.g. "contrib/otel_postgres_tracing"
 * vs an out-of-tree extension that uses the producer API to emit
 * its own spans.  Producers obtain a scope handle once at _PG_init
 * via otel_tracer_register() and cache it module-statically.
 *
 * This module populates a process-local OtelResourceAttribute array
 * once at _PG_init and exposes it to exporters via
 * OtelTracingApi.get_resource_attributes().  Pointers in the array
 * remain valid for the lifetime of the backend, so exporters may
 * cache them.  Operators override the defaults via the
 * otel_api.service_name and otel_api.service_instance_id GUCs (which match
 * OTel's environment-variable conventions OTEL_SERVICE_NAME etc.).
 *
 * Resource attributes are populated following the OTel SDK spec
 * (https://opentelemetry.io/docs/specs/otel/resource/sdk/):
 *	 - service.name: otel_api.service_name GUC → OTEL_SERVICE_NAME env →
 *	   service.name key in OTEL_RESOURCE_ATTRIBUTES → "postgres".
 *	 - service.instance.id: otel_api.service_instance_id GUC →
 *	   OTEL_SERVICE_INSTANCE_ID env → service.instance.id in
 *	   OTEL_RESOURCE_ATTRIBUTES → pg_control system identifier.
 *	 - host.name: gethostname(3); falls back to "localhost" on failure.
 *	 - Any additional k=v pairs from OTEL_RESOURCE_ATTRIBUTES are pushed
 *	   as extra attributes (values are URL-percent-decoded).
 *
 * Exporters that want richer Resource (e.g. host.arch, os.type) merge
 * their own attributes on top of what this module provides.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/otel/otel_resource.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <unistd.h>

#include "access/xlog.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#include "otel.h"
#include "otel_internal.h"

/* GUC backing storage. */
char	   *otel_service_name_guc = NULL;
char	   *otel_service_instance_id_guc = NULL;

/*
 * Resource attribute storage.  Populated at _PG_init under
 * TopMemoryContext so the strings outlive every per-statement context.
 * Sized generously so that extensions can add attributes via
 * otel_resource_add() after _PG_init completes.
 */
#define OTEL_RESOURCE_ATTR_CAPACITY		16
static OtelResourceAttribute otel_resource_attrs[OTEL_RESOURCE_ATTR_CAPACITY];
static int	otel_resource_n_attrs = 0;

static void
push_resource_attr(const char *key, const char *value)
{
	Assert(otel_resource_n_attrs < OTEL_RESOURCE_ATTR_CAPACITY);
	otel_resource_attrs[otel_resource_n_attrs].key = key;
	otel_resource_attrs[otel_resource_n_attrs].value = value;
	otel_resource_n_attrs++;
}

/*
 * otel_resource_add --- add or replace a resource attribute.
 *
 * Safe to call after _PG_init completes, before the first span is emitted
 * in a backend.  Strings are pstrdup'd into TopMemoryContext so they
 * remain valid for the backend's lifetime.  If `key` is already present,
 * the value is updated in place (last-write-wins); otherwise a new entry
 * is appended.
 *
 * Called from extension _PG_init callbacks or deferred initialization
 * hooks (e.g. ExecutorStart, ProcessUtility) to publish per-process
 * identity after catalog data becomes accessible.
 */
void
otel_resource_add(const char *key, const char *value)
{
	MemoryContext oldcxt;
	int			i;

	if (key == NULL || key[0] == '\0')
		return;

	/* Search for an existing entry with the same key. */
	for (i = 0; i < otel_resource_n_attrs; i++)
	{
		if (otel_resource_attrs[i].key != NULL &&
			strcmp(otel_resource_attrs[i].key, key) == 0)
		{
			/* Update value in place. */
			oldcxt = MemoryContextSwitchTo(TopMemoryContext);
			otel_resource_attrs[i].value = pstrdup(value ? value : "");
			MemoryContextSwitchTo(oldcxt);
			return;
		}
	}

	/* Append a new entry. */
	if (otel_resource_n_attrs >= OTEL_RESOURCE_ATTR_CAPACITY)
	{
		ereport(WARNING,
				(errmsg("otel_resource_add: resource attribute capacity (%d) exceeded; ignoring key \"%s\"",
						OTEL_RESOURCE_ATTR_CAPACITY, key)));
		return;
	}

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	otel_resource_attrs[otel_resource_n_attrs].key = pstrdup(key);
	otel_resource_attrs[otel_resource_n_attrs].value = pstrdup(value ? value : "");
	otel_resource_n_attrs++;
	MemoryContextSwitchTo(oldcxt);
}

/* -----------------------------------------------------------------------
 * OTEL_RESOURCE_ATTRIBUTES parsing helpers
 * ----------------------------------------------------------------------- */

/* Maximum k=v pairs we parse from OTEL_RESOURCE_ATTRIBUTES. */
#define OTEL_RA_ENV_MAX_PAIRS 32

typedef struct
{
	char	   *key;
	char	   *val;
	bool		consumed;		/* true once used by a precedence chain */
} OtelRaPair;

/* URL-percent-decode s in-place (e.g. %20 → space). */
static void
pct_decode_inplace(char *s)
{
	char	   *r = s,
			   *w = s;

	while (*r)
	{
		if (r[0] == '%' &&
			isxdigit((unsigned char) r[1]) &&
			isxdigit((unsigned char) r[2]))
		{
			int			hi = isdigit((unsigned char) r[1])
			? r[1] - '0'
			: tolower((unsigned char) r[1]) - 'a' + 10;
			int			lo = isdigit((unsigned char) r[2])
			? r[2] - '0'
			: tolower((unsigned char) r[2]) - 'a' + 10;

			*w++ = (char) ((hi << 4) | lo);
			r += 3;
		}
		else
			*w++ = *r++;
	}
	*w = '\0';
}

/* Trim leading/trailing ASCII whitespace in-place; return new start. */
static char *
trim_ws(char *s)
{
	char	   *end;

	while (*s && isspace((unsigned char) *s))
		s++;
	end = s + strlen(s);
	while (end > s && isspace((unsigned char) end[-1]))
		end--;
	*end = '\0';
	return s;
}

/*
 * Parse OTEL_RESOURCE_ATTRIBUTES ("k=v,k=v,...") into pairs[].
 * Values are URL-percent-decoded and keys/values are whitespace-trimmed.
 * Strings in pairs[] are pstrdup'd in the current memory context.
 * Returns count of pairs parsed; malformed entries are silently skipped.
 */
static int
parse_otel_resource_attributes(const char *raw, OtelRaPair *pairs,
								int max_pairs)
{
	char	   *buf,
			   *p;
	int			n = 0;

	if (raw == NULL || raw[0] == '\0')
		return 0;

	buf = pstrdup(raw);
	p = buf;

	while (*p && n < max_pairs)
	{
		char	   *seg_end = strchr(p, ',');
		char	   *next_p;
		char	   *eq;
		char	   *key,
				   *val;

		if (seg_end)
		{
			*seg_end = '\0';
			next_p = seg_end + 1;
		}
		else
			next_p = p + strlen(p); /* last segment; computed before modification */

		eq = strchr(p, '=');
		if (eq == NULL)
		{
			ereport(DEBUG1,
					(errmsg("otel_resource_init: skipping malformed OTEL_RESOURCE_ATTRIBUTES entry: \"%s\"",
							p)));
			p = next_p;
			continue;
		}

		*eq = '\0';
		key = trim_ws(p);
		val = trim_ws(eq + 1);
		pct_decode_inplace(val);

		if (key[0] != '\0')
		{
			pairs[n].key = pstrdup(key);
			pairs[n].val = pstrdup(val);
			pairs[n].consumed = false;
			n++;
		}

		p = next_p;
	}

	pfree(buf);
	return n;
}

/*
 * Look up key in pairs[] and mark it consumed.  Returns the value, or
 * NULL if not found / empty.
 */
static const char *
consume_ra_pair(OtelRaPair *pairs, int n, const char *key)
{
	int			i;

	for (i = 0; i < n; i++)
	{
		if (!pairs[i].consumed &&
			strcmp(pairs[i].key, key) == 0 &&
			pairs[i].val[0] != '\0')
		{
			pairs[i].consumed = true;
			return pairs[i].val;
		}
	}
	return NULL;
}

/*
 * Populate the Resource attribute array.  Called once from _PG_init
 * after the GUCs have been defined.  Strings are pstrdup'd into
 * TopMemoryContext --- they remain valid for the backend's lifetime,
 * and consumers may cache the pointers.
 *
 * Precedence for service.name and service.instance.id follows the OTel
 * SDK Resource spec:
 *   https://opentelemetry.io/docs/specs/otel/resource/sdk/
 */
void
otel_resource_init(void)
{
	MemoryContext oldcxt;
	const char *service_name;
	const char *instance_id;
	char	   *instance_id_buf = NULL;
	char		hostname[256];
	OtelRaPair	ra_pairs[OTEL_RA_ENV_MAX_PAIRS];
	int			ra_n;
	int			i;

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	/* Parse OTEL_RESOURCE_ATTRIBUTES once; used for all precedence chains. */
	ra_n = parse_otel_resource_attributes(getenv("OTEL_RESOURCE_ATTRIBUTES"),
										  ra_pairs, OTEL_RA_ENV_MAX_PAIRS);

	/*
	 * service.name: GUC → OTEL_SERVICE_NAME → OTEL_RESOURCE_ATTRIBUTES →
	 * "postgres"
	 */
	service_name = NULL;
	if (otel_service_name_guc && otel_service_name_guc[0])
		service_name = otel_service_name_guc;
	if (service_name == NULL)
	{
		const char *e = getenv("OTEL_SERVICE_NAME");

		if (e && e[0])
			service_name = e;
	}
	if (service_name == NULL)
		service_name = consume_ra_pair(ra_pairs, ra_n, "service.name");
	push_resource_attr("service.name",
					   pstrdup(service_name ? service_name : "postgres"));

	/*
	 * service.instance.id: GUC → OTEL_SERVICE_INSTANCE_ID →
	 * OTEL_RESOURCE_ATTRIBUTES → pg_control system identifier.
	 * GetSystemIdentifier() reads from pg_control, loaded by the postmaster
	 * before shared_preload_libraries init.
	 */
	instance_id = NULL;
	if (otel_service_instance_id_guc && otel_service_instance_id_guc[0])
		instance_id = otel_service_instance_id_guc;
	if (instance_id == NULL)
	{
		const char *e = getenv("OTEL_SERVICE_INSTANCE_ID");

		if (e && e[0])
			instance_id = e;
	}
	if (instance_id == NULL)
		instance_id = consume_ra_pair(ra_pairs, ra_n, "service.instance.id");
	if (instance_id == NULL)
	{
		instance_id_buf = palloc(32);
		snprintf(instance_id_buf, 32, UINT64_FORMAT, GetSystemIdentifier());
		instance_id = instance_id_buf;
	}
	push_resource_attr("service.instance.id", pstrdup(instance_id));
	if (instance_id_buf)
		pfree(instance_id_buf);

	/* host.name: gethostname(3); fall back to "localhost" on error. */
	if (gethostname(hostname, sizeof(hostname)) != 0)
		strcpy(hostname, "localhost");
	else
		hostname[sizeof(hostname) - 1] = '\0';
	push_resource_attr("host.name", pstrdup(hostname));

	/*
	 * Push remaining non-consumed OTEL_RESOURCE_ATTRIBUTES pairs as
	 * additional resource attributes.  GUC-derived values already pushed
	 * above take precedence (we skip keys already present).
	 */
	for (i = 0; i < ra_n; i++)
	{
		int			j;
		bool		already_present = false;

		if (ra_pairs[i].consumed || ra_pairs[i].key[0] == '\0')
			continue;

		for (j = 0; j < otel_resource_n_attrs; j++)
		{
			if (strcmp(otel_resource_attrs[j].key, ra_pairs[i].key) == 0)
			{
				already_present = true;
				break;
			}
		}
		if (already_present)
			continue;

		if (otel_resource_n_attrs >= OTEL_RESOURCE_ATTR_CAPACITY)
		{
			ereport(DEBUG1,
					(errmsg("otel_resource_init: OTEL_RESOURCE_ATTRIBUTES entry \"%s\" exceeds capacity; ignoring",
							ra_pairs[i].key)));
			continue;
		}

		push_resource_attr(ra_pairs[i].key, ra_pairs[i].val);
	}

	MemoryContextSwitchTo(oldcxt);
}

/*
 * Return a pointer to the Resource attribute array and write the
 * count into *n_out.  Pointers in the returned array are valid for
 * the lifetime of the backend.
 */
const OtelResourceAttribute *
otel_resource_attrs_get(int *n_out)
{
	if (n_out)
		*n_out = otel_resource_n_attrs;
	return otel_resource_attrs;
}


/*
 * tracer_register --- construct an OtelInstrumentationScope handle.
 *
 * Called once per producer extension from _PG_init.  Strings are
 * pstrdup'd into TopMemoryContext so the handle outlives any
 * per-statement context; producers cache the returned pointer
 * module-statically and pass it to every otel_span_init() call.
 *
 * version and schema_url may be NULL.  No dedup: each call allocates
 * a fresh handle.  Two producers that happen to declare identical
 * triples get separate handles, but exporters compare scopes by
 * content (string equality on name/version/schema_url), so the
 * observable behaviour is the same.
 */
OtelInstrumentationScope *
otel_tracer_register(const char *name,
					 const char *version,
					 const char *schema_url)
{
	MemoryContext oldcxt;
	OtelInstrumentationScope *scope;

	if (name == NULL || name[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("otel_tracer_register: name must be non-empty")));

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	scope = palloc(sizeof(*scope));
	scope->name = pstrdup(name);
	scope->version = version ? pstrdup(version) : NULL;
	scope->schema_url = schema_url ? pstrdup(schema_url) : NULL;
	MemoryContextSwitchTo(oldcxt);

	return scope;
}
