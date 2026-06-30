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
 * Three resource attributes are populated:
 *	 - service.name --- the otel_api.service_name GUC, default "postgres".
 *	 - service.instance.id --- the otel_api.service_instance_id GUC if
 *	   set, else the cluster's pg_control system identifier as a
 *	   decimal string.
 *	 - host.name --- gethostname(3) at _PG_init; falls back to
 *	   "localhost" on failure.
 *
 * Exporters that want richer Resource (e.g. host.arch, os.type,
 * deployment.environment.name) merge their own attributes on top of
 * what this module provides.
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
 * Resource attribute storage.  Populated once at _PG_init under
 * TopMemoryContext so the strings outlive every per-statement context.
 * Sized for the three attributes the module produces today; if the
 * set grows, bump the dimension.
 */
#define OTEL_RESOURCE_ATTR_CAPACITY		3
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
 * Populate the Resource attribute array.  Called once from _PG_init
 * after the GUCs have been defined.  Strings are pstrdup'd into
 * TopMemoryContext --- they remain valid for the backend's lifetime,
 * and consumers may cache the pointers.
 */
void
otel_resource_init(void)
{
	MemoryContext oldcxt;
	const char *service_name;
	const char *instance_id;
	char	   *instance_id_buf = NULL;
	char		hostname[256];

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	/* service.name: GUC, default "postgres". */
	/*
	 * TODO: do not let a blank otel_api.service_name override the standard
	 * OTEL_SERVICE_NAME environment variable. Precedence should be:
	 *   1. otel_api.service_name GUC if explicitly set (non-empty)
	 *   2. else OTEL_SERVICE_NAME env (and service.name in
	 *      OTEL_RESOURCE_ATTRIBUTES) if set
	 *   3. else "postgres"
	 * Today the GUC always wins because its default is the literal
	 * "postgres" (see DefineCustomStringVariable in otel.c), so the env var
	 * is silently ignored even when the GUC was never set by the operator.
	 * Implementing this requires changing the GUC default to NULL/"" so a
	 * blank value is distinguishable from an explicit "postgres", then
	 * reading getenv("OTEL_SERVICE_NAME") here as the middle fallback.
	 * Same reasoning applies to service.instance.id / OTEL_RESOURCE_ATTRIBUTES.
	 */
	service_name = (otel_service_name_guc && otel_service_name_guc[0])
		? otel_service_name_guc : "postgres";
	push_resource_attr("service.name", pstrdup(service_name));

	/* service.instance.id: GUC if set, else the cluster system id.
	 * GetSystemIdentifier() reads from pg_control, loaded by the
	 * postmaster before shared_preload_libraries init. */
	if (otel_service_instance_id_guc && otel_service_instance_id_guc[0])
	{
		instance_id = otel_service_instance_id_guc;
	}
	else
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
