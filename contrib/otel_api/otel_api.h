/*-------------------------------------------------------------------------
 *
 * otel_api.h
 *	  The otel_api root table and how other extensions find it.
 *
 * otel_api publishes one root table, OtelApi, through a rendezvous
 * variable.  The root table points to one table per audience:
 *
 *	 producer	extensions that create spans (otel_producer.h)
 *	 exporter	extensions that receive finished spans (otel_exporter.h)
 *	 internal	otel_postgres_tracing: root context, sqlcommenter,
 *				parallel-worker handoff, counters (otel_internal_api.h)
 *
 * Each table has its own version and struct_size, so each audience's
 * table can grow without affecting the others.
 *
 * Versioning, for every table:
 *	 - MAJOR (high 16 bits of version) must match exactly.  It changes on
 *	   any incompatible layout or semantic change.
 *	 - struct_size must be >= the consumer's sizeof.  Fields are only ever
 *	   appended within a MAJOR, so a consumer can use a newer producer's
 *	   table, and a smaller struct_size means the provider is older than
 *	   the consumer's headers.
 *	 - MINOR is informational.
 *
 * The API is pre-1.0: any change may bump MAJOR, and every consumer must
 * be rebuilt against the installed headers.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * otel_api/otel_api.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef OTEL_API_H
#define OTEL_API_H

#include "fmgr.h"
#include "otel_types.h"

#define OTEL_API_MAJOR_SHIFT		16
#define OTEL_API_MINOR_MASK			0xFFFFu
#define OTEL_MAKE_VERSION(maj, min)	(((uint32) (maj) << OTEL_API_MAJOR_SHIFT) | \
									 (uint16) (min))
#define OTEL_API_MAJOR(v)			((v) >> OTEL_API_MAJOR_SHIFT)
#define OTEL_API_MINOR(v)			((v) & OTEL_API_MINOR_MASK)

#define OTEL_ROOT_API_MAJOR			3
#define OTEL_ROOT_API_MINOR			0
#define OTEL_ROOT_API_VERSION		OTEL_MAKE_VERSION(OTEL_ROOT_API_MAJOR, \
												  OTEL_ROOT_API_MINOR)

/*
 * Rendezvous variable holding a const OtelApi *.  The name changed at
 * MAJOR 3; consumers built against MAJOR 2 look up the old name, find
 * nothing, and run as if otel_api were not loaded.
 */
#define OTEL_API_RENDEZVOUS_NAME	"OtelApi.v3"

/*
 * The per-audience tables are defined in their own headers and reached
 * through their accessors (otel_producer_api() etc.), which check each
 * table's version and size.  They are untyped here: a typed pointer to a
 * struct that is incomplete at this point makes bindgen generate an
 * opaque type for it, even when the full definition is also visible.
 */
typedef struct OtelApi
{
	uint32		version;		/* OTEL_ROOT_API_VERSION */
	uint32		struct_size;	/* sizeof(OtelApi) */
	const void *producer;		/* const OtelProducerApi * */
	const void *exporter;		/* const OtelExporterApi * */
	const void *internal;		/* const OtelInternalApi * */
} OtelApi;

/*
 * Sentinel cached when the provider is absent or incompatible.  Never
 * dereferenced.
 */
#define OTEL_API_MISSING	((const void *) (uintptr_t) -1)

/*
 * Check a table's version and size.  Warns once per call site's cache
 * when a table is present but incompatible; an absent provider is
 * silent.
 */
static inline bool
otel_api_table_ok(const char *what, uint32 version, uint32 struct_size,
				  uint32 want_major, uint32 want_minor, size_t want_size)
{
	if (OTEL_API_MAJOR(version) == want_major && struct_size >= want_size)
		return true;
	ereport(WARNING,
			errmsg("otel_api %s table is incompatible with this module", what),
			errdetail("Loaded otel_api provides version %u.%u (struct_size %u); "
					  "this module was built against %u.%u (struct_size %zu).",
					  OTEL_API_MAJOR(version), OTEL_API_MINOR(version),
					  struct_size, want_major, want_minor, want_size));
	return false;
}

/*
 * Return the root table, or NULL if otel_api is not loaded or is
 * incompatible.  The result is cached in a static in each translation
 * unit, so after the first call this costs one load and one compare.
 *
 * Valid once every shared_preload_libraries _PG_init has run, i.e. on
 * any code path that runs in a backend.  Don't call it from _PG_init:
 * if otel_api loads later, the "absent" result stays cached.  Exporters
 * register from _PG_init with otel_exporter_register_when_ready().
 */
static inline const OtelApi *
otel_api_get(void)
{
	static const void *cache = NULL;

	if (likely(cache != NULL))
		return cache == OTEL_API_MISSING ? NULL : (const OtelApi *) cache;

	{
		void	  **slot = find_rendezvous_variable(OTEL_API_RENDEZVOUS_NAME);
		const OtelApi *api = (const OtelApi *) *slot;

		if (api == NULL ||
			!otel_api_table_ok("root", api->version, api->struct_size,
							   OTEL_ROOT_API_MAJOR, OTEL_ROOT_API_MINOR,
							   sizeof(OtelApi)))
		{
			cache = OTEL_API_MISSING;
			return NULL;
		}
		cache = api;
		return api;
	}
}

#endif							/* OTEL_API_H */
