/*-------------------------------------------------------------------------
 *
 * pg_sdt_probe.c
 *	  Definition of the pg_sdt_probe_hook function pointer.
 *
 *	  An out-of-tree extension may set this pointer to intercept the curated
 *	  subset of TRACE_POSTGRESQL_* probes and emit OpenTelemetry spans without
 *	  requiring core to depend on any OTel headers.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/misc/pg_sdt_probe.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/pg_sdt_probe.h"

/* Hook pointer; NULL (disabled) by default. */
void		(*pg_sdt_probe_hook) (int probe_id,
								  const PgSdtArg *args, int nargs) = NULL;

/*
 * Bitmask of enabled PgSdtProbeId values; set by an extension's GUC.
 * Default 0 (all probes off).
 */
uint64		pg_sdt_probe_enabled_mask = 0;
