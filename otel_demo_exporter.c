/*-------------------------------------------------------------------------
 *
 * otel_demo_exporter.c
 *	  Bare-minimum file exporter for contrib/otel spans.
 *
 * Demonstrates the rendezvous-variable API exposed by contrib/otel:
 * locates the OtelTracingApi at _PG_init, registers an emit callback,
 * and writes one JSON object per span to a configurable file.
 *
 * Deliberately minimal: only the headline span fields (trace IDs,
 * name, status, timing, db.statement attribute) are written; full
 * attribute/event capture is intentionally left to richer
 * exporters that integrate with a real OpenTelemetry SDK.  The
 * goal here is to prove the API is usable from a separate loadable
 * module with no out-of-tree dependencies.
 *
 * Concurrency: every backend opens the file with O_APPEND on first
 * emit and keeps its own FILE*.  Each fputs is short enough (< 4KB
 * in practice) to be atomic per POSIX PIPE_BUF guarantees for
 * O_APPEND writes, so concurrent backends will not interleave their
 * span lines.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/otel_demo_exporter/otel_demo_exporter.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/json.h"

#include <otel_api/otel.h>

PG_MODULE_MAGIC;

/* GUCs */
static char *otel_demo_exporter_output_file = NULL;	/* path; NULL = disabled */

/* Per-backend output handle.  Opened lazily on first emit; closed
 * via on_proc_exit. */
static FILE *outfp = NULL;
static char *opened_path = NULL;	/* path of the currently-open file */

/* Chain target. */
static otel_span_emit_hook_type prev_emit_hook = NULL;


static void
close_output(int code, Datum arg)
{
	if (outfp != NULL)
	{
		fclose(outfp);
		outfp = NULL;
	}
}

/*
 * Open (or reopen, if the GUC has changed) the output file.  Returns
 * true on success.  All failure modes leave outfp NULL and emit a
 * LOG-level message so a misconfigured path doesn't error the
 * backend's query.
 */
static bool
ensure_output_open(void)
{
	if (outfp != NULL)
	{
		/* Reopen on GUC change. */
		if (opened_path != NULL && otel_demo_exporter_output_file != NULL
			&& strcmp(opened_path, otel_demo_exporter_output_file) == 0)
			return true;

		fclose(outfp);
		outfp = NULL;
		if (opened_path)
		{
			free(opened_path);
			opened_path = NULL;
		}
	}

	if (otel_demo_exporter_output_file == NULL || otel_demo_exporter_output_file[0] == '\0')
		return false;

	outfp = fopen(otel_demo_exporter_output_file, "a");
	if (outfp == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("otel_demo_exporter: could not open \"%s\" for append: %m",
						otel_demo_exporter_output_file)));
		return false;
	}

	opened_path = strdup(otel_demo_exporter_output_file);
	return true;
}

/*
 * Serialize the bare-minimum projection of a span into out.
 *
 * Format is one JSON object, no trailing newline.  Headline fields
 * only; richer exporters that ship real OTLP can use the OtelSpan
 * directly.
 */
static void
format_span_json(const OtelSpan *span, StringInfo out)
{
	int			i;
	const char *db_stmt = NULL;

	/* Pull the db.statement attribute out for convenience; everything
	 * else is captured via the structured fields. */
	for (i = 0; i < span->n_attrs; i++)
	{
		if (span->attrs[i].key && strcmp(span->attrs[i].key, "db.statement") == 0)
		{
			db_stmt = span->attrs[i].value;
			break;
		}
	}

	appendStringInfoChar(out, '{');
	appendStringInfoString(out, "\"trace_id\":");
	escape_json(out, span->trace_id);
	appendStringInfoString(out, ",\"span_id\":");
	escape_json(out, span->span_id);
	appendStringInfoString(out, ",\"parent_span_id\":");
	escape_json(out, span->parent_span_id);
	appendStringInfoString(out, ",\"name\":");
	escape_json(out, span->name ? span->name : "");
	appendStringInfo(out, ",\"status\":%d", (int) span->status);
	appendStringInfo(out, ",\"start_time\":%" PRId64,
					 (int64) span->start_time);
	appendStringInfo(out, ",\"end_time\":%" PRId64,
					 (int64) span->end_time);
	if (db_stmt)
	{
		appendStringInfoString(out, ",\"db_statement\":");
		escape_json(out, db_stmt);
	}
	if (span->scope)
	{
		/* Scope last so simple regex-based field extractors (the TAP
		 * harness uses one) match the top-level "name" before the
		 * nested scope.name. */
		appendStringInfoString(out, ",\"scope\":{");
		appendStringInfoString(out, "\"name\":");
		escape_json(out, span->scope->name ? span->scope->name : "");
		if (span->scope->version)
		{
			appendStringInfoString(out, ",\"version\":");
			escape_json(out, span->scope->version);
		}
		if (span->scope->schema_url)
		{
			appendStringInfoString(out, ",\"schema_url\":");
			escape_json(out, span->scope->schema_url);
		}
		appendStringInfoString(out, "}");
	}
	appendStringInfoChar(out, '}');
}

/*
 * Emit hook.  Best-effort: any allocation / I/O failure drops the
 * span silently.  Chains to the previously-registered hook on the
 * way out so other exporters still see the span.
 */
static void
otel_demo_exporter_emit(const OtelSpan *span)
{
	PG_TRY();
	{
		if (ensure_output_open())
		{
			StringInfoData buf;

			initStringInfo(&buf);
			format_span_json(span, &buf);
			appendStringInfoChar(&buf, '\n');

			if (fwrite(buf.data, 1, buf.len, outfp) != (size_t) buf.len)
				ereport(LOG,
						(errcode_for_file_access(),
						 errmsg("otel_demo_exporter: short write to \"%s\": %m",
								opened_path ? opened_path : "?")));
			fflush(outfp);

			pfree(buf.data);
		}
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();

	if (prev_emit_hook)
		prev_emit_hook(span);
}


void		_PG_init(void);

void
_PG_init(void)
{
	void	  **slot;
	const OtelTracingApi *api;

	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errmsg("otel_demo_exporter must be loaded via shared_preload_libraries")));

	slot = find_rendezvous_variable(OTEL_TRACING_API_RENDEZVOUS_NAME);
	api = (const OtelTracingApi *) *slot;
	if (api == NULL)
		ereport(ERROR,
				(errmsg("otel_demo_exporter requires contrib/otel to be loaded first"),
				 errhint("Add 'otel' before 'otel_demo_exporter' in shared_preload_libraries.")));
	if (OTEL_API_MAJOR(api->version) != OTEL_TRACING_API_MAJOR ||
		api->struct_size < sizeof(*api))
		ereport(ERROR,
				(errmsg("OtelTracingApi compatibility check failed"),
				 errdetail("Loaded otel_api exposes api version %u.%u (struct_size %u); otel_demo_exporter was built against version %u.%u (struct_size %zu).",
						   OTEL_API_MAJOR(api->version),
						   OTEL_API_MINOR(api->version),
						   api->struct_size,
						   OTEL_TRACING_API_MAJOR,
						   OTEL_TRACING_API_MINOR,
						   sizeof(*api))));

	DefineCustomStringVariable("otel_demo_exporter.output_file",
							   "Path of the JSON-lines file to write spans to.",
							   "Empty disables emission.  Relative paths are resolved against the data directory.",
							   &otel_demo_exporter_output_file,
							   "",
							   PGC_SIGHUP,
							   GUC_NOT_IN_SAMPLE,
							   NULL, NULL, NULL);

	MarkGUCPrefixReserved("otel_demo_exporter");

	api->register_emit_hook(otel_demo_exporter_emit, &prev_emit_hook);
	on_proc_exit(close_output, (Datum) 0);
}
