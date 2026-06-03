# contrib/otel_demo_exporter

A bare-minimum span exporter for [`contrib/otel`](../otel/), worked
out as a template and as TAP-test cover for the
`OtelTracingApi` rendezvous-variable interface.  Writes one JSON
object per span to a configurable file.

It is **not** intended for production use.  Treat it as a worked
example: ~200 lines of C showing the smallest possible
exporter that uses contrib/otel's public API end to end.

## Usage

Load both `otel` and `otel_demo_exporter` via
`shared_preload_libraries`, in that order, and point the exporter at
a file:

```ini
# postgresql.conf
shared_preload_libraries = 'otel,otel_demo_exporter'
otel_demo_exporter.output_file = '/var/log/postgresql/otel_spans.jsonl'
```

`CREATE EXTENSION otel_demo_exporter` is optional --- the module is
loaded purely from the preload; the SQL extension exists only so the
module shows up in `pg_extension`.

`otel_demo_exporter.output_file` is `PGC_SIGHUP`: change it and
`SIGHUP` the postmaster.  Each backend reopens the file with
`O_APPEND` on the first emit after the GUC changes.  An empty path
disables emission silently.  Relative paths are resolved against the
data directory.

Spans are written one JSON object per line:

```json
{"trace_id":"...32 hex...","span_id":"...16 hex...","parent_span_id":"...16 hex or empty...","name":"pgsql.execute","status":0,"start_time":NNN,"end_time":NNN,"db_statement":"SELECT 1"}
```

`status` is the W3C / OTel span status (`0`=UNSET, `1`=OK, `2`=ERROR).
`start_time` / `end_time` are postgres `TimestampTz` (microseconds
since 2000-01-01).

## What it deliberately doesn't do

* **No OTLP / gRPC / HTTP export.**  This is a file emitter; there is
  no network stack.  A real exporter is expected to ship spans over
  the wire using an OpenTelemetry SDK linked into its own loadable
  module.
* **No batching, no async dispatch.**  Each span is serialized and
  `fwrite`+`fflush`'d synchronously inside the emit hook.  Adequate
  for tests and low-volume diagnostics; will slow you down under
  load.
* **Bare-minimum span projection.**  Only the headline fields are
  written: trace IDs, name, status, start/end time, and the
  `db.statement` attribute.  `OtelSpan`'s full attribute list,
  events, and sampler decision are visible to the emit hook (see
  `<otel/otel.h>`) but are not serialized here.  A real exporter
  with a real OTel SDK behind it can use everything `OtelSpan` carries.
* **No file rotation.**  Operators are expected to point the file at
  a location managed by an external log shipper / rotator.

## Concurrency

Every backend opens the configured file with `O_APPEND` on its first
emit and keeps its own `FILE *` open for the life of the backend
(closed via `on_proc_exit`).  Per POSIX `PIPE_BUF` guarantees, a
single `write(2)` of less than `PIPE_BUF` bytes (4 KiB on Linux) to
an `O_APPEND` file is atomic; the emitted JSON lines are short enough
in practice that concurrent backends do not interleave their span
records.  Lines longer than `PIPE_BUF` may interleave; the format is
self-delimiting (one JSON object per line) but a corrupted line
would have to be discarded by the consumer.

## As a template for real exporters

The source ([otel_demo_exporter.c](otel_demo_exporter.c)) walks
through every step a production exporter would take:

1. Find `OtelTracingApi` via `find_rendezvous_variable()` (`_PG_init`).
2. Check `api->version == OTEL_TRACING_API_VERSION` and error out on
   mismatch.
3. Register an emit hook via `api->register_emit_hook()`, saving
   the previous hook for chaining.
4. Inside the hook, project `OtelSpan` into whatever wire format the
   exporter uses, then call the saved previous hook so other
   exporters still see the span.

Out-of-tree exporters using PGXS get `<otel/otel.h>` from the
installed server include dir via the `HEADERS` install rule in
`contrib/otel/Makefile` (and the equivalent `install_headers()` call
in `contrib/otel/meson.build`).
