# otel_api

OpenTelemetry trace-context glue for PostgreSQL.

This module wires PostgreSQL into OpenTelemetry tracing by:

* Accepting a propagated W3C `traceparent` / `tracestate` from the client
  (via query-headers, using GUCs `otel_api.traceparent` /
  `otel_api.tracestate`, or opt-in `sqlcommenter`-style query comments).
* Generating a span per top-level query and per utility statement using
  `ExecutorStart_hook` / `ExecutorEnd_hook` / `ProcessUtility_hook`.
* Capturing `ereport()`s on the active span as OTel span events.
* Exposing the current span id to parallel workers so their work attaches
  to the leader's span.

## What it does NOT do

`contrib/otel` does **not** export OpenTelemetry signals over the wire.
Shipping an OTLP/protobuf/gRPC/libcurl exporter inside the postgres tree
would pull in dependencies that disqualify it as a contrib module.

Instead, it exposes a small exporter API (see [otel.h](otel.h),
`OtelTracingApi`) that an out-of-tree
extension can implement, plugging in a real OpenTelemetry SDK and emitting
the spans contrib/otel produces in whatever wire format that SDK supports. Extensions discover the API via rendezvous variables and register callbacks for trace exporters.

Another extension, [`contrib/otel_demo_exporter`](../otel_demo_exporter/) is included for testing and to demonstrate how to use the `contrib/otel` API.

## Limitations and future work

* **No API for other extensions to produce their own telemetry.**
  `contrib/otel` does not yet provide a C-level or SQL-level API for *other*
  PostgreSQL extensions to emit their own spans, add span
  attributes/events/links, or emit OpenTelemetry metrics or logs. The
  existing `OtelTracingApi` is for *consumers* of `contrib/otel`'s spans
  (exporter SDKs), not for *producers* of additional telemetry. A
  producer-side API can be added separately; it would be a different
  surface from the exporter hook.

* **Instrumentation is limited to high-level query execution.** Spans
  cover statement execution and utility commands; ereports on the active
  span are captured as events. Deeper instrumentation --- lock waits, I/O,
  buffer reads, planner phases, replication, FDW callouts, background
  workers --- is future work, and would use existing or new hooks to
  produce child spans under the query span.
