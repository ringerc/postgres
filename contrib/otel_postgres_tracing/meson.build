# Copyright (c) 2026, PostgreSQL Global Development Group

otel_postgres_tracing_sources = files(
  'otel_log.c',
  'otel_postgres_tracing.c',
  'otel_trace.c',
)

if host_system == 'windows'
  otel_postgres_tracing_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'otel_postgres_tracing',
    '--FILEDESC', 'otel_postgres_tracing - OpenTelemetry instrumentation for PostgreSQL query execution',])
endif

otel_postgres_tracing = shared_module('otel_postgres_tracing',
  otel_postgres_tracing_sources,
  c_args: ['-I' + meson.project_source_root() / 'contrib' / 'otel'],
  kwargs: contrib_mod_args,
)
contrib_targets += otel_postgres_tracing

install_data(
  'otel_postgres_tracing.control',
  'otel_postgres_tracing--1.0.sql',
  kwargs: contrib_data_args,
)
