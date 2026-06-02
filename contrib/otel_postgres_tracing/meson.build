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

# See contrib/otel/meson.build for the OTEL_HAVE_* feature gates.
# otel_postgres_tracing only depends on OTEL_HAVE_ERRTRACE.
otel_pgt_include_args = [
  '-I' + meson.project_source_root() / 'src/include',
  '-I' + meson.project_build_root() / 'src/include',
]
otel_pgt_cppflags = []
if cc.compiles('''
    #include "postgres.h"
    #include "utils/elog.h"
    int main(void) { return (int)(uintptr_t)&errtrace; }
    ''', args: otel_pgt_include_args, name: 'errtrace declared in utils/elog.h')
  otel_pgt_cppflags += '-DOTEL_HAVE_ERRTRACE'
endif

otel_postgres_tracing = shared_module('otel_postgres_tracing',
  otel_postgres_tracing_sources,
  c_args: ['-I' + meson.project_source_root() / 'contrib' / 'otel'] + otel_pgt_cppflags,
  kwargs: contrib_mod_args,
)
contrib_targets += otel_postgres_tracing

install_data(
  'otel_postgres_tracing.control',
  'otel_postgres_tracing--1.0.sql',
  kwargs: contrib_data_args,
)
