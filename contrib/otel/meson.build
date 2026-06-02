# Copyright (c) 2022-2026, PostgreSQL Global Development Group

otel_sources = files(
  'otel.c',
  'otel_api.c',
  'otel_parallel.c',
  'otel_producer.c',
  'otel_resource.c',
)

if host_system == 'windows'
  otel_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'otel',
    '--FILEDESC', 'otel - OpenTelemetry trace-context support',])
endif

# Optional dependencies on core-postgres features.  contrib/otel
# builds against an unpatched server by detecting these at compile
# time; missing features fall back to alternative paths.
# Matching Makefile probes live in contrib/otel/Makefile.
otel_include_args = [
  '-I' + meson.project_source_root() / 'src/include',
  '-I' + meson.project_build_root() / 'src/include',
]
otel_cppflags = []
if cc.has_header('libpq/protocol_headers.h', args: otel_include_args)
  otel_cppflags += '-DOTEL_HAVE_PROTOCOL_HEADERS'
endif
# Detect errtrace() with a compile probe (has_header_symbol can't
# include postgres.h's prerequisites cleanly).
if cc.compiles('''
    #include "postgres.h"
    #include "utils/elog.h"
    int main(void) { return (int)(uintptr_t)&errtrace; }
    ''', args: otel_include_args, name: 'errtrace declared in utils/elog.h')
  otel_cppflags += '-DOTEL_HAVE_ERRTRACE'
endif

otel = shared_module('otel',
  otel_sources,
  c_args: otel_cppflags,
  kwargs: contrib_mod_args,
)
contrib_targets += otel

install_data(
  'otel.control',
  'otel--1.0.sql',
  kwargs: contrib_data_args,
)

install_headers(
  'otel.h',
  'otel_api.h',
  install_dir: dir_include_extension / 'otel',
)

tests += {
  'name': 'otel',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_otel.pl',
    ],
  },
}
