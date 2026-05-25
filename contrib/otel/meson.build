# Copyright (c) 2022-2026, PostgreSQL Global Development Group

otel_sources = files(
  'otel.c',
  'otel_api.c',
  'otel_log.c',
  'otel_trace.c',
)

if host_system == 'windows'
  otel_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'otel',
    '--FILEDESC', 'otel - OpenTelemetry trace-context support',])
endif

otel = shared_module('otel',
  otel_sources,
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
