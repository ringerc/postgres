# Copyright (c) 2026, PostgreSQL Global Development Group

otel_demo_exporter_sources = files(
  'otel_demo_exporter.c',
)

if host_system == 'windows'
  otel_demo_exporter_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'otel_demo_exporter',
    '--FILEDESC', 'otel_demo_exporter - bare-minimum file exporter for contrib/otel spans',])
endif

# Pick up <otel_api/otel.h> from contrib/otel_api for in-tree builds.
# An out-of-tree exporter using PGXS gets the same path via the
# installed server include dir (HEADERS variable in otel_api/Makefile
# + the install_headers call in otel_api/meson.build).
otel_demo_exporter_inc = include_directories('..')

otel_demo_exporter = shared_module('otel_demo_exporter',
  otel_demo_exporter_sources,
  include_directories: otel_demo_exporter_inc,
  kwargs: contrib_mod_args,
)
contrib_targets += otel_demo_exporter

install_data(
  'otel_demo_exporter.control',
  'otel_demo_exporter--0.1.1.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'otel_demo_exporter',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_file_exporter.pl',
    ],
  },
}
