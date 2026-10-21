# Copyright (c) 2026, PostgreSQL Global Development Group

test_otel_exporter_sources = files(
  'test_otel_exporter.c',
)

if host_system == 'windows'
  test_otel_exporter_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_otel_exporter',
    '--FILEDESC', 'test_otel_exporter - test-only span exporter for contrib/otel',])
endif

test_otel_exporter = shared_module('test_otel_exporter',
  test_otel_exporter_sources,
  # <otel_api/otel.h> resolves from the otel_api SOURCE tree. This file
  # only builds in the core-tree mirror (postgres_otel_api has no
  # top-level meson.build), where otel_api lives at contrib/otel_api -
  # so the include path is project-root-relative, not a sibling-relative
  # path off this file's own (mirror-specific) location.
  c_args: ['-I' + meson.project_source_root() / 'contrib'],
  kwargs: pg_test_mod_args,
)
test_install_libs += test_otel_exporter

test_install_data += files(
  'test_otel_exporter.control',
  'test_otel_exporter--0.1.1.sql',
)

tests += {
  'name': 'otel_test_exporter',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_basic.pl',
      't/002_log_emitter.pl',
      't/003_sampler_policy.pl',
      't/004_sqlcommenter.pl',
      't/005_producer_api.pl',
      't/006_pgbouncer.pl',
      't/007_resource.pl',
      't/008_scope.pl',
      't/009_order_independent.pl',
      't/010_cursor_spans.pl',
    ],
  },
}
