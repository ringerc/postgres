# Copyright (c) 2026, PostgreSQL Global Development Group

test_elog_annotations_sources = files(
  'test_elog_annotations.c',
)

if host_system == 'windows'
  test_elog_annotations_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_elog_annotations',
    '--FILEDESC', 'test_elog_annotations - exercises errannot()/errannotf() plumbing',])
endif

test_elog_annotations = shared_module('test_elog_annotations',
  test_elog_annotations_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_elog_annotations

test_install_data += files(
  'test_elog_annotations.control',
  'test_elog_annotations--1.0.sql',
)

tests += {
  'name': 'test_elog_annotations',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_annotations.pl',
    ],
  },
}
