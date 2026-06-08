# Copyright (c) 2022-2026, PostgreSQL Global Development Group

test_protocol_headers_sources = files(
  'test_protocol_headers.c',
)

if host_system == 'windows'
  test_protocol_headers_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_protocol_headers',
    '--FILEDESC', 'test_protocol_headers - test the per-message protocol headers mechanism',])
endif

test_protocol_headers = shared_module('test_protocol_headers',
  test_protocol_headers_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_protocol_headers

tests += {
  'name': 'test_protocol_headers',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_headers.pl',
    ],
  },
}
