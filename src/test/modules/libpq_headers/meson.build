# Copyright (c) 2022-2026, PostgreSQL Global Development Group

libpq_headers_sources = files(
  'libpq_headers.c',
)

if host_system == 'windows'
  libpq_headers_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'libpq_headers',
    '--FILEDESC', 'libpq_headers - test client for per-message protocol headers',])
endif

libpq_headers = executable('libpq_headers',
  libpq_headers_sources,
  dependencies: [frontend_code, libpq],
  kwargs: default_bin_args + {
    'install': false,
  },
)
testprep_targets += libpq_headers

tests += {
  'name': 'libpq_headers',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_libpq_headers.pl',
    ],
    'deps': [libpq_headers],
  },
}
