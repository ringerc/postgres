# Copyright (c) 2022-2026, PostgreSQL Global Development Group

libpq_trace_context_sources = files(
  'libpq_trace_context.c',
)

if host_system == 'windows'
  libpq_trace_context_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'libpq_trace_context',
    '--FILEDESC', 'libpq_trace_context - test client for trace-context protocol message',])
endif

libpq_trace_context = executable('libpq_trace_context',
  libpq_trace_context_sources,
  dependencies: [frontend_code, libpq],
  kwargs: default_bin_args + {
    'install': false,
  },
)
testprep_targets += libpq_trace_context

tests += {
  'name': 'libpq_trace_context',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_libpq_trace_context.pl',
    ],
    'deps': [libpq_trace_context],
  },
}
