# Copyright (c) 2022-2026, PostgreSQL Global Development Group

test_trace_context_sources = files(
  'test_trace_context.c',
)

if host_system == 'windows'
  test_trace_context_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_trace_context',
    '--FILEDESC', 'test_trace_context - test the trace-context protocol message mechanism',])
endif

test_trace_context = shared_module('test_trace_context',
  test_trace_context_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_trace_context

tests += {
  'name': 'test_trace_context',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_trace_context.pl',
    ],
  },
}
