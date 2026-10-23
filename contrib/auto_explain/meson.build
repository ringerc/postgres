# Copyright (c) 2022-2026, PostgreSQL Global Development Group

auto_explain_sources = files(
  'auto_explain.c',
)

if host_system == 'windows'
  auto_explain_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'auto_explain',
    '--FILEDESC', 'auto_explain - logging facility for execution plans',])
endif

auto_explain = shared_module('auto_explain',
  auto_explain_sources,
  # The in-tree build defines HAVE_OTEL_API unconditionally: contrib/otel_api
  # is always present in-tree, so auto_explain.c uses the real
  # otel_api/otel_producer.h instead of falling back to the vendored
  # no-op otel_producer_stub.h.  <otel_api/otel_producer.h> resolves from
  # the otel_api SOURCE tree (this module's sibling directory under
  # contrib/), same as contrib/otel_postgres_tracing.  All functions used
  # from it are "static inline" (no extern symbols), so no link
  # dependency on otel_api is needed here, only the include path.
  # Out-of-tree PGXS builds keep HAVE_OTEL_API opt-in (see the Makefile).
  c_args: ['-DHAVE_OTEL_API', '-I' + meson.current_source_dir() / '..'],
  kwargs: contrib_mod_args,
)
contrib_targets += auto_explain

tests += {
  'name': 'auto_explain',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'alter_reset',
      'extension_options',
    ],
  },
  'tap': {
    'tests': [
      't/001_auto_explain.pl',
      't/002_otel_spans.pl',
    ],
  },
}
