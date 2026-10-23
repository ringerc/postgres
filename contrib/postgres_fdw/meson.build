# Copyright (c) 2022-2026, PostgreSQL Global Development Group

postgres_fdw_sources = files(
  'connection.c',
  'deparse.c',
  'option.c',
  'postgres_fdw.c',
  'shippable.c',
)

if host_system == 'windows'
  postgres_fdw_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'postgres_fdw',
    '--FILEDESC', 'postgres_fdw - foreign data wrapper for PostgreSQL',])
endif

postgres_fdw = shared_module('postgres_fdw',
  postgres_fdw_sources,
  # The in-tree build defines HAVE_OTEL_API unconditionally: contrib/otel_api
  # is always present in-tree, so postgres_fdw.c uses the real
  # otel_api/otel_producer.h instead of falling back to the vendored
  # no-op otel_producer_stub.h.  <otel_api/otel_producer.h> resolves from
  # the otel_api SOURCE tree (this module's sibling directory under
  # contrib/), same as contrib/otel_postgres_tracing.  All functions used
  # from it are "static inline" (no extern symbols), so no link
  # dependency on otel_api is needed here, only the include path.
  # Out-of-tree PGXS builds keep HAVE_OTEL_API opt-in (see the Makefile).
  c_args: ['-DHAVE_OTEL_API', '-I' + meson.current_source_dir() / '..'],
  kwargs: contrib_mod_args + {
    'dependencies': contrib_mod_args['dependencies'] + [libpq],
  },
)
contrib_targets += postgres_fdw

install_data(
  'postgres_fdw.control',
  'postgres_fdw--1.0.sql',
  'postgres_fdw--1.0--1.1.sql',
  'postgres_fdw--1.1--1.2.sql',
  'postgres_fdw--1.2--1.3.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'postgres_fdw',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'postgres_fdw',
      'query_cancel',
    ],
    'regress_args': ['--dlpath', meson.project_build_root() / 'src/test/regress'],
  },
  'isolation': {
    'specs': [
      'eval_plan_qual',
    ],
    'regress_args': ['--load-extension=postgres_fdw'],
  },
  'tap': {
    'tests': [
      't/001_auth_scram.pl',
      't/002_otel_spans.pl',
      't/010_subscription.pl',
    ],
  },
}
