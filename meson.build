# Copyright (c) 2026, PostgreSQL Global Development Group

otel_postgres_tracing_sources = files(
  'otel_log.c',
  'otel_postgres_tracing.c',
  'otel_sdt_bridge.c',
  'otel_trace.c',
)

if host_system == 'windows'
  otel_postgres_tracing_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'otel_postgres_tracing',
    '--FILEDESC', 'otel_postgres_tracing - OpenTelemetry instrumentation for PostgreSQL query execution',])
endif

# See contrib/otel/meson.build for the OTEL_HAVE_* feature gates.
# otel_postgres_tracing only depends on OTEL_HAVE_ERRANNOT.  Text-grep
# the header (not a compile probe) because elog.h pulls in
# errcodes.h, which is generated at build time and isn't present yet
# when meson setup runs.
otel_pgt_cppflags = []
otel_pgt_fs = import('fs')
otel_pgt_elog_h = meson.project_source_root() / 'src/include/utils/elog.h'
if otel_pgt_fs.exists(otel_pgt_elog_h)
  otel_pgt_elog_text = otel_pgt_fs.read(otel_pgt_elog_h)
  if otel_pgt_elog_text.contains('errannot(')
    otel_pgt_cppflags += '-DOTEL_HAVE_ERRANNOT'
  endif
endif

# The SDT-probe -> span bridge needs no build-system detection: it is gated on
# PG_HAVE_SDT_PROBE_HOOK, advertised by the core patch in <pg_config_manual.h>.
# On a stock server the macro is absent and otel_sdt_bridge.c compiles to a
# no-op stub.

otel_postgres_tracing = shared_module('otel_postgres_tracing',
  otel_postgres_tracing_sources,
  # <otel_api/otel.h> resolves from contrib/otel_api/ in the in-tree
  # layout via -I.../contrib.
  c_args: ['-I' + meson.project_source_root() / 'contrib'] + otel_pgt_cppflags,
  kwargs: contrib_mod_args,
)
contrib_targets += otel_postgres_tracing

install_data(
  'otel_postgres_tracing.control',
  'otel_postgres_tracing--0.1.1.sql',
  kwargs: contrib_data_args,
)
