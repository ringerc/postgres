# Copyright (c) 2022-2026, PostgreSQL Global Development Group

otel_sources = files(
  'otel.c',
  'otel_api.c',
  'otel_parallel.c',
  'otel_producer.c',
  'otel_resource.c',
)

if host_system == 'windows'
  otel_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'otel',
    '--FILEDESC', 'otel - OpenTelemetry trace-context support',])
endif

# Optional dependencies on core-postgres features.  contrib/otel
# builds against an unpatched server by detecting these at compile
# time; missing features fall back to alternative paths.
# Matching Makefile probes live in contrib/otel/Makefile.
otel_include_args = [
  '-I' + meson.project_source_root() / 'src/include',
  '-I' + meson.project_build_root() / 'src/include',
]
otel_cppflags = []
if cc.has_header('libpq/protocol_headers.h', args: otel_include_args)
  otel_cppflags += '-DOTEL_HAVE_PROTOCOL_HEADERS'
endif
# Detect errannot() in utils/elog.h.  A compile probe doesn't work
# here: utils/elog.h includes utils/errcodes.h, which is generated
# at build time and isn't present yet when meson setup runs against
# a fresh build directory.  Text-grep the header instead --- robust
# and identical in behaviour to what the matching Makefile probe does.
otel_fs = import('fs')
otel_elog_h = meson.project_source_root() / 'src/include/utils/elog.h'
if otel_fs.exists(otel_elog_h)
  otel_elog_text = otel_fs.read(otel_elog_h)
  if otel_elog_text.contains('errannot(')
    otel_cppflags += '-DOTEL_HAVE_ERRANNOT'
  endif
endif

otel = shared_module('otel',
  otel_sources,
  c_args: otel_cppflags,
  kwargs: contrib_mod_args,
)
contrib_targets += otel

install_data(
  'otel.control',
  'otel--1.0.sql',
  kwargs: contrib_data_args,
)

install_headers(
  'otel.h',
  'otel_api.h',
  install_dir: dir_include_extension / 'otel',
)

tests += {
  'name': 'otel',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_otel.pl',
    ],
  },
}
