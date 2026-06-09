# Copyright (c) 2022-2026, PostgreSQL Global Development Group

otel_api_sources = files(
  'otel.c',
  'otel_api.c',
  'otel_parallel.c',
  'otel_producer.c',
  'otel_resource.c',
)

if host_system == 'windows'
  otel_api_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'otel_api',
    '--FILEDESC', 'otel_api - OpenTelemetry trace-context API for extensions',])
endif

# Optional dependencies on core-postgres features.  otel_api builds
# against an unpatched server by detecting these at compile time;
# missing features fall back to alternative paths.  Matching
# Makefile probes live in otel_api/Makefile (out-of-tree path).
otel_api_include_args = [
  '-I' + meson.project_source_root() / 'src/include',
  '-I' + meson.project_build_root() / 'src/include',
]
otel_api_cppflags = []
if cc.has_header('libpq/protocol_headers.h', args: otel_api_include_args)
  otel_api_cppflags += '-DOTEL_HAVE_PROTOCOL_HEADERS'
endif
# Detect errannot() in utils/elog.h.  A compile probe doesn't work
# here: utils/elog.h includes utils/errcodes.h, which is generated
# at build time and isn't present yet when meson setup runs against
# a fresh build directory.  Text-grep the header instead --- robust
# and identical in behaviour to what the matching Makefile probe does.
otel_api_fs = import('fs')
otel_api_elog_h = meson.project_source_root() / 'src/include/utils/elog.h'
if otel_api_fs.exists(otel_api_elog_h)
  otel_api_elog_text = otel_api_fs.read(otel_api_elog_h)
  if otel_api_elog_text.contains('errannot(')
    otel_api_cppflags += '-DOTEL_HAVE_ERRANNOT'
  endif
endif

otel_api = shared_module('otel_api',
  otel_api_sources,
  c_args: otel_api_cppflags,
  kwargs: contrib_mod_args,
)
contrib_targets += otel_api

install_data(
  'otel_api.control',
  'otel_api--0.1.1.sql',
  kwargs: contrib_data_args,
)

install_headers(
  'otel.h',
  'otel_api.h',
  install_dir: dir_include_extension / 'otel_api',
)

tests += {
  'name': 'otel_api',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_otel.pl',
    ],
  },
}
