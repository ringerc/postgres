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
#
# Note: unlike the PGXS Makefile path (which supports ENABLE_TRACE_CONTEXT=0
# and ENABLE_ERRANNOT=0 overrides on the make command line), the meson path
# auto-detects only.  Force-disable/enable via meson would require entries in
# meson_options.txt and get_option() calls; that is not wired up here because
# the in-tree build is not the primary development path for this extension.
otel_api_include_args = [
  '-I' + meson.project_source_root() / 'src/include',
  '-I' + meson.project_build_root() / 'src/include',
]
otel_api_cppflags = []

# OTEL_HAVE_TRACE_CONTEXT: compile probe — include libpq/trace_context.h
# and reference TraceContextApplyCb to confirm the header is present and
# the types it introduces are usable.  cc.has_header() is a pure
# preprocessor check; cc.compiles() with a type reference is more robust
# (catches the case where the header exists but fails to compile).
if cc.compiles(
  '''
  #include "postgres.h"
  #include "libpq/trace_context.h"
  static TraceContextApplyCb otel_probe_ = ((void *)0);
  ''',
  args: otel_api_include_args,
  name: 'libpq/trace_context.h (TraceContextApplyCb)',
)
  otel_api_cppflags += '-DOTEL_HAVE_TRACE_CONTEXT'
endif

# OTEL_HAVE_ERRANNOT: compile probe — include utils/elog.h and reference
# errannot() by name.  cc.has_function() requires a linkable library and
# is unsuitable here (we are probing a header-only declaration in an
# extension that must not link against libpq-server directly).
# cc.has_header_symbol() is a preprocessor macro/declaration check; it
# won't verify the function is callable.
# cc.compiles() with a function reference is the right tool: the
# compiler rejects an undeclared function even in C17 when the declaration
# is absent from the included headers, which is exactly the defect the
# old text-grep could not catch (wrong return type, moved header, etc.).
#
# Note on errcodes.h: on the elog-annotations branch, utils/elog.h gains
# an #include "utils/errcodes.h".  errcodes.h is a generated file
# (custom_target in src/include/utils/meson.build) that exists in the
# build directory only after at least one `meson compile` run.  On a
# completely fresh build tree — one that has never been compiled — the
# probe will fail to compile because errcodes.h is absent, and the
# feature will not be detected.  This is a safe false-negative: the
# extension builds without errannot support rather than breaking.  On any
# previously-built tree (the typical in-tree development path), errcodes.h
# is present and the probe works correctly.
if cc.compiles(
  '''
  #include "postgres.h"
  #include "utils/elog.h"
  static void otel_probe_(void) { (void)(errannot); }
  ''',
  args: otel_api_include_args,
  name: 'errannot() in utils/elog.h',
)
  otel_api_cppflags += '-DOTEL_HAVE_ERRANNOT'
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
