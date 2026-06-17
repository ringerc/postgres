# otel_api/Makefile

MODULE_big = otel_api
OBJS = \
	$(WIN32RES) \
	otel.o \
	otel_api.o \
	otel_parallel.o \
	otel_producer.o \
	otel_resource.o

EXTENSION = otel_api
DATA = otel_api--0.1.1.sql
HEADERS = otel.h otel_api.h
PGFILEDESC = "otel_api - OpenTelemetry trace-context API for extensions"

TAP_TESTS = 1

# Empty REGRESS/ISOLATION lists so PGXS's installcheck target is a
# no-op for the SQL-regression and isolation runners (neither could
# work anyway: this module's behaviour requires shared_preload_libraries,
# which a plain pg_regress installcheck can't reconfigure).  Leaving
# NO_INSTALLCHECK unset means PGXS still invokes prove_installcheck
# for the TAP suite under t/, which spins up its own temp clusters via
# PostgreSQL::Test::Cluster and configures shared_preload_libraries
# per-test.
REGRESS =
ISOLATION =

ifdef USE_PGXS
PG_CONFIG = pg_config
OTEL_PROBE_INC := $(shell $(PG_CONFIG) --includedir-server)
OTEL_PROBE_CC  := $(shell $(PG_CONFIG) --cc)
else
OTEL_PROBE_INC := $(top_srcdir)/src/include
# In-tree: CC is set by Makefile.global (included below), but at probe time Make
# uses its built-in default which is also 'cc' on every supported platform.
OTEL_PROBE_CC  := cc
endif

# Optional dependencies on core-postgres features.  otel_api can build
# against an unpatched server by detecting these at compile time and
# falling back to alternative paths when the features are absent.
#
#   OTEL_HAVE_TRACE_CONTEXT --- the 'M' TraceContext protocol
#       message (protocol 3.3+) + RegisterTraceContextHandler.
#       Without it, trace context can only enter via SET
#       otel.traceparent or sqlcommenter.
#
#   OTEL_HAVE_ERRANNOT --- generic errannot() / errannotf() helpers
#       and ErrorAnnotation list on ErrorData; %A / %{key}A in
#       log_line_prefix; trace context surfaces as named annotations
#       in JSON/CSV log output.  Without it, emit_log_hook injects
#       trace context into edata->context as a textual fallback so
#       it still surfaces in textual log destinations.
#
# Auto-detect, but allow override from the command line:
#
#   make USE_PGXS=1 ENABLE_TRACE_CONTEXT=0   # force-disable
#   make USE_PGXS=1 ENABLE_ERRANNOT=1        # force-enable
#
# NB: this block MUST appear BEFORE the PGXS include / Makefile.global
# include below.  PGXS evaluates COMPILE.c immediately when it pulls in
# Makefile.global, baking in the current value of PG_CPPFLAGS; any
# additions made after the include are silently lost from the actual
# compile command line.
#
# Detection uses compile probes rather than text-grepping headers so that
# signature changes, return-type changes, or the function moving to a
# different header do not silently disable the feature.  Each probe feeds
# a minimal C snippet to the compiler and gates on the exit code.
# -w suppresses all warnings; -Werror=implicit-function-declaration makes
# an undeclared function a hard error even under C17 (where it is
# otherwise a non-fatal warning).
ifeq ($(origin ENABLE_TRACE_CONTEXT),undefined)
  # Compile probe: include libpq/trace_context.h and reference the
  # TraceContextApplyCb type to confirm the header is present and usable.
  # A plain wildcard/header-presence test would also work here (the header
  # is new and specific to this feature), but we use a compile probe for
  # consistency and to catch cases where the header exists but cannot be
  # compiled (e.g. missing dependencies).
  ENABLE_TRACE_CONTEXT := $(shell \
    printf '%s\n' \
      '#include "postgres.h"' \
      '#include "libpq/trace_context.h"' \
      'static TraceContextApplyCb otel_probe_ = NULL;' \
    | $(OTEL_PROBE_CC) -w -c -I'$(OTEL_PROBE_INC)' -x c - -o /dev/null 2>/dev/null \
    && echo 1 || echo 0)
endif
ifeq ($(ENABLE_TRACE_CONTEXT),1)
  PG_CPPFLAGS += -DOTEL_HAVE_TRACE_CONTEXT
endif

ifeq ($(origin ENABLE_ERRANNOT),undefined)
  # Compile probe: include utils/elog.h and reference errannot() by name.
  # An undeclared function is a hard error with -Werror=implicit-function-declaration,
  # so the probe exits non-zero when errannot() is absent from the header,
  # regardless of return type or spelling changes.  This replaces the previous
  # text-grep which would silently mis-fire on return-type or header-location changes.
  ENABLE_ERRANNOT := $(shell \
    printf '%s\n' \
      '#include "postgres.h"' \
      '#include "utils/elog.h"' \
      'void otel_probe_(void) { (void)(errannot); }' \
    | $(OTEL_PROBE_CC) -w -Werror=implicit-function-declaration \
        -c -I'$(OTEL_PROBE_INC)' -x c - -o /dev/null 2>/dev/null \
    && echo 1 || echo 0)
endif
ifeq ($(ENABLE_ERRANNOT),1)
  PG_CPPFLAGS += -DOTEL_HAVE_ERRANNOT
endif

ifdef USE_PGXS
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/otel_api
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
