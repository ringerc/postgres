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
else
OTEL_PROBE_INC := $(top_srcdir)/src/include
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
ifeq ($(origin ENABLE_TRACE_CONTEXT),undefined)
  ifneq (,$(wildcard $(OTEL_PROBE_INC)/libpq/trace_context.h))
    ENABLE_TRACE_CONTEXT = 1
  else
    ENABLE_TRACE_CONTEXT = 0
  endif
endif
ifeq ($(ENABLE_TRACE_CONTEXT),1)
  PG_CPPFLAGS += -DOTEL_HAVE_TRACE_CONTEXT
endif

ifeq ($(origin ENABLE_ERRANNOT),undefined)
  ifneq (,$(shell grep -l '^extern int[[:space:]].*errannot' $(OTEL_PROBE_INC)/utils/elog.h 2>/dev/null))
    ENABLE_ERRANNOT = 1
  else
    ENABLE_ERRANNOT = 0
  endif
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
