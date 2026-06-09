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

# CREATE EXTENSION exposes one introspection function, but the module's
# trace-context handler is registered from _PG_init() and therefore only
# functions when otel_api is loaded via shared_preload_libraries.  The TAP
# test takes care of that; a plain installcheck would have neither the
# preload nor the protocol-level client and is therefore disabled.
NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
OTEL_PROBE_INC := $(shell $(PG_CONFIG) --includedir-server)
else
subdir = contrib/otel_api
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
OTEL_PROBE_INC := $(top_srcdir)/src/include
endif

# Optional dependencies on core-postgres features.  otel_api can build
# against an unpatched server by detecting these at compile time and
# falling back to alternative paths when the features are absent.
#
#   OTEL_HAVE_PROTOCOL_HEADERS --- the 'M' RequestHeaders protocol
#       message + RegisterProtocolHeaderHandler.  Without it, trace
#       context can only enter via SET otel.traceparent or
#       sqlcommenter.
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
#   make USE_PGXS=1 ENABLE_PROTOCOL_HEADERS=0   # force-disable
#   make USE_PGXS=1 ENABLE_ERRANNOT=1           # force-enable
ifeq ($(origin ENABLE_PROTOCOL_HEADERS),undefined)
  ifneq (,$(wildcard $(OTEL_PROBE_INC)/libpq/protocol_headers.h))
    ENABLE_PROTOCOL_HEADERS = 1
  else
    ENABLE_PROTOCOL_HEADERS = 0
  endif
endif
ifeq ($(ENABLE_PROTOCOL_HEADERS),1)
  PG_CPPFLAGS += -DOTEL_HAVE_PROTOCOL_HEADERS
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
