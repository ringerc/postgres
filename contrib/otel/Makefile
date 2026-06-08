# contrib/otel/Makefile

MODULE_big = otel
OBJS = \
	$(WIN32RES) \
	otel.o \
	otel_api.o \
	otel_parallel.o \
	otel_producer.o \
	otel_resource.o

EXTENSION = otel
DATA = otel--1.0.sql
HEADERS = otel.h otel_api.h
PGFILEDESC = "otel - OpenTelemetry trace-context support"

TAP_TESTS = 1

# CREATE EXTENSION exposes one introspection function, but the module's
# trace-context handler is registered from _PG_init() and therefore only
# functions when otel is loaded via shared_preload_libraries.  The TAP
# test takes care of that; a plain installcheck would have neither the
# preload nor the protocol-level client and is therefore disabled.
NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
OTEL_PROBE_INC := $(shell $(PG_CONFIG) --includedir-server)
else
subdir = contrib/otel
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
OTEL_PROBE_INC := $(top_srcdir)/src/include
endif

# Optional dependencies on core-postgres features.  contrib/otel can
# build against an unpatched server by detecting these at compile time
# and falling back to alternative paths when the features are absent.
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
ifneq (,$(wildcard $(OTEL_PROBE_INC)/libpq/protocol_headers.h))
PG_CPPFLAGS += -DOTEL_HAVE_PROTOCOL_HEADERS
endif
ifneq (,$(shell grep -l '^extern int[[:space:]].*errannot' $(OTEL_PROBE_INC)/utils/elog.h 2>/dev/null))
PG_CPPFLAGS += -DOTEL_HAVE_ERRANNOT
endif
