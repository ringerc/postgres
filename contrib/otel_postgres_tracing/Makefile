# otel_postgres_tracing/Makefile

MODULE_big = otel_postgres_tracing
OBJS = \
	$(WIN32RES) \
	otel_log.o \
	otel_postgres_tracing.o \
	otel_trace.o

EXTENSION = otel_postgres_tracing
DATA = otel_postgres_tracing--0.1.1.sql
PGFILEDESC = "otel_postgres_tracing - OpenTelemetry instrumentation for PostgreSQL query execution"

NO_INSTALLCHECK = 1

# <otel_api/otel.h> resolves from:
#   * in-tree: contrib/otel_api/otel.h via -I$(top_srcdir)/contrib
#   * out-of-tree (PGXS): <pg_config --includedir-server>/extension/otel_api/otel.h
#
# Both branches set PG_CPPFLAGS BEFORE include $(PGXS) because pgxs.mk
# bakes it into the compile rules at include time.
ifdef USE_PGXS
PG_CONFIG ?= pg_config
PG_CPPFLAGS = -I$(shell $(PG_CONFIG) --includedir-server)/extension
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
OTEL_PROBE_INC := $(shell $(PG_CONFIG) --includedir-server)
else
PG_CPPFLAGS = -I$(top_srcdir)/contrib
subdir = contrib/otel_postgres_tracing
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
OTEL_PROBE_INC := $(top_srcdir)/src/include
endif

# See otel_api/Makefile for the OTEL_HAVE_* feature gates.
# otel_postgres_tracing only depends on OTEL_HAVE_ERRANNOT.
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
