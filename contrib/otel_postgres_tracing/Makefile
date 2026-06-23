# otel_postgres_tracing/Makefile

MODULE_big = otel_postgres_tracing
OBJS = \
	$(WIN32RES) \
	otel_log.o \
	otel_postgres_tracing.o \
	otel_sdt_bridge.o \
	otel_trace.o

EXTENSION = otel_postgres_tracing
DATA = otel_postgres_tracing--0.1.1.sql
PGFILEDESC = "otel_postgres_tracing - OpenTelemetry instrumentation for PostgreSQL query execution"

# No TAP suite lives in this directory --- the tests that exercise
# otel_postgres_tracing (log annotations, query tracing, sampler
# policy, sqlcommenter, ...) all live under tests/otel_test_exporter/t/
# because they need the test-only exporter to observe captured spans.
# NO_INSTALLCHECK suppresses PGXS's installcheck target outright.
NO_INSTALLCHECK = 1

# <otel_api/otel.h> resolves from:
#   * in-tree: contrib/otel_api/otel.h via -I$(top_srcdir)/contrib
#   * out-of-tree (PGXS): <pg_config --includedir-server>/extension/otel_api/otel.h
#
# All PG_CPPFLAGS additions (-I and -D feature gates) MUST appear BEFORE
# include $(PGXS) / Makefile.global because pgxs.mk bakes COMPILE.c at
# include time; later additions to PG_CPPFLAGS never reach the actual
# compile command line.
ifdef USE_PGXS
PG_CONFIG ?= pg_config
PG_CPPFLAGS = -I$(shell $(PG_CONFIG) --includedir-server)/extension
OTEL_PROBE_INC := $(shell $(PG_CONFIG) --includedir-server)
else
PG_CPPFLAGS = -I$(top_srcdir)/contrib
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

# The SDT-probe -> span bridge (otel_sdt_bridge.c) needs no build-system
# feature detection: it is gated on PG_HAVE_SDT_PROBE_HOOK, which the core
# patch advertises in <pg_config_manual.h>.  On a stock server that macro is
# absent and the bridge compiles to a no-op stub, so the module builds and
# loads with no unresolved pg_sdt_probe_hook symbol.

ifdef USE_PGXS
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/otel_postgres_tracing
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
