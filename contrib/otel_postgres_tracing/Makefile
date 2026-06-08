# contrib/otel_postgres_tracing/Makefile

MODULE_big = otel_postgres_tracing
OBJS = \
	$(WIN32RES) \
	otel_log.o \
	otel_postgres_tracing.o \
	otel_trace.o

PG_CPPFLAGS = -I$(top_srcdir)/contrib/otel

EXTENSION = otel_postgres_tracing
DATA = otel_postgres_tracing--1.0.sql
PGFILEDESC = "otel_postgres_tracing - OpenTelemetry instrumentation for PostgreSQL query execution"

NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
OTEL_PROBE_INC := $(shell $(PG_CONFIG) --includedir-server)
else
subdir = contrib/otel_postgres_tracing
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
OTEL_PROBE_INC := $(top_srcdir)/src/include
endif

# See contrib/otel/Makefile for the OTEL_HAVE_* feature gates.
# otel_postgres_tracing only depends on OTEL_HAVE_ERRANNOT.
ifneq (,$(shell grep -l '^extern int[[:space:]].*errannot' $(OTEL_PROBE_INC)/utils/elog.h 2>/dev/null))
PG_CPPFLAGS += -DOTEL_HAVE_ERRANNOT
endif
