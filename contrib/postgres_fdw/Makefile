# contrib/postgres_fdw/Makefile

MODULE_big = postgres_fdw
OBJS = \
	$(WIN32RES) \
	connection.o \
	deparse.o \
	option.o \
	postgres_fdw.o \
	shippable.o
PGFILEDESC = "postgres_fdw - foreign data wrapper for PostgreSQL"

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

EXTENSION = postgres_fdw
DATA = postgres_fdw--1.0.sql postgres_fdw--1.0--1.1.sql postgres_fdw--1.1--1.2.sql postgres_fdw--1.2--1.3.sql

REGRESS = postgres_fdw query_cancel
ISOLATION = eval_plan_qual
ISOLATION_OPTS = --load-extension=postgres_fdw
TAP_TESTS = 1

# Add otel_api header path (optional OTel instrumentation).  PG_CPPFLAGS
# additions MUST precede include $(PGXS) / Makefile.global.
ifdef USE_PGXS
PG_CONFIG ?= pg_config
PG_CPPFLAGS += -I$(shell $(PG_CONFIG) --includedir-server)/extension
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
PG_CPPFLAGS += -I$(top_srcdir)/contrib
subdir = contrib/postgres_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
