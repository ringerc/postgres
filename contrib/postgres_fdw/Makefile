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

# "+=" (not "="): preserves an out-of-tree builder's own PG_CPPFLAGS
# (e.g. "env PG_CPPFLAGS=-DHAVE_OTEL_API") instead of silently discarding
# it -- a plain "=" here would stomp that opt-in before the ifdef
# USE_PGXS block below ever runs.
PG_CPPFLAGS += -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

EXTENSION = postgres_fdw
DATA = postgres_fdw--1.0.sql postgres_fdw--1.0--1.1.sql postgres_fdw--1.1--1.2.sql

REGRESS = postgres_fdw query_cancel
ISOLATION = eval_plan_qual
ISOLATION_OPTS = --load-extension=postgres_fdw
TAP_TESTS = 1

# otel_api/otel_producer.h (optional OTel instrumentation; built with
# -DHAVE_OTEL_API) is resolved via the server extension include path below
# for an out-of-tree (PGXS) build, where HAVE_OTEL_API remains opt-in.
# The in-tree build defines HAVE_OTEL_API unconditionally (contrib/otel_api
# is always present in-tree) and resolves the header from the source tree
# instead, via $(top_srcdir)/contrib.  Without HAVE_OTEL_API, postgres_fdw.c
# falls back to the vendored otel_producer_stub.h in this directory.
# PG_CPPFLAGS additions MUST precede include $(PGXS) / Makefile.global.
ifdef USE_PGXS
PG_CONFIG ?= pg_config
PG_CPPFLAGS += -I$(shell $(PG_CONFIG) --includedir-server)/extension
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
PG_CPPFLAGS += -DHAVE_OTEL_API -I$(top_srcdir)/contrib
SHLIB_PREREQS = submake-libpq
subdir = contrib/postgres_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
