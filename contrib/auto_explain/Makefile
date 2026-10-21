# contrib/auto_explain/Makefile

MODULE_big = auto_explain
OBJS = \
	$(WIN32RES) \
	auto_explain.o
PGFILEDESC = "auto_explain - logging facility for execution plans"

REGRESS = alter_reset

TAP_TESTS = 1

# otel_api/otel_producer.h (optional OTel span enrichment; built with
# -DHAVE_OTEL_API) is resolved via the server extension include path below
# for an out-of-tree (PGXS) build, where HAVE_OTEL_API remains opt-in.
# The in-tree build defines HAVE_OTEL_API unconditionally (contrib/otel_api
# is always present in-tree) and resolves the header from the source tree
# instead, via $(top_srcdir)/contrib.  Without HAVE_OTEL_API, auto_explain.c
# falls back to the vendored otel_producer_stub.h in this directory.
# PG_CPPFLAGS additions MUST precede include $(PGXS) / Makefile.global.
ifdef USE_PGXS
PG_CONFIG = pg_config
PG_CPPFLAGS += -I$(shell $(PG_CONFIG) --includedir-server)/extension
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
PG_CPPFLAGS += -DHAVE_OTEL_API -I$(top_srcdir)/contrib
subdir = contrib/auto_explain
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
