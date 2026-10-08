# contrib/auto_explain/Makefile

MODULE_big = auto_explain
OBJS = \
	$(WIN32RES) \
	auto_explain.o
PGFILEDESC = "auto_explain - logging facility for execution plans"

EXTRA_INSTALL = contrib/pg_overexplain
REGRESS = alter_reset extension_options

TAP_TESTS = 1

# otel_api/otel_producer.h (optional OTel span enrichment; built with
# -DHAVE_OTEL_API) is resolved via the server extension include path below.
# Without HAVE_OTEL_API, auto_explain.c falls back to the vendored
# otel_producer_stub.h in this directory, needing no extra include path.
# PG_CPPFLAGS additions MUST precede include $(PGXS) / Makefile.global.
ifdef USE_PGXS
PG_CONFIG = pg_config
PG_CPPFLAGS += -I$(shell $(PG_CONFIG) --includedir-server)/extension
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/auto_explain
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
