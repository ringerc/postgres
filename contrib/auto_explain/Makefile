# contrib/auto_explain/Makefile

MODULE_big = auto_explain
OBJS = \
	$(WIN32RES) \
	auto_explain.o
PGFILEDESC = "auto_explain - logging facility for execution plans"

EXTRA_INSTALL = contrib/pg_overexplain
REGRESS = alter_reset extension_options

TAP_TESTS = 1

# Add otel_api header path (optional OTel span enrichment).  PG_CPPFLAGS
# additions MUST precede include $(PGXS) / Makefile.global.
ifdef USE_PGXS
PG_CONFIG = pg_config
PG_CPPFLAGS += -I$(shell $(PG_CONFIG) --includedir-server)/extension
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
PG_CPPFLAGS += -I$(top_srcdir)/contrib
subdir = contrib/auto_explain
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
