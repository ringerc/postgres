# otel_demo_exporter/Makefile

MODULE_big = otel_demo_exporter
OBJS = \
	$(WIN32RES) \
	otel_demo_exporter.o

EXTENSION = otel_demo_exporter
DATA = otel_demo_exporter--0.1.1.sql
PGFILEDESC = "otel_demo_exporter - bare-minimum file exporter for otel_api spans"

TAP_TESTS = 1

# Activated via shared_preload_libraries; plain installcheck has neither
# that preload nor the otel_api module loaded first.
NO_INSTALLCHECK = 1

# <otel_api/otel.h> is provided by the otel_api extension.  In-tree we
# look in contrib/otel_api/ via -I$(top_srcdir)/contrib; under PGXS we
# pick it up from <pg_config --includedir-server>/extension/otel_api/.
ifdef USE_PGXS
PG_CONFIG = pg_config
PG_CPPFLAGS = -I$(shell $(PG_CONFIG) --includedir-server)/extension
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
PG_CPPFLAGS = -I$(top_srcdir)/contrib
subdir = contrib/otel_demo_exporter
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
