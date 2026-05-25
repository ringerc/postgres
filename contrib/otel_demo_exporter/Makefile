# contrib/otel_demo_exporter/Makefile

MODULE_big = otel_demo_exporter
OBJS = \
	$(WIN32RES) \
	otel_demo_exporter.o

EXTENSION = otel_demo_exporter
DATA = otel_demo_exporter--1.0.sql
PGFILEDESC = "otel_demo_exporter - bare-minimum file exporter for contrib/otel spans"

TAP_TESTS = 1

# Activated via shared_preload_libraries; plain installcheck has neither
# that preload nor the contrib/otel module loaded first.
NO_INSTALLCHECK = 1

# <otel/otel.h> is provided by contrib/otel.  In-tree we look in
# the source tree; under PGXS we pick it up from the installed
# server include dir.
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
