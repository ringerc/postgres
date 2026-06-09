# tests/otel_test_exporter/Makefile
#
# Test-only span exporter used by the otel_api TAP suites. Built like a
# normal PGXS extension, but the standalone repo's top-level Makefile
# excludes this directory from `make install` so it isn't shipped to
# production installs. In the in-tree (postgres source) layout this
# module lives at src/test/modules/otel_test_exporter/, where the
# postgres build infrastructure handles the same "build but don't
# install in release" convention.

MODULE_big = test_otel_exporter
OBJS = \
	$(WIN32RES) \
	test_otel_exporter.o
PGFILEDESC = "test_otel_exporter - test-only span exporter for otel_api"

EXTENSION = test_otel_exporter
DATA = test_otel_exporter--0.1.1.sql

TAP_TESTS = 1
NO_INSTALLCHECK = 1

# <otel_api/otel.h> --- same in-tree vs PGXS paths as the consumers.
ifdef USE_PGXS
PG_CONFIG = pg_config
PG_CPPFLAGS = -I$(shell $(PG_CONFIG) --includedir-server)/extension
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
PG_CPPFLAGS = -I$(top_srcdir)/contrib
subdir = src/test/modules/otel_test_exporter
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
