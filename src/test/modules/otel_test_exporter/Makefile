# src/test/modules/otel_test_exporter/Makefile

MODULE_big = test_otel_exporter
OBJS = \
	$(WIN32RES) \
	test_otel_exporter.o
PGFILEDESC = "test_otel_exporter - test-only span exporter for contrib/otel"

EXTENSION = test_otel_exporter
DATA = test_otel_exporter--1.0.sql

TAP_TESTS = 1
NO_INSTALLCHECK = 1

PG_CPPFLAGS = -I$(top_srcdir)/contrib/otel

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/otel_test_exporter
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
