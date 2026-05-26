# src/test/modules/test_protocol_headers/Makefile

MODULE_big = test_protocol_headers
OBJS = \
	$(WIN32RES) \
	test_protocol_headers.o
PGFILEDESC = "test_protocol_headers - test the per-message protocol headers mechanism"

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_protocol_headers
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
