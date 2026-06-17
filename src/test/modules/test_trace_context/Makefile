# src/test/modules/test_trace_context/Makefile

MODULE_big = test_trace_context
OBJS = \
	$(WIN32RES) \
	test_trace_context.o
PGFILEDESC = "test_trace_context - test the trace-context protocol message mechanism"

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_trace_context
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
