# src/test/modules/libpq_trace_context/Makefile

PGFILEDESC = "libpq_trace_context - test client for trace-context protocol message"
PGAPPICON = win32

PROGRAM = libpq_trace_context
OBJS = $(WIN32RES) libpq_trace_context.o

NO_INSTALL = 1

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS_INTERNAL += $(libpq_pgport)

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/libpq_trace_context
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
