# src/test/modules/libpq_headers/Makefile

PGFILEDESC = "libpq_headers - test client for per-message protocol headers"
PGAPPICON = win32

PROGRAM = libpq_headers
OBJS = $(WIN32RES) libpq_headers.o

NO_INSTALL = 1

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS_INTERNAL += $(libpq_pgport)

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/libpq_headers
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
