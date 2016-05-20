# src/test/modules/test_libpq/Makefile

OBJS = testlibpqbatch.o
PROGRAM = testlibpqbatch

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS += $(libpq)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_libpq
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

check: prove-check

prove-check:
	$(prove_check)
