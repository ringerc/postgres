# contrib/otel/Makefile

MODULE_big = otel
OBJS = \
	$(WIN32RES) \
	otel.o \
	otel_log.o

EXTENSION = otel
DATA = otel--1.0.sql
PGFILEDESC = "otel - OpenTelemetry trace-context support"

TAP_TESTS = 1

# CREATE EXTENSION exposes one introspection function, but the module's
# trace-context handler is registered from _PG_init() and therefore only
# functions when otel is loaded via shared_preload_libraries.  The TAP
# test takes care of that; a plain installcheck would have neither the
# preload nor the protocol-level client and is therefore disabled.
NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/otel
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
