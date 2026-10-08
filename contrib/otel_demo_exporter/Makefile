# otel_demo_exporter/Makefile

MODULE_big = otel_demo_exporter
OBJS = \
	$(WIN32RES) \
	otel_demo_exporter.o

EXTENSION = otel_demo_exporter
DATA = otel_demo_exporter--0.1.1.sql
PGFILEDESC = "otel_demo_exporter - bare-minimum file exporter for otel_api spans"

TAP_TESTS = 1

# See otel_api/Makefile for the rationale: empty REGRESS / ISOLATION
# make PGXS's installcheck a no-op for the SQL-regression and isolation
# runners (which can't reconfigure shared_preload_libraries), while
# leaving NO_INSTALLCHECK unset so prove_installcheck still runs the
# TAP suite under t/.
REGRESS =
ISOLATION =

# <otel_api/otel.h> resolves from the otel_api SOURCE tree (this
# module's sibling directory), not any installed copy: the P2 API
# reshape is still in flight and an installed copy may be stale.
# $(srcdir) is set correctly under VPATH by pgxs.mk.
ifdef USE_PGXS
PG_CONFIG = pg_config
PG_CPPFLAGS = -I$(srcdir)/..
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
PG_CPPFLAGS = -I$(top_srcdir)/contrib
subdir = contrib/otel_demo_exporter
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
