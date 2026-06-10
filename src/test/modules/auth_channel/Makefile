# src/test/modules/auth_channel/Makefile
#
# TAP-only test module for the protocol-level authorization channel
# (irrevocable-privilege-drop Phase 4).  Exercises the V / e / U / b
# frontend tags and Y AuthLockResponse backend message via raw
# socket I/O — no libpq dependency.

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/auth_channel
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
