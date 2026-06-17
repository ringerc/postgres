# src/test/modules/test_elog_annotations/Makefile

MODULES = test_elog_annotations

EXTENSION = test_elog_annotations
DATA = test_elog_annotations--1.0.sql
PGFILEDESC = "test_elog_annotations - exercises errannot()/errannotf() plumbing"

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_elog_annotations
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
