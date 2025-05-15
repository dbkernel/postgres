# contrib/sandbox/Makefile

MODULE_big = sandbox
OBJS = \
	$(WIN32RES) \
	sandbox.o \
	sandbox_guc.o

EXTENSION = sandbox
DATA = sandbox--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/sandbox
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
