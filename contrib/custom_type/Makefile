# contrib/custom_type/Makefile

MODULE_big = custom_type
OBJS = \
	$(WIN32RES) \
	custom_type.o

EXTENSION = custom_type
DATA = custom_type--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/custom_type
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
