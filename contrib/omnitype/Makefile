# contrib/omnitype/Makefile

MODULE_big = omnitype
OBJS = \
	$(WIN32RES) \
	omni_utils.o \
	omnitype.o \
	complex.o \
	mytext.o \
	tinyint.o \
	tinyint_v2.o

EXTENSION = omnitype
DATA = omnitype--1.0.sql
PG_CFLAGS += -Wno-declaration-after-statement

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/omnitype
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
