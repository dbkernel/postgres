# contrib/omnitype/Makefile

MODULE_big = omnitype
OBJS = \
	$(WIN32RES) \
	omnitype.o \
	complex.o \
	mytext.o \
	tinyint.o

EXTENSION = omnitype
DATA = omnitype--1.0.sql
PG_CPPFLAGS = -std=c99 # PG 默认采用 C90 标准

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
