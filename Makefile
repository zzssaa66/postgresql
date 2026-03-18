# Makefile for myam extension

MODULE_big = myam
# Build both the AM handler and background worker into the same shared library
OBJS = myam_am.o myam_bgworker.o

EXTENSION = myam
DATA = myam--1.0.sql

# Use local server's pg_config to build via PGXS (align with /usr/local/pgsql)
PG_CONFIG = /usr/local/pgsql/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)