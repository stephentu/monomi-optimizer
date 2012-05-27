PWD=$(shell pwd)

PG_HOME=$(HOME)/pgsql
#PG_HOME=/usr/local/Cellar/postgresql/9.1.3

INC_PATH=$(PG_HOME)/include/server
PSQL=$(PG_HOME)/bin/psql

DB_NAME=tpch_10_00

UNAME=$(shell uname)
ifeq ($(UNAME), Linux)
	CFLAGS=-fPIC
	LDFLAGS=-shared
endif 
ifeq ($(UNAME), Darwin)
	CFLAGS=
	LDFLAGS=-bundle -flat_namespace -undefined suppress
endif 

all: dummy_udfs.c
	$(CC) $(CFLAGS) -c dummy_udfs.c -I$(INC_PATH)
	$(CC) $(LDFLAGS) -o dummy_udfs.so dummy_udfs.o

.PHONY: install-udfs
install-udfs:
	echo "create function searchswp(a0 varchar, a1 varchar, a2 varchar, a3 varchar) returns boolean language C as '$(PWD)/dummy_udfs.so'" | $(PSQL) -d $(DB_NAME)
	echo "create function dumb_add(a0 int, a1 decimal, a2 varchar, a3 int) returns int language C as '$(PWD)/dummy_udfs.so'" | $(PSQL) -d $(DB_NAME)
	echo "create function dumb_add(a0 int, a1 decimal, a2 varchar, a3 int, a4 varchar) returns int language C as '$(PWD)/dummy_udfs.so'" | $(PSQL) -d $(DB_NAME)
	echo "create aggregate hom_agg(decimal, varchar, int) (sfunc = dumb_add, stype = int)" | $(PSQL) -d $(DB_NAME)
	echo "create aggregate hom_agg(decimal, varchar, int, varchar) (sfunc = dumb_add, stype = int)" | $(PSQL) -d $(DB_NAME)

.PHONY: clean
clean:
	rm -rf *.o *.so
