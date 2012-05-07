#include "postgres.h"

// these are the dummy UDFs needed to give postgres valid queries

// create function searchswp(a0 varchar, a1 varchar, a2 varchar, a3 int)
//    returns boolean language C as '/Users/stephentu/scala-sql/src/main/c/dummy_udfs.so';
// create function searchswp(a0 varchar, a1 varchar, a2 varchar, a3 int, a4 varchar)
//    returns boolean language C as '/Users/stephentu/scala-sql/src/main/c/dummy_udfs.so';
//
// create function dumb_add(a0 int, a1 decimal, a2 varchar, a3 int)
//    returns int language C as '/Users/stephentu/scala-sql/src/main/c/dummy_udfs.so';
// create function dumb_add(a0 int, a1 decimal, a2 varchar, a3 int, a4 varchar)
//    returns int language C as '/Users/stephentu/scala-sql/src/main/c/dummy_udfs.so';
//
// create aggregate hom_agg(decimal, varchar, int) (sfunc = dumb_add, stype = int);
// create aggregate hom_agg(decimal, varchar, int, varchar) (sfunc = dumb_add, stype = int);

PG_MODULE_MAGIC;

Datum searchswp(PG_FUNCTION_ARGS);
Datum dumb_add(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(searchswp);
PG_FUNCTION_INFO_V1(dumb_add);

Datum searchswp(PG_FUNCTION_ARGS) {
  PG_RETURN_BOOL(0);
}

Datum dumb_add(PG_FUNCTION_ARGS) {
  PG_RETURN_INT64(0);
}
