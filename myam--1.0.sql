-- myam access method extension

-- Create the myam_handler function
CREATE FUNCTION myam_handler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME', 'myam_handler'
LANGUAGE C STRICT;

-- Create the myam access method
CREATE ACCESS METHOD myam TYPE INDEX HANDLER myam_handler;

-- Create operator classes for common data types
-- Integer operator class
CREATE OPERATOR CLASS int4_myam_ops
    DEFAULT FOR TYPE int4 USING myam AS
        OPERATOR        1       <,
        OPERATOR        2       <=,
        OPERATOR        3       =,
        OPERATOR        4       >=,
        OPERATOR        5       >,
        FUNCTION        1       btint4cmp(int4, int4);

-- Text operator class
CREATE OPERATOR CLASS text_myam_ops
    DEFAULT FOR TYPE text USING myam AS
        OPERATOR        1       <,
        OPERATOR        2       <=,
        OPERATOR        3       =,
        OPERATOR        4       >=,
        OPERATOR        5       >,
        FUNCTION        1       bttextcmp(text, text);

-- Bigint operator class
CREATE OPERATOR CLASS int8_myam_ops
    DEFAULT FOR TYPE int8 USING myam AS
        OPERATOR        1       <,
        OPERATOR        2       <=,
        OPERATOR        3       =,
        OPERATOR        4       >=,
        OPERATOR        5       >,
        FUNCTION        1       btint8cmp(int8, int8);

-- Smallint operator class
CREATE OPERATOR CLASS int2_myam_ops
    DEFAULT FOR TYPE int2 USING myam AS
        OPERATOR        1       <,
        OPERATOR        2       <=,
        OPERATOR        3       =,
        OPERATOR        4       >=,
        OPERATOR        5       >,
        FUNCTION        1       btint2cmp(int2, int2);

-- stats function
CREATE FUNCTION myam_cache_stats(oid)
RETURNS TABLE(hits bigint, misses bigint, size int, capacity int)
AS 'MODULE_PATHNAME', 'myam_cache_stats'
LANGUAGE C STRICT;

CREATE FUNCTION myam_meta_tuple_count(oid)
RETURNS int4
AS 'MODULE_PATHNAME', 'myam_meta_tuple_count'
LANGUAGE C STRICT;

CREATE FUNCTION myam_bench(oid, int4, int4, int4)
RETURNS int4
AS 'MODULE_PATHNAME', 'myam_bench'
LANGUAGE C STRICT;

CREATE FUNCTION index_bench_stats(oid, int4, int4, int4)
RETURNS TABLE(index_type text, total_queries int4, avg_time_ms float8, min_time_ms float8, max_time_ms float8, stddev_ms float8)
AS 'MODULE_PATHNAME', 'index_bench_stats'
LANGUAGE C STRICT;

CREATE FUNCTION myam_btree_lookup(oid, int4)
RETURNS bool
AS 'MODULE_PATHNAME', 'myam_btree_lookup'
LANGUAGE C STRICT;
