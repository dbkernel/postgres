/* contrib/omnitype/omnitype--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION omnitype" to load this file. \quit

-- ==================== 前置条件检查 ====================

DO $$
DECLARE
    bloom_index_count INTEGER;
BEGIN
    -- 检查是否存在使用bloom访问方法的索引
    SELECT COUNT(*) INTO bloom_index_count
    FROM pg_extension WHERE extname = 'bloom';

    -- 如果不存在Bloom索引，抛出错误并退出
    IF bloom_index_count = 0 THEN
        RAISE EXCEPTION 'bloom extension not exist, please CREATE EXTENSION bloom;';
    END IF;

    -- 继续执行插件初始化的其他逻辑
    RAISE INFO 'bloom extension already exists, continue ...';

    -- 其他初始化代码

END $$;

-- ==================== 创建自定义操作符 ====================

\echo Use "Create @@ Operator"

-- 注册函数
-- 写法一：效率低
-- CREATE FUNCTION abs_diff(numeric, numeric) RETURNS numeric
--     AS 'SELECT abs($1 -$2);'
--     LANGUAGE SQL;
-- 写法二：效率高
CREATE FUNCTION abs_diff(numeric, numeric) RETURNS numeric AS $$
BEGIN
    RETURN abs($1 - $2);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- 创建一个名为 @@ 的操作符，用于计算两个数值的差的绝对值
CREATE OPERATOR @@ (
    LEFTARG = numeric,
    RIGHTARG = numeric,
    PROCEDURE = abs_diff,
    COMMUTATOR = @@
);

-- 删除自定义操作符（在 DROP EXTENSION 时会自动删除，无法手动删除，此处仅做记录）
-- DROP OPERATOR @@ (numeric, numeric);
-- DROP FUNCTION abs_diff(numeric, numeric);

-- ==================== 创建 tinyint（基于 int4、C 函数） ====================

CREATE TYPE tinyint;

-------------------- 创建基础函数 --------------------

CREATE FUNCTION tinyint_in(cstring)
    RETURNS tinyint
    AS 'omnitype', 'tinyint_in'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tinyint_out(tinyint)
    RETURNS cstring
    AS 'omnitype', 'tinyint_out'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tinyint_recv(internal)
    RETURNS tinyint
    AS 'omnitype', 'tinyint_recv'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tinyint_send(tinyint)
    RETURNS bytea
    AS 'omnitype', 'tinyint_send'
    LANGUAGE C IMMUTABLE STRICT;

-------------------- 定义完整类型 --------------------

CREATE TYPE tinyint (
    internallength = 4,
    input = tinyint_in,
    output = tinyint_out,
    receive = tinyint_recv,
    send = tinyint_send,
    alignment = int4,
    storage = plain,
    category = 'N',
    like = int4
);

-------------------- tinyint 与 integer 的比较函数（正向）、操作符（正向） --------------------

CREATE FUNCTION tinyint_lt_integer(tinyint, integer)
    RETURNS bool
    AS 'omnitype', 'tinyint_lt_integer'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tinyint_le_integer(tinyint, integer)
    RETURNS bool
    AS 'omnitype', 'tinyint_le_integer'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tinyint_eq_integer(tinyint, integer)
    RETURNS bool
    AS 'omnitype', 'tinyint_eq_integer'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tinyint_ne_integer(tinyint, integer)
    RETURNS bool
    AS 'omnitype', 'tinyint_ne_integer'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tinyint_ge_integer(tinyint, integer)
    RETURNS bool
    AS 'omnitype', 'tinyint_ge_integer'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tinyint_gt_integer(tinyint, integer)
    RETURNS bool
    AS 'omnitype', 'tinyint_gt_integer'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR < (
    LEFTARG = tinyint,
    -- RIGHTARG = tinyint, -- 适用于 c1 < '100'::tinyint
    RIGHTARG = integer, -- 适用于 c1 < 100，因为 100 默认被解析成 integer
    PROCEDURE = tinyint_lt_integer,
    COMMUTATOR = >,
    NEGATOR = >=
);

CREATE OPERATOR <= (
    LEFTARG = tinyint,
    -- RIGHTARG = tinyint, -- 适用于 c1 <= '100'::tinyint
    RIGHTARG = integer, -- 适用于 c1 <= 100，因为 100 默认被解析成 integer
    PROCEDURE = tinyint_le_integer,
    COMMUTATOR = >=,
    NEGATOR = >
);

CREATE OPERATOR = (
    LEFTARG = tinyint,
    -- RIGHTARG = tinyint, -- 适用于 c1 = '100'::tinyint
    RIGHTARG = integer, -- 适用于 c1 = 100，因为 100 默认被解析成 integer
    PROCEDURE = tinyint_eq_integer,
    COMMUTATOR = =,
    NEGATOR = <>,
    HASHES,  -- 支持哈希索引
    MERGES   -- 支持合并连接
);

CREATE OPERATOR <> (
    LEFTARG = tinyint,
    -- RIGHTARG = tinyint, -- 适用于 c1 <> '100'::tinyint
    RIGHTARG = integer, -- 适用于 c1 <> 100，因为 100 默认被解析成 integer
    PROCEDURE = tinyint_ne_integer,
    COMMUTATOR = <>,
    NEGATOR = =
);

CREATE OPERATOR >= (
    LEFTARG = tinyint,
    -- RIGHTARG = tinyint, -- 适用于 c1 >= '100'::tinyint
    RIGHTARG = integer, -- 适用于 c1 >= 100，因为 100 默认被解析成 integer
    PROCEDURE = tinyint_ge_integer,
    COMMUTATOR = <=,
    NEGATOR = <
);

CREATE OPERATOR > (
    LEFTARG = tinyint,
    -- RIGHTARG = tinyint, -- 适用于 c1 > '100'::tinyint
    RIGHTARG = integer, -- 适用于 c1 > 100，因为 100 默认被解析成 integer
    PROCEDURE = tinyint_gt_integer,
    COMMUTATOR = <,
    NEGATOR = <=
);

CREATE FUNCTION tinyint_cmp_integer(tinyint, integer)
    RETURNS integer
    AS 'omnitype', 'tinyint_cmp_integer'
    LANGUAGE C IMMUTABLE STRICT;

-------------------- integer 与 tinyint 的比较函数（反向）、操作符（反向） --------------------

CREATE FUNCTION integer_lt_tinyint(integer, tinyint)
    RETURNS bool
    AS 'omnitype', 'integer_lt_tinyint'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION integer_le_tinyint(integer, tinyint)
    RETURNS bool
    AS 'omnitype', 'integer_le_tinyint'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION integer_eq_tinyint(integer, tinyint)
    RETURNS bool
    AS 'omnitype', 'integer_eq_tinyint'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION integer_ne_tinyint(integer, tinyint)
    RETURNS bool
    AS 'omnitype', 'integer_ne_tinyint'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION integer_ge_tinyint(integer, tinyint)
    RETURNS bool
    AS 'omnitype', 'integer_ge_tinyint'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION integer_gt_tinyint(integer, tinyint)
    RETURNS bool
    AS 'omnitype', 'integer_gt_tinyint'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR < (
    LEFTARG = integer, -- 适用于 100 < c1，因为 100 默认被解析成 integer
    RIGHTARG = tinyint,
    PROCEDURE = integer_lt_tinyint,
    COMMUTATOR = >,
    NEGATOR = >=
);

CREATE OPERATOR <= (
    LEFTARG = integer, -- 适用于 100 <= c1，因为 100 默认被解析成 integer
    RIGHTARG = tinyint,
    PROCEDURE = integer_le_tinyint,
    COMMUTATOR = >=,
    NEGATOR = >
);

CREATE OPERATOR = (
    LEFTARG = integer, -- 适用于 100 = c1，因为 100 默认被解析成 integer
    RIGHTARG = tinyint,
    PROCEDURE = integer_eq_tinyint,
    COMMUTATOR = =,
    NEGATOR = <>,
    HASHES,  -- 支持哈希索引
    MERGES   -- 支持合并连接
);

CREATE OPERATOR <> (
    LEFTARG = integer, -- 适用于 100 <> c1，因为 100 默认被解析成 integer
    RIGHTARG = tinyint,
    PROCEDURE = integer_ne_tinyint,
    COMMUTATOR = <>,
    NEGATOR = =
);

CREATE OPERATOR >= (
    LEFTARG = integer, -- 适用于 100 >= c1，因为 100 默认被解析成 integer
    RIGHTARG = tinyint,
    PROCEDURE = integer_ge_tinyint,
    COMMUTATOR = <=,
    NEGATOR = <
);

CREATE OPERATOR > (
    LEFTARG = integer, -- 适用于 100 > c1，因为 100 默认被解析成 integer
    RIGHTARG = tinyint,
    PROCEDURE = integer_gt_tinyint,
    COMMUTATOR = <,
    NEGATOR = <=
);

CREATE FUNCTION integer_cmp_tinyint(integer, tinyint)
    RETURNS integer
    AS 'omnitype', 'integer_cmp_tinyint'
    LANGUAGE C IMMUTABLE STRICT;

-------------------- tinyint 与 tinyint 的比较函数、操作符 --------------------

CREATE FUNCTION tinyint_lt(tinyint, tinyint)
    RETURNS bool
    AS 'omnitype', 'tinyint_lt'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tinyint_le(tinyint, tinyint)
    RETURNS bool
    AS 'omnitype', 'tinyint_le'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tinyint_eq(tinyint, tinyint)
    RETURNS bool
    AS 'omnitype', 'tinyint_eq'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tinyint_ne(tinyint, tinyint)
    RETURNS bool
    AS 'omnitype', 'tinyint_ne'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tinyint_ge(tinyint, tinyint)
    RETURNS bool
    AS 'omnitype', 'tinyint_ge'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tinyint_gt(tinyint, tinyint)
    RETURNS bool
    AS 'omnitype', 'tinyint_gt'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR < (
    LEFTARG = tinyint,
    RIGHTARG = tinyint,
    PROCEDURE = tinyint_lt,
    COMMUTATOR = >,
    NEGATOR = >=
);

CREATE OPERATOR <= (
    LEFTARG = tinyint,
    RIGHTARG = tinyint,
    PROCEDURE = tinyint_le,
    COMMUTATOR = >=,
    NEGATOR = >
);

CREATE OPERATOR = (
    LEFTARG = tinyint,
    RIGHTARG = tinyint,
    PROCEDURE = tinyint_eq,
    COMMUTATOR = =,
    NEGATOR = <>,
    HASHES,  -- 支持哈希索引
    MERGES   -- 支持合并连接
);

CREATE OPERATOR <> (
    LEFTARG = tinyint,
    RIGHTARG = tinyint,
    PROCEDURE = tinyint_ne,
    COMMUTATOR = <>,
    NEGATOR = =
);

CREATE OPERATOR >= (
    LEFTARG = tinyint,
    RIGHTARG = tinyint,
    PROCEDURE = tinyint_ge,
    COMMUTATOR = <=,
    NEGATOR = <
);

CREATE OPERATOR > (
    LEFTARG = tinyint,
    RIGHTARG = tinyint,
    PROCEDURE = tinyint_gt,
    COMMUTATOR = <,
    NEGATOR = <=
);

CREATE FUNCTION tinyint_cmp(tinyint, tinyint)
    RETURNS integer
    AS 'omnitype', 'tinyint_cmp'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tinyint_hash(tinyint)
    RETURNS integer
    AS 'omnitype', 'tinyint_hash'
    LANGUAGE C IMMUTABLE STRICT;

-------------------- 操作符类 --------------------

-- BTree 操作符类
CREATE OPERATOR CLASS tinyint_btree_ops
    DEFAULT FOR TYPE tinyint USING btree AS
        -- 同类型操作符
        OPERATOR 1 < ,
        OPERATOR 2 <= ,
        OPERATOR 3 = ,
        OPERATOR 4 >= ,
        OPERATOR 5 > ,

        -- 跨类型操作符 (tinyint vs integer)，正向
        OPERATOR 1 < (tinyint, integer),
        OPERATOR 2 <= (tinyint, integer),
        OPERATOR 3 = (tinyint, integer),
        OPERATOR 4 >= (tinyint, integer),
        OPERATOR 5 > (tinyint, integer),

        -- 跨类型操作符 (integer vs tinyint)，反向
        OPERATOR 1 < (integer, tinyint),
        OPERATOR 2 <= (integer, tinyint),
        OPERATOR 3 = (integer, tinyint),
        OPERATOR 4 >= (integer, tinyint), -- 现在已定义
        OPERATOR 5 > (integer, tinyint),

        -- 指定多个比较函数
        FUNCTION 1 tinyint_cmp(tinyint, tinyint),
        FUNCTION 1 tinyint_cmp_integer(tinyint, integer),
        FUNCTION 1 integer_cmp_tinyint(integer, tinyint);

-- Hash 操作符类
CREATE OPERATOR CLASS tinyint_hash_ops
    DEFAULT FOR TYPE tinyint USING hash AS
        OPERATOR 1 = ,
        FUNCTION 1 tinyint_hash(tinyint);

-------------------- 类型转换 --------------------

-- integer --> tinyint 转换（带范围检查）

CREATE FUNCTION int4_to_tinyint(integer)
    RETURNS tinyint
    AS 'omnitype', 'int4_to_tinyint'
    LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (integer AS tinyint)
    WITH FUNCTION int4_to_tinyint(integer)
    AS IMPLICIT; -- 必须是 IMPLICIT 转换

-- tinyint --> integer 转换（安全转换）

CREATE FUNCTION tinyint_to_int4(tinyint)
    RETURNS integer
    AS 'SELECT $1::integer;'
    LANGUAGE SQL IMMUTABLE STRICT;

CREATE CAST (tinyint AS integer)
    WITH FUNCTION tinyint_to_int4(tinyint)
    AS IMPLICIT; -- 必须是 IMPLICIT 转换

-- ==================== 创建 tinyfloat（基于 CHECK 约束） ====================

-- 创建自定义 tinyfloat 域类型（基于 real 类型）
CREATE DOMAIN tinyfloat AS real
CHECK (
    VALUE >= -10.0 AND
    VALUE <= 10.0
)
NOT NULL;  -- 根据需求决定是否允许 NULL

-- ==================== 创建自定义复数类型 complex、操作符及索引 ====================

\echo Use "Create Complex Data Type"

-- 注册 C 函数
CREATE FUNCTION complex_in(cstring) RETURNS complex
    AS 'omnitype', 'complex_in'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION complex_out(complex) RETURNS cstring
    AS 'omnitype', 'complex_out'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION complex_add(complex, complex) RETURNS complex
    AS 'omnitype', 'complex_add'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION complex_op_lt(complex, complex) RETURNS boolean
    AS 'omnitype', 'complex_op_lt'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION complex_op_le(complex, complex) RETURNS boolean
    AS 'omnitype', 'complex_op_le'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION complex_op_ge(complex, complex) RETURNS boolean
    AS 'omnitype', 'complex_op_ge'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION complex_op_eq(complex, complex) RETURNS boolean
    AS 'omnitype', 'complex_op_eq'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION complex_op_gt(complex, complex) RETURNS boolean
    AS 'omnitype', 'complex_op_gt'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION complex_cmp(complex, complex) RETURNS integer
    AS 'omnitype', 'complex_cmp'
    LANGUAGE C IMMUTABLE STRICT;

-- 定义类型 complex
CREATE TYPE complex (
    INPUT = complex_in,
    OUTPUT = complex_out,
    INTERNALLENGTH = 16,
    PASSEDBYVALUE = false
);

-- 创建复数类型的操作符

CREATE OPERATOR < (
    LEFTARG = complex,
    RIGHTARG = complex,
    PROCEDURE = complex_op_lt,
    COMMUTATOR = >, -- 指定了操作符的对称操作符
    NEGATOR = >=, -- 指定了操作符的相反（否定）操作符
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel
);

CREATE OPERATOR <= (
    LEFTARG = complex,
    RIGHTARG = complex,
    PROCEDURE = complex_op_le,
    COMMUTATOR = >=,
    NEGATOR = >,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel
);

CREATE OPERATOR = (
    LEFTARG = complex,
    RIGHTARG = complex,
    PROCEDURE = complex_op_eq,
    COMMUTATOR = =,
    NEGATOR = <>,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);

CREATE OPERATOR >= (
    LEFTARG = complex,
    RIGHTARG = complex,
    PROCEDURE = complex_op_ge,
    COMMUTATOR = <=,
    NEGATOR = <,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel
);

CREATE OPERATOR > (
    LEFTARG = complex,
    RIGHTARG = complex,
    PROCEDURE = complex_op_gt,
    COMMUTATOR = <,
    NEGATOR = <=,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);

-- 创建复数类型的操作符类
CREATE OPERATOR CLASS complex_ops FOR TYPE complex USING btree AS
    OPERATOR 1 <, -- 定义操作符，其对应的函数返回值必须是 bool 类型
    OPERATOR 2 <=,
    OPERATOR 3 =,
    OPERATOR 4 >=,
    OPERATOR 5 >,
    FUNCTION 1 complex_cmp(complex, complex); -- 定义比较函数，其返回值必须是 int 类型

-- 删除 complex 类型相关对象（在 DROP EXTENSION 时会自动删除，无法手动删除，此处仅做记录）
-- DROP OPERATOR < (complex, complex);
-- DROP OPERATOR <= (complex, complex);
-- DROP OPERATOR = (complex, complex);
-- DROP OPERATOR >= (complex, complex);
-- DROP OPERATOR > (complex, complex);
-- DROP OPERATOR CLASS complex_ops USING btree;
-- DROP FUNCTION complex_in(cstring);
-- DROP FUNCTION complex_out(complex);
-- DROP FUNCTION complex_add(complex, complex);
-- DROP FUNCTION complex_op_lt(complex, complex);
-- DROP FUNCTION complex_op_le(complex, complex);
-- DROP FUNCTION complex_op_ge(complex, complex);
-- DROP FUNCTION complex_op_eq(complex, complex);
-- DROP FUNCTION complex_op_gt(complex, complex);
-- DROP FUNCTION complex_cmp(complex, complex);
-- DROP TYPE complex;

-- ==================== 创建自定义枚举类型 mood ====================

-- 枚举类型 mood
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');

-- ==================== 创建自定义字符串类型 mytext、操作符及索引 ====================

\echo Use "Create Mytext Data Type"

-------------------- in/out 函数 --------------------

CREATE FUNCTION mytext_in(cstring) RETURNS mytext
    AS 'omnitype', 'mytext_in'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION mytext_out(mytext) RETURNS cstring
    AS 'omnitype', 'mytext_out'
    LANGUAGE C IMMUTABLE STRICT;

-------------------- 创建 mytext 类型 --------------------

-- extended 支持 TOAST 存储可能导致失效, plain支持varchar, COLLATABLE = true 设置排序规则为默认排序规则
CREATE TYPE mytext (
    INPUT = mytext_in,
    OUTPUT = mytext_out,
    INTERNALLENGTH = VARIABLE,
    STORAGE = plain,
    COLLATABLE = true
);

-------------------- 注册 BTree 索引函数、操作符、操作符类 --------------------

-- 注册 mytext_cmp 函数
CREATE FUNCTION mytext_cmp(mytext, mytext) RETURNS integer
    AS 'omnitype', 'mytext_cmp'
    LANGUAGE C STRICT;

-- 注册操作符所用函数，返回值必须为 bool 类型
CREATE FUNCTION mytext_op_le(mytext, mytext) RETURNS boolean
    AS 'omnitype', 'mytext_op_le'
    LANGUAGE C STRICT;

CREATE FUNCTION mytext_op_lt(mytext, mytext) RETURNS boolean
    AS 'omnitype', 'mytext_op_lt'
    LANGUAGE C STRICT;

CREATE FUNCTION mytext_op_eq(mytext, mytext) RETURNS boolean
    AS 'omnitype', 'mytext_op_eq'
    LANGUAGE C STRICT;

CREATE FUNCTION mytext_op_gt(mytext, mytext) RETURNS boolean
    AS 'omnitype', 'mytext_op_gt'
    LANGUAGE C STRICT;

CREATE FUNCTION mytext_op_ge(mytext, mytext) RETURNS boolean
    AS 'omnitype', 'mytext_op_ge'
    LANGUAGE C STRICT;

-- 创建操作符

CREATE OPERATOR < (
    LEFTARG = mytext,
    RIGHTARG = mytext,
    PROCEDURE = mytext_op_lt,
    COMMUTATOR = >,
    NEGATOR = >=,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);

CREATE OPERATOR <= (
    LEFTARG = mytext,
    RIGHTARG = mytext,
    PROCEDURE = mytext_op_le,
    COMMUTATOR = >=,
    NEGATOR = >,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel
);

CREATE OPERATOR = (
    LEFTARG = mytext,
    RIGHTARG = mytext,
    PROCEDURE = mytext_op_eq,
    COMMUTATOR = =,
    NEGATOR = <>,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);

CREATE OPERATOR >= (
    LEFTARG = mytext,
    RIGHTARG = mytext,
    PROCEDURE = mytext_op_ge,
    COMMUTATOR = <=,
    NEGATOR = <,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel
);

CREATE OPERATOR > (
    LEFTARG = mytext,
    RIGHTARG = mytext,
    PROCEDURE = mytext_op_gt,
    COMMUTATOR = <,
    NEGATOR = <=,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);

-- 创建操作符类
CREATE OPERATOR CLASS mytext_btree_ops DEFAULT FOR TYPE mytext USING btree AS
    OPERATOR 1 < (mytext, mytext), -- 操作符与序号严格对应，比如 1 只能关联 < 操作符
    OPERATOR 2 <= (mytext, mytext), -- 定义操作符，其对应的函数返回值必须是 bool 类型
    OPERATOR 3 = (mytext, mytext),
    OPERATOR 4 >= (mytext, mytext),
    OPERATOR 5 > (mytext, mytext),
    FUNCTION 1 mytext_cmp(mytext, mytext); -- 定义比较函数，其返回值必须是 int 类型

-- 删除 mytext 类型相关对象（在 DROP EXTENSION 时会自动删除，无法手动删除，此处仅做记录）
-- DROP OPERATOR < (mytext, mytext);
-- DROP OPERATOR <= (mytext, mytext);
-- DROP OPERATOR = (mytext, mytext);
-- DROP OPERATOR >= (mytext, mytext);
-- DROP OPERATOR > (mytext, mytext);
-- DROP OPERATOR CLASS mytext_ops USING btree;
-- DROP FUNCTION mytext_in(cstring);
-- DROP FUNCTION mytext_out(mytext);
-- DROP FUNCTION mytext_cmp(mytext, mytext);
-- DROP FUNCTION mytext_op_le(mytext, mytext);
-- DROP FUNCTION mytext_op_lt(mytext, mytext);
-- DROP FUNCTION mytext_op_eq(mytext, mytext);
-- DROP FUNCTION mytext_op_gt(mytext, mytext);
-- DROP FUNCTION mytext_op_ge(mytext, mytext);
-- DROP TYPE mytext;

-------------------- 注册 Hash 索引函数、操作符、操作符类 --------------------

-- 使用单个哈希函数将键值映射到哈希表中，仅支持等值查询（=）

-- 注册 hash 函数
CREATE FUNCTION mytext_hash(mytext) RETURNS integer
AS 'omnitype', 'mytext_hash'
LANGUAGE C IMMUTABLE STRICT;

-- 创建操作符类
CREATE OPERATOR CLASS mytext_hash_ops
    DEFAULT FOR TYPE mytext USING hash AS
    OPERATOR 1 = (mytext, mytext),
    FUNCTION 1 mytext_hash(mytext);

-------------------- 注册 Bloom 索引函数、操作符、操作符类 --------------------

-- 支持将键值映射到多个位（bit）位置，支持多键等值查询（如 WHERE a=? AND b=?）
-- Bloom 索引的哈希函数是内置实现且对用户透明的，它们基于高效的通用哈希算法，无需用户干预。
-- PostgreSQL 使用种子偏移技术生成多个哈希函数，而非独立实现多个函数：
-- （1）基础哈希算法：通常使用 MurmurHash 或类似算法（具体实现可能因版本而异）
-- （2）多函数生成：通过同一个基础哈希函数，配合不同的种子值（seed）生成多个哈希结果，以减少误判。哈希函数越多，误判率越低。
-- （3）数学公式：hash_i(key) = base_hash(key, seed_i) ，其中 seed_i 是第 i 个哈希函数的种子值

-- Bloom 索引的限制：
-- 1. 该模块仅包含 int4 和 text 的操作符类。
-- 2. 搜索仅支持 = 操作符。但是将来有可能添加对具有并集和交集操作的数组的支持。
-- 3. bloom 访问方法不支持 UNIQUE 索引。
-- 4. bloom 访问方法不支持搜索 NULL 值。

-- 参考 contrib/bloom/bloom--1.0.sql
CREATE OPERATOR CLASS mytext_bloom_ops
    DEFAULT FOR TYPE mytext USING bloom AS
    OPERATOR 1 = (mytext, mytext),  -- 等值操作符
    FUNCTION 1 mytext_hash(mytext); -- 自定义哈希函数

-------------------- 注册 BRIN 索引函数、操作符、操作符类 --------------------

-- 创建 BRIN 索引所需函数

-- 初始化
CREATE FUNCTION mytext_brin_minmax_opcinfo(internal)
RETURNS internal
AS 'omnitype', 'mytext_brin_minmax_opcinfo'
LANGUAGE C IMMUTABLE STRICT;

-- 添加值到摘要
CREATE FUNCTION mytext_brin_minmax_add_value(internal, internal, internal, internal)
RETURNS boolean
AS 'omnitype', 'mytext_brin_minmax_add_value'
LANGUAGE C IMMUTABLE STRICT;

-- 一致性检查
CREATE FUNCTION mytext_brin_minmax_consistent(internal, internal, internal)
RETURNS boolean
AS 'omnitype', 'mytext_brin_minmax_consistent'
LANGUAGE C IMMUTABLE STRICT;

-- 合并多个数据块的摘要
CREATE FUNCTION mytext_brin_minmax_union(internal, internal, internal)
RETURNS boolean
AS 'omnitype', 'mytext_brin_minmax_union'
LANGUAGE C IMMUTABLE STRICT;

-- 处理索引创建选项（可选）
CREATE FUNCTION mytext_brin_minmax_options(internal)
RETURNS void
AS 'omnitype', 'mytext_brin_minmax_options'
LANGUAGE C IMMUTABLE STRICT;

-- 在创建操作符类之前添加操作符类
-- CREATE OPERATOR FAMILY mytext_brin_ops_family USING brin;

-- 创建 BRIN 操作符类
CREATE OPERATOR CLASS mytext_brin_ops
    DEFAULT FOR TYPE mytext USING brin AS
    -- 声明比较操作符（策略编号1-5）
    OPERATOR 1 < (mytext, mytext),    -- 策略1：小于
    OPERATOR 2 <= (mytext, mytext),   -- 策略2：小于等于
    OPERATOR 3 = (mytext, mytext),    -- 策略3：等于
    OPERATOR 4 >= (mytext, mytext),   -- 策略4：大于等于
    OPERATOR 5 > (mytext, mytext),    -- 策略5：大于
    -- 声明支持函数（编号1-4）
    FUNCTION 1 mytext_brin_minmax_opcinfo(internal), -- 初始化
    FUNCTION 2 mytext_brin_minmax_add_value(internal, internal, internal, internal), -- 添加值到摘要
    FUNCTION 3 mytext_brin_minmax_consistent(internal, internal, internal), -- 一致性检查
    FUNCTION 4 mytext_brin_minmax_union(internal, internal, internal); -- 合并多个数据块的摘要

    -- FUNCTION 5 mytext_brin_minmax_options(internal); -- 处理索引创建选项（可选）

    -- Deep Seek 认为BRIN索引操作符类支持指定代价估算函数，但在 pg_proc.dat 中并无示例：
    -- Deep Seek 认为的示例：mytext_brin_minmax_penalty(internal, internal, internal), -- 计算索引扫描代价（可选）

    -- BRIN 索引的 MinMax 操作符类、Bloom 操作符类，对应函数编号为 1-5, 11
    -- 从 pg_proc.dat 中 BRIN 索引相关函数定义（minmax oid）来看：
    -- 1    brin_bloom_opcinfo
    -- 2    brin_bloom_add_value
    -- 3    brin_bloom_consistent
    -- 4    brin_bloom_union
    -- 5    brin_bloom_options
    -- 11   hashvarlena

-- ==================== 创建自定义复合类型 composite、操作符及索引 ====================

CREATE FUNCTION composite_in(cstring)
RETURNS composite
AS 'omnitype', 'composite_in'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION composite_out(composite)
RETURNS cstring
AS 'omnitype', 'composite_out'
LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE composite (
    INPUT = composite_in,
    OUTPUT = composite_out,
    INTERNALLENGTH = VARIABLE, -- 变长类型
    STORAGE = EXTENDED,
    COLLATABLE = true -- 声明支持排序规则
);

-- 注册 composite_cmp 函数
CREATE FUNCTION composite_cmp(composite, composite) RETURNS integer
    AS 'omnitype', 'composite_cmp'
    LANGUAGE C STRICT;

-- BTree 操作符函数

-- 以下两种方法二选一：

-- 方式一：基于 composite_cmp 函数

-- 小于 <
CREATE OR REPLACE FUNCTION composite_lt(Composite, Composite)
RETURNS boolean AS $$
BEGIN
    RETURN composite_cmp($1, $2) < 0;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- 小于等于 <=
CREATE OR REPLACE FUNCTION composite_le(Composite, Composite)
RETURNS boolean AS $$
BEGIN
    RETURN composite_cmp($1, $2) <= 0;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- 等于 =
CREATE OR REPLACE FUNCTION composite_eq(Composite, Composite)
RETURNS boolean AS $$
BEGIN
    RETURN composite_cmp($1, $2) = 0;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- 大于等于 >=
CREATE OR REPLACE FUNCTION composite_ge(Composite, Composite)
RETURNS boolean AS $$
BEGIN
    RETURN composite_cmp($1, $2) >= 0;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- 大于 >
CREATE OR REPLACE FUNCTION composite_gt(Composite, Composite)
RETURNS boolean AS $$
BEGIN
    RETURN composite_cmp($1, $2) > 0;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- 方式二：基于同名C函数

-- -- 小于 <
-- CREATE FUNCTION composite_lt(composite, composite) RETURNS boolean
--     AS 'omnitype', 'composite_lt'
--     LANGUAGE C STRICT;

-- -- 小于等于 <=
-- CREATE FUNCTION composite_le(composite, composite) RETURNS boolean
--     AS 'omnitype', 'composite_le'
--     LANGUAGE C STRICT;

-- -- 等于 =
-- CREATE FUNCTION composite_eq(composite, composite) RETURNS boolean
--     AS 'omnitype', 'composite_eq'
--     LANGUAGE C STRICT;

-- -- 大于等于 >=
-- CREATE FUNCTION composite_ge(composite, composite) RETURNS boolean
--     AS 'omnitype', 'composite_ge'
--     LANGUAGE C STRICT;

-- -- 大于 >
-- CREATE FUNCTION composite_gt(composite, composite) RETURNS boolean
--     AS 'omnitype', 'composite_gt'
--     LANGUAGE C STRICT;

-- BTree 比较操作符
-- 小于 <
CREATE OPERATOR < (
    leftarg = Composite,
    rightarg = Composite,
    procedure = composite_lt,
    commutator = >,    -- 交换子：a < b ⇨ b > a
    negator = >=        -- 否定子：!(a < b) ⇨ a >= b
);

-- 小于等于 <=
CREATE OPERATOR <= (
    leftarg = Composite,
    rightarg = Composite,
    procedure = composite_le,
    commutator = >=,    -- 交换子：a <= b ⇨ b >= a
    negator = >         -- 否定子：!(a <= b) ⇨ a > b
);

-- 等于 =
CREATE OPERATOR = (
    leftarg = Composite,
    rightarg = Composite,
    procedure = composite_eq,
    commutator = =      -- 交换子：a = b ⇨ b = a
);

-- 大于等于 >=
CREATE OPERATOR >= (
    leftarg = Composite,
    rightarg = Composite,
    procedure = composite_ge,
    commutator = <=,    -- 交换子：a >= b ⇨ b <= a
    negator = <         -- 否定子：!(a >= b) ⇨ a < b
);

-- 大于 >
CREATE OPERATOR > (
    leftarg = Composite,
    rightarg = Composite,
    procedure = composite_gt,
    commutator = <,     -- 交换子：a > b ⇨ b < a
    negator = <=        -- 否定子：!(a > b) ⇨ a <= b
);

-- BTree 操作符类
CREATE OPERATOR CLASS composite_btree_ops
    DEFAULT FOR TYPE composite USING btree AS
        OPERATOR 1 <  (composite, composite),
        OPERATOR 2 <= (composite, composite),
        OPERATOR 3 =  (composite, composite),
        OPERATOR 4 >= (composite, composite),
        OPERATOR 5 >  (composite, composite),
        FUNCTION 1 composite_cmp(composite, composite);
