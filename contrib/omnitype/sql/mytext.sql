-- ====================== mytext 类型元信息验证 ======================

-- 验证操作符类是否创建成功（注意替换 mytext_ops 和 btree）
SELECT opcname AS opclass_name,
       amname AS index_method,
       opcintype::regtype AS data_type,
       opcfamily::regclass AS opfamily
FROM pg_opclass
JOIN pg_am ON pg_am.oid = opcmethod
WHERE opcname = 'mytext_btree_ops'
  AND amname = 'btree';

-- 预期输出：
--    opclass_name   | index_method | data_type | opfamily
-- ------------------+--------------+-----------+----------
--  mytext_btree_ops | btree        | mytext    | 16561
-- (1 row)

-- 查看操作符类绑定的操作符及策略号（策略号1-5对应B-tree标准顺序）
SELECT amopstrategy AS strategy,
       amopopr::regoperator AS operator,
       amoplefttype::regtype AS left_type,
       amoprighttype::regtype AS right_type
FROM pg_amop
WHERE amopfamily = (SELECT opcfamily FROM pg_opclass WHERE opcname = 'mytext_btree_ops')
ORDER BY amopstrategy;

-- 预期输出：
--  strategy |     operator      | left_type | right_type
-- ----------+-------------------+-----------+------------
--         1 | <(mytext,mytext)  | mytext    | mytext
--         2 | <=(mytext,mytext) | mytext    | mytext
--         3 | =(mytext,mytext)  | mytext    | mytext
--         4 | >=(mytext,mytext) | mytext    | mytext
--         5 | >(mytext,mytext)  | mytext    | mytext
-- (5 rows)

-- 查看操作符类关联的比较函数
SELECT amprocnum AS func_num,
       amproc::regprocedure AS function,
       amproclefttype::regtype AS left_type,
       amprocrighttype::regtype AS right_type
FROM pg_amproc
WHERE amprocfamily = (SELECT opcfamily FROM pg_opclass WHERE opcname = 'mytext_btree_ops');

-- 预期输出：
--  func_num |         function          | left_type | right_type
-- ----------+---------------------------+-----------+------------
--         1 | mytext_cmp(mytext,mytext) | mytext    | mytext
-- (1 row)

-- 查看自定义操作符的详细定义
SELECT oprname AS operator,
       oprleft::regtype AS left_type,
       oprright::regtype AS right_type,
       oprcode::regprocedure AS function,
       oprcom::regoperator AS commutator,
       oprnegate::regoperator AS negator
FROM pg_operator
WHERE oprname IN ('<', '<=', '>', '>=', '=')
  AND oprleft = 'mytext'::regtype
  AND oprright = 'mytext'::regtype;

-- 预期输出片段：
--  operator | left_type | right_type |          function           |    commutator     |      negator
-- ----------+-----------+------------+-----------------------------+-------------------+-------------------
--  <        | mytext    | mytext     | mytext_op_lt(mytext,mytext) | >(mytext,mytext)  | >=(mytext,mytext)
--  <=       | mytext    | mytext     | mytext_op_le(mytext,mytext) | >=(mytext,mytext) | >(mytext,mytext)
--  =        | mytext    | mytext     | mytext_op_eq(mytext,mytext) | =(mytext,mytext)  | <>(mytext,mytext)
--  >        | mytext    | mytext     | mytext_op_gt(mytext,mytext) | <(mytext,mytext)  | <=(mytext,mytext)
--  >=       | mytext    | mytext     | mytext_op_ge(mytext,mytext) | <=(mytext,mytext) | <(mytext,mytext)
-- (5 rows)


-- ==================== 测试自定义类型 mytext ====================

---------------- 准备测试数据 ----------------

CREATE TABLE test_mytext (
    id SERIAL PRIMARY KEY,
    value mytext,
    value2 mytext,
    time timestamptz
);

INSERT INTO test_mytext (value, value2, time) VALUES
    ('hello', 'a001', '2025-05-21 18:00:00'),
    ('world', 'w100', '2025-05-01 12:12:00'),
    ('testing', 'a088', '2023-04-01 12:12:00'),
    ('hello', 't800', '2023-04-03 12:13:00'),
    ('example', 'b02', '2024-05-01 11:30:30'),
    ('postgresql', 't033', '2025-04-01 22:33:44'),
    ('mysql', 't800', '2021-06-01 10:50:40');

SELECT id, value, value2, time FROM test_mytext ORDER BY value;

---------------- 验证操作符的比较 ----------------

-- 只验证操作符
DO $$
DECLARE
    text1 mytext := 'hello';
    text2 mytext := 'world';
BEGIN
    IF text1 < text2 THEN
        RAISE NOTICE '% is less than %', text1, text2;
    ELSE
        RAISE NOTICE '% is not less than %', text1, text2;
    END IF;

    IF text1 = text2 THEN
        RAISE NOTICE '% is equal to %', text1, text2;
    ELSE
        RAISE NOTICE '% is not equal to %', text1, text2;
    END IF;
END $$;

---------------- 验证操作符比较表数据（无索引） ----------------

-- 使用自定义操作符进行查询（无索引）
SELECT * FROM test_mytext WHERE value > 'hello'; -- 测试大于操作符
SELECT * FROM test_mytext WHERE value <= 'world'; -- 测试小于等于操作符
SELECT * FROM test_mytext WHERE value = 'example'; -- 测试等于操作符
SELECT * FROM test_mytext WHERE value >= 'testing'; -- 测试大于等于操作符
SELECT * FROM test_mytext WHERE value < 'postgresql'; -- 测试小于操作符
-- 期望输出的过滤条件为：Filter: (test_mytext.value > 'hello'::mytext)
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT * FROM test_mytext WHERE value > 'hello'; -- 测试大于操作符
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT * FROM test_mytext WHERE value <= 'world'; -- 测试小于等于操作符
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT * FROM test_mytext WHERE value = 'example'; -- 测试等于操作符
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT * FROM test_mytext WHERE value >= 'testing'; -- 测试大于等于操作符
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT * FROM test_mytext WHERE value < 'postgresql'; -- 测试小于操作符

---------------- 验证操作符（BTree 索引） ----------------

CREATE INDEX mytext_value_btree_idx ON test_mytext USING btree (value mytext_btree_ops);

-- 验证 BTree 索引是否使用正确的操作符类
WITH index_info AS (
    SELECT idx.relname AS index_name, pg_get_indexdef(idx.oid) AS index_definition,
        unnest(i.indkey) AS attnum, unnest(i.indclass) AS opclass_oid
    FROM pg_index i JOIN pg_class idx ON i.indexrelid = idx.oid
    WHERE i.indrelid = 'test_mytext'::regclass),
column_info AS (
    SELECT att.attnum, att.attname AS column_name, format_type(att.atttypid, att.atttypmod) AS data_type, opc.opcname AS default_opclass
    FROM pg_attribute att
    JOIN pg_type typ ON att.atttypid = typ.oid
    LEFT JOIN pg_opclass opc ON opc.opcintype = att.atttypid AND opc.opcdefault AND opc.opcfamily IN (
        SELECT opf.oid FROM pg_opfamily opf JOIN pg_am am ON opf.opfmethod = am.oid WHERE am.amname = 'btree')
    WHERE att.attrelid = 'test_mytext'::regclass AND att.attnum > 0)
SELECT idx.index_name, idx.index_definition, col.column_name, col.data_type, col.default_opclass, opc.opcname AS index_opclass
FROM index_info idx
JOIN column_info col ON idx.attnum = col.attnum
LEFT JOIN pg_opclass opc ON idx.opclass_oid = opc.oid;

-- 期望输出：
--        index_name       |                               index_definition                                | column_name | data_type | default_opclass  |  index_opclass
-- ------------------------+-------------------------------------------------------------------------------+-------------+-----------+------------------+------------------
--  test_mytext_pkey       | CREATE UNIQUE INDEX test_mytext_pkey ON public.test_mytext USING btree (id)   | id          | integer   | int4_ops         | int4_ops
--  mytext_value_btree_idx | CREATE INDEX mytext_value_btree_idx ON public.test_mytext USING btree (value) | value       | mytext    | mytext_btree_ops | mytext_btree_ops
-- (2 rows)

-- 使用自定义操作符进行查询（BTree索引）
SET enable_seqscan = off;
SELECT * FROM test_mytext WHERE value > 'hello'; -- 测试大于操作符
SELECT * FROM test_mytext WHERE value <= 'world'; -- 测试小于等于操作符
SELECT * FROM test_mytext WHERE value = 'example'; -- 测试等于操作符
SELECT * FROM test_mytext WHERE value >= 'testing'; -- 测试大于等于操作符
SELECT * FROM test_mytext WHERE value < 'postgresql'; -- 测试小于操作符
-- 期望输出的过滤条件为：Index Cond: (test_mytext.value > 'hello'::mytext)
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT * FROM test_mytext WHERE value > 'hello'; -- 测试大于操作符
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT * FROM test_mytext WHERE value <= 'world'; -- 测试小于等于操作符
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT * FROM test_mytext WHERE value = 'example'; -- 测试等于操作符
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT * FROM test_mytext WHERE value >= 'testing'; -- 测试大于等于操作符
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT * FROM test_mytext WHERE value < 'postgresql'; -- 测试小于操作符
SET enable_seqscan = on;

-- 清理 btree 索引
DROP INDEX mytext_value_btree_idx;

---------------- 验证操作符（Hash 索引） ----------------

CREATE INDEX mytext_value_hash_idx ON test_mytext USING hash (value mytext_hash_ops);

-- 验证 Hash 索引是否使用正确的操作符类
WITH index_info AS (
    SELECT idx.relname AS index_name, pg_get_indexdef(idx.oid) AS index_definition,
        unnest(i.indkey) AS attnum, unnest(i.indclass) AS opclass_oid
    FROM pg_index i JOIN pg_class idx ON i.indexrelid = idx.oid
    WHERE i.indrelid = 'test_mytext'::regclass),
column_info AS (
    SELECT att.attnum, att.attname AS column_name, format_type(att.atttypid, att.atttypmod) AS data_type, opc.opcname AS default_opclass
    FROM pg_attribute att
    JOIN pg_type typ ON att.atttypid = typ.oid
    LEFT JOIN pg_opclass opc ON opc.opcintype = att.atttypid AND opc.opcdefault AND opc.opcfamily IN (
        SELECT opf.oid FROM pg_opfamily opf JOIN pg_am am ON opf.opfmethod = am.oid WHERE am.amname = 'hash')
    WHERE att.attrelid = 'test_mytext'::regclass AND att.attnum > 0)
SELECT idx.index_name, idx.index_definition, col.column_name, col.data_type, col.default_opclass, opc.opcname AS index_opclass
FROM index_info idx
JOIN column_info col ON idx.attnum = col.attnum
LEFT JOIN pg_opclass opc ON idx.opclass_oid = opc.oid;
-- 期望输出
--       index_name       |                              index_definition                               | column_name | data_type | default_opclass |  index_opclass
-- -----------------------+-----------------------------------------------------------------------------+-------------+-----------+-----------------+-----------------
--  test_mytext_pkey      | CREATE UNIQUE INDEX test_mytext_pkey ON public.test_mytext USING btree (id) | id          | integer   | int4_ops        | int4_ops
--  mytext_value_hash_idx | CREATE INDEX mytext_value_hash_idx ON public.test_mytext USING hash (value) | value       | mytext    | mytext_hash_ops | mytext_hash_ops
-- (2 rows)

-- 使用自定义操作符进行查询（Hash 索引）
SET enable_seqscan = off;
SELECT * FROM test_mytext WHERE value = 'example'; -- 测试等于操作符
-- 期望输出：Index Scan using mytext_value_hash_idx 以及 Index Cond: (test_mytext.value = 'example'::mytext)
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT * FROM test_mytext WHERE value = 'example'; -- 测试等于操作符
SET enable_seqscan = on;

DROP INDEX mytext_value_hash_idx;

---------------- 验证操作符（Bloom 索引） ----------------

---------------- 单 hash 函数索引 ----------------

CREATE INDEX mytext_value_bloom_idx ON test_mytext USING bloom (value mytext_bloom_ops);

-- 验证 Bloom 索引是否使用正确的操作符类
WITH index_info AS (
    SELECT idx.relname AS index_name, pg_get_indexdef(idx.oid) AS index_definition,
        unnest(i.indkey) AS attnum, unnest(i.indclass) AS opclass_oid
    FROM pg_index i JOIN pg_class idx ON i.indexrelid = idx.oid
    WHERE i.indrelid = 'test_mytext'::regclass),
column_info AS (
    SELECT att.attnum, att.attname AS column_name, format_type(att.atttypid, att.atttypmod) AS data_type, opc.opcname AS default_opclass
    FROM pg_attribute att
    JOIN pg_type typ ON att.atttypid = typ.oid
    LEFT JOIN pg_opclass opc ON opc.opcintype = att.atttypid AND opc.opcdefault AND opc.opcfamily IN (
        SELECT opf.oid FROM pg_opfamily opf JOIN pg_am am ON opf.opfmethod = am.oid WHERE am.amname = 'bloom')
    WHERE att.attrelid = 'test_mytext'::regclass AND att.attnum > 0)
SELECT idx.index_name, idx.index_definition, col.column_name, col.data_type, col.default_opclass, opc.opcname AS index_opclass
FROM index_info idx
JOIN column_info col ON idx.attnum = col.attnum
LEFT JOIN pg_opclass opc ON idx.opclass_oid = opc.oid;
-- 期望输出
--        index_name       |                               index_definition                                | column_name | data_type | default_opclass  |  index_opclass
-- ------------------------+-------------------------------------------------------------------------------+-------------+-----------+------------------+------------------
--  test_mytext_pkey       | CREATE UNIQUE INDEX test_mytext_pkey ON public.test_mytext USING btree (id)   | id          | integer   | int4_ops         | int4_ops
--  mytext_value_bloom_idx | CREATE INDEX mytext_value_bloom_idx ON public.test_mytext USING bloom (value) | value       | mytext    | mytext_bloom_ops | mytext_bloom_ops
-- (2 rows)

SET enable_seqscan = off;

SELECT * FROM test_mytext WHERE value = 'example'; -- 测试等于操作符

-- 期望结果包含：
-- Bitmap Heap Scan on public.test_mytext
-- Recheck Cond: (test_mytext.value = 'example'::mytext)
-- Bitmap Index Scan on mytext_value_bloom_idx
-- Index Cond: (test_mytext.value = 'example'::mytext)
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT * FROM test_mytext WHERE value = 'example'; -- 测试等于操作符

SET enable_seqscan = on;

DROP INDEX mytext_value_bloom_idx;

---------------- 多 hash 函数索引 ----------------

-- 创建的索引的签名长度总位数为 80 位，即位数组的大小。其中，列 value 映射到 8 位（即使用8个哈希函数），列 value2 映射到 4 位（即使用 4 个哈希函数）。
-- 代码中最多有 32 个哈希函数，其实都是基于基础哈希函数 mytext_hash，配合不同的种子值（seed）生成多个哈希结果，目的是减少误判。哈希函数越多，误判率越低。
-- WITH 部分可以省略，因为其有默认值。
CREATE INDEX mytext_value_bloom_idx2 ON test_mytext USING bloom (value, value2) WITH (length=80, col1=8, col2=4);
-- 等效于：
-- WITH (
--   bloom_length = 80,                 -- 总位数
--   col1_hashfunctions = 8,            -- 第一列的哈希函数数
--   col2_hashfunctions = 4             -- 第二列的哈希函数数
-- );

-- 验证 Bloom 索引是否使用正确的操作符类
WITH index_info AS (
    SELECT idx.relname AS index_name, pg_get_indexdef(idx.oid) AS index_definition,
        unnest(i.indkey) AS attnum, unnest(i.indclass) AS opclass_oid
    FROM pg_index i JOIN pg_class idx ON i.indexrelid = idx.oid
    WHERE i.indrelid = 'test_mytext'::regclass),
column_info AS (
    SELECT att.attnum, att.attname AS column_name, format_type(att.atttypid, att.atttypmod) AS data_type, opc.opcname AS default_opclass
    FROM pg_attribute att
    JOIN pg_type typ ON att.atttypid = typ.oid
    LEFT JOIN pg_opclass opc ON opc.opcintype = att.atttypid AND opc.opcdefault AND opc.opcfamily IN (
        SELECT opf.oid FROM pg_opfamily opf JOIN pg_am am ON opf.opfmethod = am.oid WHERE am.amname = 'bloom')
    WHERE att.attrelid = 'test_mytext'::regclass AND att.attnum > 0)
SELECT idx.index_name, idx.index_definition, col.column_name, col.data_type, col.default_opclass, opc.opcname AS index_opclass
FROM index_info idx
JOIN column_info col ON idx.attnum = col.attnum
LEFT JOIN pg_opclass opc ON idx.opclass_oid = opc.oid;
-- 期望输出：索引包括两个列，所以输出了两条
--        index_name        |                                                       index_definition                                                        | column_name | data_type | default_opclass  |  index_opclass
-- -------------------------+-------------------------------------------------------------------------------------------------------------------------------+-------------+-----------+------------------+------------------
--  test_mytext_pkey        | CREATE UNIQUE INDEX test_mytext_pkey ON public.test_mytext USING btree (id)                                                   | id          | integer   | int4_ops         | int4_ops
--  mytext_value_bloom_idx2 | CREATE INDEX mytext_value_bloom_idx2 ON public.test_mytext USING bloom (value, value2) WITH (length='80', col1='8', col2='4') | value       | mytext    | mytext_bloom_ops | mytext_bloom_ops
--  mytext_value_bloom_idx2 | CREATE INDEX mytext_value_bloom_idx2 ON public.test_mytext USING bloom (value, value2) WITH (length='80', col1='8', col2='4') | value2      | mytext    | mytext_bloom_ops | mytext_bloom_ops
-- (3 rows)

-- 使用自定义操作符进行查询（Bloom 索引）
SET enable_seqscan = off;

SELECT * FROM test_mytext WHERE value = 'example'; -- 测试等于操作符

-- 期望结果包含：
-- Bitmap Heap Scan on public.test_mytext
-- Recheck Cond: (test_mytext.value = 'example'::mytext)
-- Bitmap Index Scan on mytext_value_bloom_idx
-- Index Cond: (test_mytext.value = 'example'::mytext)
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT * FROM test_mytext WHERE value = 'example'; -- 测试等于操作符

SET enable_seqscan = on;

DROP INDEX mytext_value_bloom_idx2;

---------------- 验证操作符（BRIN 索引） ----------------

-- Deep Seek 认为 BRIN 索引可以支持 mytext 类型，但需满足以下条件：
-- 1. 实现比较操作符（<, <=, =, >=, >）。
-- 2. 正确编写 BRIN 支持函数（初始化、添加值、一致性检查、合并摘要）。
-- 3. 在操作符类中绑定操作符与函数。

CREATE INDEX mytext_value_brin_idx ON test_mytext USING brin (value mytext_brin_ops) WITH (pages_per_range=128);

-- 验证 BRIN 索引是否使用正确的操作符类
WITH index_info AS (
    SELECT idx.relname AS index_name, pg_get_indexdef(idx.oid) AS index_definition,
        unnest(i.indkey) AS attnum, unnest(i.indclass) AS opclass_oid
    FROM pg_index i JOIN pg_class idx ON i.indexrelid = idx.oid
    WHERE i.indrelid = 'test_mytext'::regclass),
column_info AS (
    SELECT att.attnum, att.attname AS column_name, format_type(att.atttypid, att.atttypmod) AS data_type, opc.opcname AS default_opclass
    FROM pg_attribute att
    JOIN pg_type typ ON att.atttypid = typ.oid
    LEFT JOIN pg_opclass opc ON opc.opcintype = att.atttypid AND opc.opcdefault AND opc.opcfamily IN (
        SELECT opf.oid FROM pg_opfamily opf JOIN pg_am am ON opf.opfmethod = am.oid WHERE am.amname = 'brin')
    WHERE att.attrelid = 'test_mytext'::regclass AND att.attnum > 0)
SELECT idx.index_name, idx.index_definition, col.column_name, col.data_type, col.default_opclass, opc.opcname AS index_opclass
FROM index_info idx
JOIN column_info col ON idx.attnum = col.attnum
LEFT JOIN pg_opclass opc ON idx.opclass_oid = opc.oid;
-- 期望输出
--       index_name       |                                             index_definition                                             | column_name | data_type | default_opclass |  index_opclass
-- -----------------------+----------------------------------------------------------------------------------------------------------+-------------+-----------+-----------------+-----------------
--  test_mytext_pkey      | CREATE UNIQUE INDEX test_mytext_pkey ON public.test_mytext USING btree (id)                              | id          | integer   | int4_minmax_ops | int4_ops
--  mytext_value_brin_idx | CREATE INDEX mytext_value_brin_idx ON public.test_mytext USING brin (value) WITH (pages_per_range='128') | value       | mytext    | mytext_brin_ops | mytext_brin_ops
-- (2 rows)

-- 使用自定义操作符进行查询（BRIN 索引）
SET enable_seqscan = off;
SELECT * FROM test_mytext WHERE value > 'hello'; -- 测试大于操作符
SELECT * FROM test_mytext WHERE value <= 'world'; -- 测试小于等于操作符
SELECT * FROM test_mytext WHERE value = 'example'; -- 测试等于操作符
SELECT * FROM test_mytext WHERE value >= 'testing'; -- 测试大于等于操作符
SELECT * FROM test_mytext WHERE value < 'postgresql'; -- 测试小于操作符
-- 期望输出的过滤条件为：
-- Recheck Cond: (test_mytext.value > 'hello'::mytext)
-- Index Cond: (test_mytext.value > 'hello'::mytext)
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT * FROM test_mytext WHERE value > 'hello'; -- 测试大于操作符
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT * FROM test_mytext WHERE value <= 'world'; -- 测试小于等于操作符
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT * FROM test_mytext WHERE value = 'example'; -- 测试等于操作符
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT * FROM test_mytext WHERE value >= 'testing'; -- 测试大于等于操作符
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT * FROM test_mytext WHERE value < 'postgresql'; -- 测试小于操作符
SET enable_seqscan = on;

DROP INDEX mytext_value_brin_idx;

---------------- 清理数据 ----------------
DROP TABLE test_mytext;