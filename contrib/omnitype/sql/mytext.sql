-- ====================== mytext 类型元信息验证 ======================
-- 验证操作符类是否创建成功（注意替换 mytext_ops 和 btree）
SELECT opcname AS opclass_name,
       amname AS index_method,
       opcintype::regtype AS data_type,
       opcfamily::regclass AS opfamily
FROM pg_opclass
JOIN pg_am ON pg_am.oid = opcmethod
WHERE opcname = 'mytext_ops'
  AND amname = 'btree';

-- 预期输出：
--  opclass_name | index_method | data_type | opfamily 
-- --------------+--------------+-----------+----------
--  mytext_ops   | btree        | mytext    | 16436
-- (1 row)

-- 查看操作符类绑定的操作符及策略号（策略号1-5对应B-tree标准顺序）
SELECT amopstrategy AS strategy,
       amopopr::regoperator AS operator,
       amoplefttype::regtype AS left_type,
       amoprighttype::regtype AS right_type
FROM pg_amop
WHERE amopfamily = (SELECT opcfamily FROM pg_opclass WHERE opcname = 'mytext_ops')
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
WHERE amprocfamily = (SELECT opcfamily FROM pg_opclass WHERE opcname = 'mytext_ops');

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
    value mytext
);

INSERT INTO test_mytext (value) VALUES
    ('hello'),
    ('world'),
    ('example'),
    ('testing'),
    ('postgresql'),
    ('mytext_test');

-- 查询插入的数据
SELECT id, value FROM test_mytext ORDER BY value;

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

-- 创建索引
CREATE INDEX idx_mytext_value ON test_mytext USING btree (value mytext_ops);

-- 验证B-tree索引是否使用正确的操作符类
SELECT relname AS index_name,
       pg_get_indexdef(indexrelid) AS index_definition
FROM pg_index
JOIN pg_class ON pg_class.oid = indexrelid
WHERE indrelid = 'test_mytext'::regclass;

-- 预期输出：
--     index_name    |                              index_definition                               
-- ------------------+-----------------------------------------------------------------------------
--  test_mytext_pkey | CREATE UNIQUE INDEX test_mytext_pkey ON public.test_mytext USING btree (id)
--  idx_mytext_value | CREATE INDEX idx_mytext_value ON public.test_mytext USING btree (value)
-- (2 rows)

-- 使用自定义操作符进行查询（有索引）
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

---------------- 清理测试数据 ----------------
DROP INDEX idx_mytext_value;
DROP TABLE test_mytext;