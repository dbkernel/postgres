-- ==================== 测试自定义操作符 ====================

---------------- 准备测试数据 ----------------

CREATE TABLE test1 (
    id int,
    value numeric
);

INSERT INTO test1 (id, value) VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 15);
INSERT INTO test1 (id, value) VALUES (6, 35), (7, 14), (8, 36), (9, 25), (10, 30), (11, 20);

CREATE TABLE test2 (
    id int,
    value numeric
);

INSERT INTO test2 (id, value) VALUES (1, 20), (2, 30), (3, 40), (5, 15), (6, 35), (9, 25);

---------------- 测试自定义操作符 ----------------

-- 使用操作符计算两个数值的差的绝对值
SELECT 5 @@ 3; -- 返回 2
SELECT -2.5 @@ 3.1; -- 返回 5.6

-- 在WHERE子句中使用操作符
-- 期望执行计划中过滤条件为：
-- Filter: (abs((test1.value - '25'::numeric)) <= '10'::numeric) 或
-- Filter: ((test1.value @@ '25'::numeric) <= '10'::numeric)
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) 
    SELECT * FROM test1 WHERE value @@ 25 <= 10;

-- 在JOIN条件中使用操作符
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
    SELECT * FROM test1
    JOIN test2 ON test1.id = test2.id
    WHERE (test1.value @@ 25) <= 10
    AND (test2.value @@ 25) <= 10;

---------------- 清理测试数据 ----------------

DROP TABLE test1, test2;