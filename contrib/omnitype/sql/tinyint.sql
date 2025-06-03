-- ==================== 测试自定义数据类型 tinyint ====================

---------------- 基础功能测试 ----------------

-- 创建测试表
CREATE TABLE tinyint_test (
    id SERIAL PRIMARY KEY,
    value tinyint
);

-- 创建索引
CREATE INDEX idx_btree_tinyint ON tinyint_test USING BTREE (value tinyint_btree_ops); -- ops 字段可选
CREATE INDEX idx_hash_tinyint ON tinyint_test USING HASH (value tinyint_hash_ops);

-- 有效值测试
INSERT INTO tinyint_test (value) VALUES (100);  -- 成功
INSERT INTO tinyint_test (value) VALUES (120);  -- 成功
INSERT INTO tinyint_test (value) VALUES (-127); -- 成功

-- 自动类型转换测试
INSERT INTO tinyint_test (value) VALUES ('110'); -- 字符串自动转整型

-- 无效值测试
INSERT INTO tinyint_test (value) VALUES (128);  -- 超出上限 (触发错误)
INSERT INTO tinyint_test (value) VALUES (-130); -- 超出下限 (触发错误)

-- 索引使用测试
SET enable_seqscan = OFF;

SELECT * FROM tinyint_test WHERE value >= 100;
SELECT * FROM tinyint_test WHERE 100 >= value;
SELECT * FROM tinyint_test WHERE value = 120;

-- 显示使用 BTree 索引
EXPLAIN ANALYZE SELECT * FROM tinyint_test WHERE value >= 100; -- 预期过滤条件：Index Cond: (value >= '100'::tinyint)
EXPLAIN ANALYZE SELECT * FROM tinyint_test WHERE 100 >= value;  -- 预期过滤条件：Index Cond: (value <= '100'::tinyint)
-- 显示使用 Hash 索引
EXPLAIN ANALYZE SELECT * FROM tinyint_test WHERE value = 120; -- 预期过滤条件：Index Cond: (value = '120'::tinyint)

RESET enable_seqscan;

-- 清理测试数据

DROP INDEX IF EXISTS idx_btree_tinyint;
DROP INDEX IF EXISTS idx_hash_tinyint;
DROP TABLE IF EXISTS tinyint_test;

---------------- 特殊情况（tinyint --> int4） ----------------

-- 创建测试表
CREATE TABLE tinyint_test (
    id SERIAL PRIMARY KEY,
    value int
);

-- 创建索引
CREATE INDEX idx_btree_int ON tinyint_test USING BTREE (value);
CREATE INDEX idx_hash_int ON tinyint_test USING HASH (value);

-- 有效值测试
INSERT INTO tinyint_test (value) VALUES (-127); -- 成功
INSERT INTO tinyint_test (value) VALUES (-130); -- 成功
INSERT INTO tinyint_test (value) VALUES (128); -- 成功
INSERT INTO tinyint_test (value) VALUES (100); -- 成功
INSERT INTO tinyint_test (value) VALUES (90); -- 成功

-- 无效值测试
-- 报错：ERROR:  column "value" is of type integer but expression is of type tinyint
INSERT INTO tinyint_test (value) VALUES (100::tinyint);
INSERT INTO tinyint_test (value) VALUES (120::tinyint);
INSERT INTO tinyint_test (value) VALUES (128::tinyint);

-- 自动类型转换测试
INSERT INTO tinyint_test (value) VALUES ('110'); -- 字符串自动转整型

-- 索引使用测试
SET enable_seqscan = OFF;

SELECT * FROM tinyint_test WHERE value >= 100;
SELECT * FROM tinyint_test WHERE 100 >= value;
SELECT * FROM tinyint_test WHERE value = 90;

-- 显示使用 BTree 索引
EXPLAIN ANALYZE SELECT * FROM tinyint_test WHERE value >= 100; -- Index Cond: (value >= 100)
EXPLAIN ANALYZE SELECT * FROM tinyint_test WHERE 100 >= value; -- Index Cond: (value <= 100)
-- 显示使用 Hash 索引
EXPLAIN ANALYZE SELECT * FROM tinyint_test WHERE value = 90; -- Index Cond: (value = 90)

RESET enable_seqscan;

-- 清理测试数据
DROP INDEX IF EXISTS idx_btree_int;
DROP INDEX IF EXISTS idx_hash_int;
DROP TABLE IF EXISTS tinyint_test;
