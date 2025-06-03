-- ==================== 测试自定义数据类型 tinyint_v2 ====================

---------------- 基础功能测试 ----------------

-- 创建测试表
CREATE TABLE tinyint_v2_test (
    id SERIAL PRIMARY KEY,
    value tinyint_v2
);

-- 创建索引（自动继承 real 类型的索引支持）
CREATE INDEX idx_btree_tinyint_v2 ON tinyint_v2_test USING BTREE (value tinyint_v2_btree_ops); -- ops 字段可选
CREATE INDEX idx_hash_tinyint_v2 ON tinyint_v2_test USING HASH (value tinyint_v2_hash_ops);

-- 有效值测试
INSERT INTO tinyint_v2_test (value) VALUES (100);   -- 成功
INSERT INTO tinyint_v2_test (value) VALUES (120);  -- 成功
INSERT INTO tinyint_v2_test (value) VALUES (-127);  -- 成功

-- 自动类型转换测试
INSERT INTO tinyint_v2_test (value) VALUES ('110'); -- 字符串自动转整型

-- 无效值测试
INSERT INTO tinyint_v2_test (value) VALUES (128);  -- 超出上限 (触发错误)
INSERT INTO tinyint_v2_test (value) VALUES (-130); -- 超出下限 (触发错误)

-- 索引使用测试
SET enable_seqscan = OFF;

SELECT * FROM tinyint_v2_test WHERE value >= 100;
SELECT * FROM tinyint_v2_test WHERE 100 >= value;
SELECT * FROM tinyint_v2_test WHERE value = 120;

-- 显示使用 BTree 索引
EXPLAIN ANALYZE SELECT * FROM tinyint_v2_test WHERE value >= 100; -- Index Cond: (value >= 100)
EXPLAIN ANALYZE SELECT * FROM tinyint_v2_test WHERE 100 >= value;  -- Index Cond: (value <= 100)，必须存在 integer <= tinyint_v2 操作符，否则会报错
-- 显示使用 Hash 索引
EXPLAIN ANALYZE SELECT * FROM tinyint_v2_test WHERE value = 120; -- Index Cond: (value = 120)

RESET enable_seqscan;

-- 清理测试数据

DROP INDEX IF EXISTS idx_btree_tinyint_v2;
DROP INDEX IF EXISTS idx_hash_tinyint_v2;
DROP TABLE IF EXISTS tinyint_v2_test;

---------------- 特殊情况（tinyint_v2 --> int4） ----------------

-- 创建测试表
CREATE TABLE tinyint_v2_test (
    id SERIAL PRIMARY KEY,
    value int
);

-- 创建索引
CREATE INDEX idx_btree_int ON tinyint_v2_test USING BTREE (value);
CREATE INDEX idx_hash_int ON tinyint_v2_test USING HASH (value);

-- 有效值测试
INSERT INTO tinyint_v2_test (value) VALUES (100::tinyint_v2); -- 成功
INSERT INTO tinyint_v2_test (value) VALUES (120::tinyint_v2); -- 成功
INSERT INTO tinyint_v2_test (value) VALUES (-127); -- 成功

-- 自动类型转换测试
INSERT INTO tinyint_v2_test (value) VALUES ('110'); -- 字符串自动转整型

-- 无效值测试
INSERT INTO tinyint_v2_test (value) VALUES (128::tinyint_v2);  -- 超出上限 (触发错误)
INSERT INTO tinyint_v2_test (value) VALUES (-130::tinyint_v2); -- 超出上限 (触发错误)

-- 索引使用测试
SET enable_seqscan = OFF;

SELECT * FROM tinyint_v2_test WHERE value >= 100;
SELECT * FROM tinyint_v2_test WHERE 100 >= value;
SELECT * FROM tinyint_v2_test WHERE value = 120;

-- 显示使用 BTree 索引
EXPLAIN ANALYZE SELECT * FROM tinyint_v2_test WHERE value >= 100; -- Index Cond: (value >= 100)
EXPLAIN ANALYZE SELECT * FROM tinyint_v2_test WHERE 100 >= value; -- Index Cond: (value <= 100)
-- 显示使用 Hash 索引
EXPLAIN ANALYZE SELECT * FROM tinyint_v2_test WHERE value = 120; -- Index Cond: (value = 120)

RESET enable_seqscan;

-- 清理测试数据
DROP INDEX IF EXISTS idx_btree_int;
DROP INDEX IF EXISTS idx_hash_int;
DROP TABLE IF EXISTS tinyint_v2_test;

