-- ==================== 测试自定义数据类型 tinyfloat ====================

---------------- 基础功能测试 ----------------

-- 创建测试表
CREATE TABLE float_test (
    id SERIAL PRIMARY KEY,
    value tinyfloat
);

-- 创建索引（自动继承 real 类型的索引支持）
CREATE INDEX idx_btree_float ON float_test USING BTREE (value);
CREATE INDEX idx_hash_float ON float_test USING HASH (value);

-- 有效值测试
INSERT INTO float_test (value) VALUES (5.5);   -- 成功
INSERT INTO float_test (value) VALUES (10.0);  -- 成功
INSERT INTO float_test (value) VALUES (-9.9);  -- 成功

-- 自动类型转换测试
INSERT INTO float_test (value) VALUES (8);     -- 整数自动转浮点 (8.0)
INSERT INTO float_test (value) VALUES ('7.2'); -- 字符串自动转浮点

-- 无效值测试
INSERT INTO float_test (value) VALUES (10.1);  -- 超出上限 (触发错误)
INSERT INTO float_test (value) VALUES (-10.1); -- 超出下限 (触发错误)
INSERT INTO float_test (value) VALUES (200);   -- 整数值 200 超出范围 (触发错误)

-- 索引使用测试
SET enable_seqscan = OFF;
EXPLAIN SELECT * FROM float_test WHERE value >= 5.5;  -- 显示使用 BTree 索引
EXPLAIN SELECT * FROM float_test WHERE value = -9.9; -- 显示使用 Hash 索引
RESET enable_seqscan;

---------------- 清理测试数据 ----------------

DROP INDEX IF EXISTS idx_btree_float;
DROP INDEX IF EXISTS idx_hash_float;
DROP TABLE IF EXISTS float_test;