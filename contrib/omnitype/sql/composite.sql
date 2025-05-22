-- ====================== composite 类型元信息验证 ======================
-- 验证操作符类是否创建成功（注意替换 composite_btree_ops 和 btree）
SELECT opcname AS opclass_name,
       amname AS index_method,
       opcintype::regtype AS data_type,
       opcfamily::regclass AS opfamily
FROM pg_opclass
JOIN pg_am ON pg_am.oid = opcmethod
WHERE opcname = 'composite_btree_ops'
  AND amname = 'btree';

-- 预期输出：
--     opclass_name     | index_method | data_type | opfamily
-- ---------------------+--------------+-----------+----------
--  composite_btree_ops | btree        | composite | 99620
-- (1 row)

-- 查看操作符类绑定的操作符及策略号（策略号1-5对应B-tree标准顺序）
SELECT amopstrategy AS strategy,
       amopopr::regoperator AS operator,
       amoplefttype::regtype AS left_type,
       amoprighttype::regtype AS right_type
FROM pg_amop
WHERE amopfamily = (SELECT opcfamily FROM pg_opclass WHERE opcname = 'composite_btree_ops')
ORDER BY amopstrategy;

-- 预期输出：
--  strategy |        operator         | left_type | right_type
-- ----------+-------------------------+-----------+------------
--         1 | <(composite,composite)  | composite | composite
--         2 | <=(composite,composite) | composite | composite
--         3 | =(composite,composite)  | composite | composite
--         4 | >=(composite,composite) | composite | composite
--         5 | >(composite,composite)  | composite | composite
-- (5 rows)

-- 查看操作符类关联的比较函数
SELECT amprocnum AS func_num,
       amproc::regprocedure AS function,
       amproclefttype::regtype AS left_type,
       amprocrighttype::regtype AS right_type
FROM pg_amproc
WHERE amprocfamily = (SELECT opcfamily FROM pg_opclass WHERE opcname = 'composite_btree_ops');

-- 预期输出：
--  func_num |              function              | left_type | right_type
-- ----------+------------------------------------+-----------+------------
--         1 | composite_cmp(composite,composite) | composite | composite
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
  AND oprleft = 'composite'::regtype
  AND oprright = 'composite'::regtype;

-- 预期输出片段：
--  operator | left_type | right_type |             function              |       commutator        |         negator
-- ----------+-----------+------------+-----------------------------------+-------------------------+-------------------------
--  <        | composite | composite  | composite_lt(composite,composite) | >(composite,composite)  | >=(composite,composite)
--  <=       | composite | composite  | composite_le(composite,composite) | >=(composite,composite) | >(composite,composite)
--  =        | composite | composite  | composite_eq(composite,composite) | =(composite,composite)  | 0
--  >        | composite | composite  | composite_gt(composite,composite) | <(composite,composite)  | <=(composite,composite)
--  >=       | composite | composite  | composite_ge(composite,composite) | <=(composite,composite) | <(composite,composite)
-- (5 rows)


-- ==================== 测试自定义复数类型 composite ====================

---------------- 基础功能测试 ----------------
-- 测试 in、out 函数
-- 包含空格的子串必须以 "" 标注，否则默认情况下空格等空白符会被当成分隔符
-- SELECT 'text|varchar|char|\xDEADBEEF|{"key":123}|<xml>data</xml>|192.168.1.1|B10101|"quick brown"|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|108.666|"2023-01-01 12:34:56"|2023-01-01|5678|{1,2,3}|[2023-01-01,2023-12-31]'::composite; -- 包含数组、范围类型
SELECT 'text|varchar|char|\xDEADBEEF|{"key":123}|<xml>data</xml>|192.168.1.1|B10101|"quick brown"|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|108.666|"2023-01-01 12:34:56"|2023-01-01|5678'::composite;
-- 期望结果
--                                                                                  composite
-- ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--  text|varchar|char|\xdeadbeef|{"key":123}|<xml>data</xml>|192.168.1.1|10101|'brown' 'quick'|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|108.666|2023-01-01 12:34:56|2023-01-01|5678
-- (1 row)

---------------- 比较运算符 ----------------

-- 期望 true

SELECT 'text|varchar|char|\xDEADBEEF|{"key":123}|<xml>data</xml>|192.168.1.1|B10101|"quick brown"|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|108.666|"2023-01-01 12:34:56"|2023-01-01
|5678'::composite < 'text|varchar|char|\xDEADBEEF|{"key":123}|<xml>data</xml>|192.168.1.1|B10101|"quick brown"|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|108.666|"2023-01-01 12:34:56"|2023-01-02|5678'::composite; -- 2023-01-01 < 2023-01-02

SELECT 'text|varchar|char|\xDEADBEEF|{"key":123}|<xml>data</xml>|192.168.1.1|B10101|"quick brown"|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|108.666|"2023-01-01 12:34:56"|2023-01-01
|5678'::composite <= 'text|varchar|char|\xDEADBEEF|{"key":124}|<xml>data</xml>|192.168.1.1|B10101|"quick brown"|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|108.666|"2023-01-01 12:34:56"|2023-01-01|5678'::composite; -- json 部分 123 < 124

SELECT 'text|varchar|char|\xDEADBEEF|{"key":123}|<xml>data</xml>|192.168.1.1|B10101|"quick brown"|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|108.666|"2023-01-01 12:34:56"|2023-01-01
|5678'::composite = 'text|varchar|char|\xDEADBEEF|{"key":123}|<xml>data</xml>|192.168.1.1|B10101|"quick brown"|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|108.666|"2023-01-01 12:34:56"|2023-01-01|5678'::composite; -- 完全相同

SELECT 'text|varchar|char|\xDEADBEEF|{"key":123}|<xml>data</xml>|192.168.1.1|B10101|"quick brown"|b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|108.666|"2023-01-01 12:34:56"|2023-01-01
|5678'::composite >= 'text|varchar|char|\xDEADBEEF|{"key":123}|<xml>data</xml>|192.168.1.1|B10101|"quick brown"|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|108.666|"2023-01-01 12:34:56"|2023-01-01|5678'::composite; -- uuid 部分 b0eebc99 >= a0eebc99

SELECT 'text|varchar|char|\xDEADBEEF|{"key":123}|<xml>data</xml>|192.168.1.1|B10101|"quick brown"|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|108.666|"2023-01-01 12:34:56"|2023-01-01|5678'::composite > 'text|varchar|char|\xDEADBEEF|{"key":123}|<xml>data</xml>|192.168.1.1|B10101|"quick brown"|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|108.666|"2023-01-01 12:34:56"|2023-01-01|5677'::composite; -- 5678 > 5677

-- 期望 false

SELECT 'text|varchar|char|\xDEADBEEF|{"key":123}|<xml>data3</xml>|192.168.1.1|B10101|"quick brown"|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|108.666|"2023-01-01 12:34:56"|2023-01-01|5678'::composite <= 'text|varchar|char|\xDEADBEEF|{"key":123}|<xml>data2</xml>|192.168.1.1|B10101|"quick brown"|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|108.666|"2023-01-01 12:34:56"|2023-01-01|5678'::composite; -- xml 部分 data3 > data2

-- 验证词法单元的分解、比较
SELECT tsvector_to_array('quick fast'::tsvector);  -- 输出：{fast,quick}
SELECT tsvector_to_array('quick brown'::tsvector); -- 输出：{brown,quick}
SELECT 'quick fast'::tsvector < 'quick brown'::tsvector;  -- 返回 t（true）
SELECT 'quick fast'::tsvector > 'quick brown'::tsvector;  -- 返回 f（false）
SELECT 'text|varchar|char|\xDEADBEEF|{"key":123}|<xml>data</xml>|192.168.1.1|B10101|"quick fast"|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|108.666|"2023-01-01 12:34:56"|2023-01-01|5678'::composite >= 'text|varchar|char|\xDEADBEEF|{"key":123}|<xml>data</xml>|192.168.1.1|B10101|"quick brown"|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|108.666|"2023-01-01 12:34:56"|2023-01-01|5677'::composite; -- TSVector 部分 quick fast < quick brown 的，反常理，怀疑是逆序

-- 测试类型创建
DROP TABLE IF EXISTS comp_test;
CREATE TABLE comp_test (
    id serial PRIMARY KEY,
    data composite
);

-- 注意：传入的字符串与最后存储的内容在格式上可能有一定偏差，比如 TSVector，传入 "quick brown"，存储的却是 'brown' 'quick'
INSERT INTO comp_test (data) VALUES
    ('text1|varchar1|char1|\x48656c6c6f20576f726c6421|{"key":123}|<xml>data1</xml>|192.168.1.1|B10100|"quick brown"|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|100.106|"2023-01-01 12:30:55"|2023-01-01|5678'); -- 二进制部分是 "Hello World!" 的十六进制表示

INSERT INTO comp_test (data) VALUES
    ('text2|varchar2|char2|\x546869732069732061207465737421|{"key":123}|<xml>data2</xml>|192.168.1.2|B10101|"quick call"|ffffbc99-ffff-4ef8-ffff-6bb9bd380a11|110.360|"2023-02-01 13:00:01"|2023-02-01|6789'); -- 二进制部分是 "This is a test!" 的十六进制表示

INSERT INTO comp_test (data) VALUES
    ('text4|varchar4|char4|\x42696e6172792044617461|{"key":123}|<xml>data3</xml>|192.168.1.4|B10110|quickfast|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|111.666|"2023-03-01 18:00:01"|2023-03-01|6666'); -- 二进制部分是 "Binary Data" 的十六进制表示

INSERT INTO comp_test (data) VALUES
    ('text3|varchar3|char3|\x42696e6172792044617461|{"key":123}|<xml>data3</xml>|192.168.1.3|B10110|quickfast|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|111.666|"2023-03-01 18:00:01"|2023-03-01|6666'); -- 二进制部分是 "Binary Data" 的十六进制表示

INSERT INTO comp_test (data) VALUES
    ('text2|varchar3|char2|\x546869732069732061207465737421|{"key":123}|<xml>data2</xml>|192.168.1.2|B11101|"quick call"|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|110.360|"2023-02-01 13:00:01"|2023-02-01|6789'); -- 二进制部分是 "This is a test!" 的十六进制表示

INSERT INTO comp_test (data) VALUES
    ('text2|varchar2|char3|\x546869732069732061207465737421|{"key":123}|<xml>data2</xml>|192.168.1.2|B10101|"quick call"|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|110.360|"2023-02-01 13:00:01"|2023-02-01|6789'); -- 二进制部分是 "This is a test!" 的十六进制表示

    -- 其他：
    -- \x42696e6172792044617461  -- Binary Data
    -- \x48656c6c6f  -- Hello
    -- \x506f737467726571534c  -- PostgreSQL
    -- \x446174612053746f72616765  -- Data Storage
    -- \x53616d706c652054657874  -- Sample Text
    -- \x496d6167652044617461  -- Image Data
    -- \x46696c65205472616e73666572  -- File Transfer
    -- \x4461746162617365204578616d706c65  -- Database Example
    -- \x4e6574776f726b205061636b6574  -- Network Packet

-- 测试无效输入格式（应该抛出异常）
INSERT INTO comp_test (data) VALUES('text2|varchar2|char2|');
-- 期望输出
-- ERROR:  invalid input syntax for composite type
-- LINE 1: INSERT INTO comp_test (data) VALUES('text2|varchar2|char2|')...
                                            ^
INSERT INTO comp_test (data) VALUES('text2|varchar2|char2|\x1212');
-- 期望输出
-- ERROR:  composite type requires exactly 14 fields
-- LINE 1: INSERT INTO comp_test (data) VALUES('text2|varchar2|char2|\x...

-- 测试查询输出
SELECT * FROM comp_test;
-- 期望输出
--  id |                                                                                                 data                                                                                                 
-- ----+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--   1 | text1|varchar1|char1|\x48656c6c6f20576f726c6421|{"key": 123}|<xml>data1</xml>|192.168.1.1|10100|'brown' 'quick'|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|100.106|2023-01-01 12:30:55|2023-01-01|5678
--   2 | text2|varchar2|char2|\x546869732069732061207465737421|{"key": 123}|<xml>data2</xml>|192.168.1.2|10101|'call' 'quick'|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|110.36|2023-02-01 13:00:01|2023-02-01|6789
-- (2 rows)

-- order by
SELECT * FROM comp_test ORDER BY data; -- 依赖比较操作符

-- where 条件
SELECT * FROM comp_test WHERE data > 'text1|varchar2|char1|\x48656c6c6f20576f726c6421|{"key":123}|<xml>data1</xml>|192.168.1.1|B10100|"quick brown"|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|100.106|"2023-01-01 12:30:55"|2023-01-01|5678'; -- 会跳过 text1 开头的行

-- update 操作
UPDATE comp_test SET data = 'text20|varchar20|char20|\x48656c6c6f20576f726c6421|{"key":123}|<xml>data1</xml>|192.168.1.1|B10100|"quick brown"|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|100.106|"2023-01-01 12:30:55"|2023-01-01|5678' WHERE data = 'text1|varchar1|char1|\x48656c6c6f20576f726c6421|{"key":123}|<xml>data1</xml>|192.168.1.1|B10100|"quick brown"|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|100.106|"2023-01-01 12:30:55"|2023-01-01|5678';
-- 期待输出：UPDATE 1

-- delete 操作，与 update 操作类似

---------------- 索引测试 ----------------
-- 创建索引
CREATE INDEX comp_test_btree_idx ON comp_test (data composite_btree_ops);

-- 验证B-tree索引是否使用正确的操作符类
SELECT opc.opcname AS opclass_name, opf.opfname AS opfamily_name,
    am.amname AS index_method, opc.opcintype::regtype AS indexed_type
FROM pg_opclass opc
JOIN pg_opfamily opf ON opc.opcfamily = opf.oid
JOIN pg_am am ON opf.opfmethod = am.oid
WHERE opc.opcname = 'composite_btree_ops';
-- 期望输出
--     opclass_name     |    opfamily_name    | index_method | indexed_type
-- ---------------------+---------------------+--------------+--------------
--  composite_btree_ops | composite_btree_ops | btree        | composite
-- (1 row)

SET enable_seqscan = OFF;

-- 关键输出：Index Scan using comp_test_btree_idx
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT * FROM comp_test ORDER BY data;
-- 关键输出：Index Cond: (comp_test.data > 'text1|varchar2|char1|\x48656c6c6f20576f726c6421|{"key": 123}|<xml>data1</xml>|192.168.1.1|10100|''brown'' ''quick''|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|100.106|2023-01-01 12:30:55|2023-01-01|5678'::composite)
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT * FROM comp_test WHERE data > 'text1|varchar2|char1|\x48656c6c6f20576f726c6421|{"key":123}|<xml>data1</xml>|192.168.1.1|B10100|"quick brown"|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|100.106|"2023-01-01 12:30:55"|2023-01-01|5678';;

RESET enable_seqscan;

---------------- 清理测试数据 ----------------
DROP INDEX IF EXISTS comp_test_btree_idx;
DROP TABLE IF EXISTS comp_test;
