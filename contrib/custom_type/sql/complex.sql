-- ====================== complex 类型元信息验证 ======================
-- 验证操作符类是否创建成功（注意替换 complex_ops 和 btree）
SELECT opcname AS opclass_name,
       amname AS index_method,
       opcintype::regtype AS data_type,
       opcfamily::regclass AS opfamily
FROM pg_opclass
JOIN pg_am ON pg_am.oid = opcmethod
WHERE opcname = 'complex_ops'
  AND amname = 'btree';

-- 预期输出：
--  opclass_name | index_method | data_type | opfamily 
-- --------------+--------------+-----------+----------
--  complex_ops  | btree        | complex   | 16405
-- (1 row)

-- 查看操作符类绑定的操作符及策略号（策略号1-5对应B-tree标准顺序）
SELECT amopstrategy AS strategy,
       amopopr::regoperator AS operator,
       amoplefttype::regtype AS left_type,
       amoprighttype::regtype AS right_type
FROM pg_amop
WHERE amopfamily = (SELECT opcfamily FROM pg_opclass WHERE opcname = 'complex_ops')
ORDER BY amopstrategy;

-- 预期输出：
--  strategy |      operator       | left_type | right_type 
-- ----------+---------------------+-----------+------------
--         1 | <(complex,complex)  | complex   | complex
--         2 | <=(complex,complex) | complex   | complex
--         3 | =(complex,complex)  | complex   | complex
--         4 | >=(complex,complex) | complex   | complex
--         5 | >(complex,complex)  | complex   | complex
-- (5 rows)

-- 查看操作符类关联的比较函数
SELECT amprocnum AS func_num,
       amproc::regprocedure AS function,
       amproclefttype::regtype AS left_type,
       amprocrighttype::regtype AS right_type
FROM pg_amproc
WHERE amprocfamily = (SELECT opcfamily FROM pg_opclass WHERE opcname = 'complex_ops');

-- 预期输出：
--  func_num |           function           | left_type | right_type 
-- ----------+------------------------------+-----------+------------
--         1 | complex_cmp(complex,complex) | complex   | complex
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
  AND oprleft = 'complex'::regtype
  AND oprright = 'complex'::regtype;

-- 预期输出片段：
--  operator | left_type | right_type |            function            |     commutator      |       negator       
-- ----------+-----------+------------+--------------------------------+---------------------+---------------------
--  <        | complex   | complex    | complex_op_lt(complex,complex) | >(complex,complex)  | >=(complex,complex)
--  <=       | complex   | complex    | complex_op_le(complex,complex) | >=(complex,complex) | >(complex,complex)
--  =        | complex   | complex    | complex_op_eq(complex,complex) | =(complex,complex)  | <>(complex,complex)
--  >        | complex   | complex    | complex_op_gt(complex,complex) | <(complex,complex)  | <=(complex,complex)
--  >=       | complex   | complex    | complex_op_ge(complex,complex) | <=(complex,complex) | <(complex,complex)
-- (5 rows)


-- ==================== 测试自定义复数类型 complex ====================

---------------- 基础功能测试 ----------------
-- 测试类型创建
CREATE TABLE complex_numbers (
    id SERIAL PRIMARY KEY,
    num complex,
    description TEXT
);

-- 测试有效输入格式
INSERT INTO complex_numbers (num, description) VALUES 
('3.5+2.1i', '基本正数'),
('-1.2+4i',  '负实部'),
('0+3.9i',   '零实部'),
('5+0i',     '零虚部'),
(NULL,       '空值测试');

-- 测试无效输入格式（应该抛出异常）
-- 缺少虚部，ERROR:  invalid input syntax for complex: "3.5"
INSERT INTO complex_numbers (num) VALUES ('3.5');
-- 非数字字符，ERROR:  invalid input syntax for complex: "abc+defi"
INSERT INTO complex_numbers (num) VALUES ('abc+defi');
-- 多余字符
INSERT INTO complex_numbers (num) VALUES ('2.3+4i5');

-- 测试查询输出
SELECT id, num, description FROM complex_numbers ORDER BY id;

-- 测试WHERE条件
SELECT * FROM complex_numbers WHERE num = '3.5+2.1i'; -- 过滤条件为：Filter: (num = '3.5+2.1i'::complex)
SELECT * FROM complex_numbers WHERE num IS NULL; -- 过滤条件为：Filter: (num IS NULL)

-- 测试更新操作
UPDATE complex_numbers SET num = '-0.5+1.2i' WHERE id = 1 RETURNING *;

-- 测试删除操作
DELETE FROM complex_numbers WHERE num = '5+0i' RETURNING *;

---------------- 运算函数测试 ----------------
-- 测试复数加法
SELECT complex_add('1+2i', '3+4i') AS sum_result;

-- 测试运算与存储结合
INSERT INTO complex_numbers (num) VALUES
(complex_add('1.5+2.5i', '0.5+0.5i')),
(complex_add('-2+3i', '4+-1i'));

---------------- 类型转换测试 ----------------
-- 测试显式类型转换
SELECT '3.14+-2.72i'::complex;

-- 测试非法转换（应该报错），预期 ERROR:  invalid input syntax for complex: "invalid"
SELECT 'invalid'::complex;

---------------- 索引测试 ----------------【失败，缺少对应的操作符】
-- 创建索引
CREATE INDEX complex_num_idx ON complex_numbers (num complex_ops);

-- 验证B-tree索引是否使用正确的操作符类
SELECT relname AS index_name,
       pg_get_indexdef(indexrelid) AS index_definition
FROM pg_index
JOIN pg_class ON pg_class.oid = indexrelid
WHERE indrelid = 'complex_numbers'::regclass;

-- 预期输出：
--       index_name      |                                   index_definition                                   
-- ----------------------+--------------------------------------------------------------------------------------
--  complex_numbers_pkey | CREATE UNIQUE INDEX complex_numbers_pkey ON public.complex_numbers USING btree (id)
--  complex_num_idx      | CREATE INDEX complex_num_idx ON public.complex_numbers USING btree (num complex_ops)
-- (2 rows)

SET enable_seqscan = OFF;
-- 预期过滤条件为：Index Cond: (complex_numbers.num = '3.5+2.1i'::complex)
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) SELECT * FROM complex_numbers WHERE num = '3.5+2.1i';
RESET enable_seqscan;

---------------- 边界值测试 ----------------
-- 极大/极小值测试
INSERT INTO complex_numbers (num) VALUES
('1e308+1e308i'),   -- 最大双精度值
('-1e308+1e308i'),
('1e-324+1e-324i'); -- 最小双精度值

-- 测试特殊数值（不识别，应该报错）
INSERT INTO complex_numbers (num) VALUES
('NaN+NaNi'),       -- NaN 测试
('1e308+1e308i');  -- 用非常大的数字代替 Infinity

-- 验证特殊值存储
SELECT num::text FROM complex_numbers WHERE num = 'NaN+NaNi'::complex; -- NaN 不等于 NaN，因此应该返回空
SELECT num::text FROM complex_numbers WHERE num = '1e308+1e308i'::complex;

---------------- 清理测试数据 ----------------
DROP INDEX IF EXISTS complex_num_idx;
DROP TABLE IF EXISTS complex_numbers;