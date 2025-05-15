-- ==================== 动态方式启动进程 ====================

-- 启动多个 sandbox worker in dynamic 进程
SELECT * FROM start_sandbox_worker();
SELECT * FROM start_sandbox_worker();
SELECT * FROM start_sandbox_worker();
-- 通过 ps -xf 可以看到进程列表
-- 2982456 ?        Ss     0:00  \_ postgres: sandbox worker in static
-- 2982809 ?        Ss     0:00  \_ postgres: sandbox worker in dynamic
-- 2988086 ?        Ss     0:00  \_ postgres: sandbox worker in dynamic
-- 2988139 ?        Ss     0:00  \_ postgres: sandbox worker in dynamic

-- ==================== 查询自定义进程元信息 ====================
-- 查询自定义进程
SELECT pid, backend_type, query_start, state
FROM pg_stat_activity
WHERE backend_type like 'sandbox worker%';
-- 示例输出
--    pid   |       backend_type        | query_start | state
-- ---------+---------------------------+-------------+-------
--  2982456 | sandbox worker in static  |             |
--  2982809 | sandbox worker in dynamic |             |
--  2988086 | sandbox worker in dynamic |             |
--  2988139 | sandbox worker in dynamic |             |
-- (4 rows)

-- ==================== 查询 C UDF 元信息 ====================
SELECT proname, provolatile
FROM pg_proc
WHERE proname = 'validate_table_c' OR proname = 'insert_records_c' OR
      proname like 'find_record%c' OR proname = 'drop_table_c';

-- C 函数样例
 select * from find_record_demo();
-- 期望输出
--  id | name | description |          created_at
-- ----+------+-------------+-------------------------------
--     |      | Jiang Su    | 2025-05-17 15:56:12.564014+08
-- (1 row)

-- ==================== 特别说明 ====================

-- NOTICE: 下面用例中的 AS row 语句指定了函数返回列的名称及类型；
--         若不想指定，可在函数定义中添加 RETURNS TABLE 子句指定函数返回列的名称及类型，比如 find_record_demo 函数。
--         这类似于 pg_proc.dat 中定义的内容。

-- ==================== 普通表（不涉及批量操作） ====================

-- 1. 创建表
SELECT validate_table_c('test_table');

-- 2. 插入多行数据
SELECT insert_records_c(
    'test_table'::text,
    ARRAY['First Data', 'Second Data', 'Third Data']
);

-- 3. 读取数据
-- 只读取一条
SELECT * FROM find_record_c('test_table'::text, 'NULL'::text) AS row(id int, data text); -- 无where条件，期望结果为 First Data
SELECT * FROM find_record_c('test_table'::text, 'data like ''Second%'''::text) AS row(id int, data text); -- 期望结果为 Second Data
SELECT * FROM find_record_c('test_table'::text, 'data like ''FF%'''::text) AS row(id int, data text); -- 期望结果为空
-- 读取多条（一次性缓存）
SELECT * FROM find_records_c('test_table'::text, 'NULL'::text) AS row(id int, data text);
SELECT * FROM find_records_c('test_table'::text, 'data like ''First%'''::text) AS row(id int, data text);
-- 读取多条（分批读取）
SELECT * FROM find_records_multi_call_c('test_table'::text, 'NULL'::text) AS row(id int, data text);
SELECT * FROM find_records_multi_call_c('test_table'::text, 'data like ''First%'''::text) AS row(id int, data text);

-- 4. 删除表
SELECT drop_table_c('test_table');

-- ==================== 大表（批量操作） ====================

-- 创建测试表
SELECT validate_table_c('large_table');

-- 插入测试数据（插入25行）
DO $$
DECLARE
    i text;
BEGIN
    FOR i IN 1..25 LOOP
        PERFORM insert_records_c('large_table'::text, ARRAY['Data ' || i]);
    END LOOP;
END $$;

-- 读取多条（一次性缓存）
SELECT * FROM find_records_c('large_table'::text, 'NULL'::text) AS row(id int, data text);
SELECT * FROM find_records_c('large_table'::text, 'data like ''Data 1%'''::text) AS row(id int, data text);
-- 读取多条（分批读取）
SELECT * FROM find_records_multi_call_c('large_table'::text, 'NULL'::text) AS row(id int, data text);
SELECT * FROM find_records_multi_call_c('large_table'::text, 'data like ''Data 1%'''::text) AS row(id int, data text);

-- 删除表
SELECT drop_table_c('large_table');

-- ==================== SQL UDF ====================

-- 建表：第一个参数是表名，第二个参数是表定义
SELECT validate_table_sql('employees', 'id SERIAL PRIMARY KEY, name TEXT, salary INT');

-- 插入一条：第一个参数是表名，第二个参数是列名，第三个参数是元组值
SELECT * FROM insert_record_sql(
    'employees',
    'name, salary',
    ARRAY['John', '50000']  -- 注意：所有元素需为同一类型（此处均转为TEXT）
) AS inserted(id INT, name TEXT, salary INT);

-- 插入多条：第一个参数是表名，第二个参数是列名，第三个参数是多行元组值
SELECT * FROM insert_records_sql(
    'employees',
    'name, salary',
    ARRAY[
        ARRAY['John'::TEXT, '50000'::TEXT],  -- 明确转换为TEXT数组
        ARRAY['Jane'::TEXT, '60000'::TEXT]
    ]
) AS inserted(id INT, name TEXT, salary INT);

-- 插入多条：第一个参数是表名，第二个参数是列名，第三个参数是多行元组值
SELECT * FROM insert_records_extend_sql(
    'employees',
    'name, salary',
    ARRAY[
        ['John', '50000'],  -- 无需::TEXT转换
        ['Jane', '60000']
    ]
) AS inserted(id INT, name TEXT, salary INT);

-- 查询：第一个参数是表名，第二个参数是 where 条件
SELECT * FROM find_records_sql('employees', 'salary > 40000')
AS result(id INT, name TEXT, salary INT);
SELECT * FROM find_records_sql('employees', 'salary > 50000')
AS result(id INT, name TEXT, salary INT);

-- 删表
SELECT drop_table_sql('employees');

-- ==================== 自定义聚合函数 ====================

-------------------- 准备测试数据 --------------------

-- 1. 员工表（累计工资/部门排名）
CREATE TABLE employees (
  emp_id SERIAL PRIMARY KEY,
  emp_name VARCHAR(50) NOT NULL,
  salary NUMERIC(10,2),
  hire_date DATE,
  dep_id INTEGER
);

INSERT INTO employees (emp_name, salary, hire_date, dep_id) VALUES
('Alice', 9000, '2020-01-15', 1),
('Bob', 8000, '2020-03-22', 1),
('Carol', 8500, '2021-02-10', 2),
('David', 5000, '2019-11-05', NULL),
('Eva', 6000, '2020-07-30', 1),
('Frank', 4500, '2021-05-12', 2),
('Grace', 7500, '2020-09-18', 1);

-- 2. 销售表（相邻月份比较/移动平均）
CREATE TABLE sales (
  month_id SERIAL PRIMARY KEY,
  month TEXT CHECK (month IN ('Jan','Feb','Mar','Apr','May','Jun')),
  revenue NUMERIC(10,2)
);

INSERT INTO sales (month, revenue) VALUES
('Jan', 10000),
('Feb', 12000),
('Mar', 15000),
('Apr', 13000),
('May', 17000),
('Jun', 16000);

-- 3. 学生成绩表（NTILE分布）
CREATE TABLE student_scores (
  student_id SERIAL PRIMARY KEY,
  student_name VARCHAR(50),
  score INTEGER,
  class_id INTEGER
);

INSERT INTO student_scores (student_name, score, class_id) VALUES
('Amy', 92, 1),
('Ben', 85, 1),
('Chris', 78, 2),
('Diana', 95, 1),
('Eric', 88, 2),
('Fiona', 82, 2),
('George', 90, 1),
('Hannah', 79, 2);

-------------------- 测试 PL/pgSQL 聚集函数 --------------------

-- 收集 employees 表中各部门的唯一工资值
SELECT
  dep_id,
  unique_array(salary) AS unique_salaries
FROM employees
GROUP BY dep_id;
-- 期望输出：
--  dep_id |          unique_salaries
-- --------+-----------------------------------
--         | {5000.00}
--       2 | {8500.00,4500.00}
--       1 | {9000.00,8000.00,6000.00,7500.00}
-- (3 rows)

-------------------- 测试 C 聚集函数 --------------------

-- 按部门计算工资中位数
SELECT dep_id, median_agg(salary) AS median_salary
FROM employees GROUP BY dep_id;
-- 期望输出：
--  dep_id |     median_salary
-- --------+-----------------------
--         |               5000.00
--       2 | 6500.0000000000000000
--       1 | 7750.0000000000000000
-- (3 rows)
