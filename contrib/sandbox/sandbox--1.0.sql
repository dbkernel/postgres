-- ==================== 动态方式启动进程 ====================

CREATE OR REPLACE FUNCTION start_sandbox_worker()
RETURNS bool
AS 'sandbox', 'start_sandbox_worker'
LANGUAGE C STRICT;

-- ==================== C UDF ====================

-- 注册函数：指定返回的各个字段类型，而无需在调用时指定
CREATE OR REPLACE FUNCTION find_record_demo()
RETURNS TABLE (
    id          int,            -- 对应int32或numeric
    name        varchar(100),   -- 对应varchar
    description text,           -- 对应text
    created_at  timestamp with time zone  -- 对应timestamp with time zone
)
AS 'sandbox', 'find_record_demo'
LANGUAGE C STRICT;

-- 检查并创建表
CREATE OR REPLACE FUNCTION validate_table_c(tablename text)
RETURNS void
AS 'sandbox', 'validate_table'
LANGUAGE C STRICT;

-- 插入多条记录
CREATE OR REPLACE FUNCTION insert_records_c(
    tablename text,
    data_array text[]
)
RETURNS bool
AS 'sandbox', 'insert_records'
LANGUAGE C VOLATILE;

-- 仅查找一条记录
CREATE OR REPLACE FUNCTION find_record_c(tablename text, wherecond text)
RETURNS SETOF record
AS 'sandbox', 'find_record'
LANGUAGE C VOLATILE;

-- 查找多条记录并返回（方式一）：物化方式（InitMaterializedSRF + tuplestore_putvalues）
-- 一次性生成所有结果，存入tuplestore，由PostgreSQL逐行返回，结果集上限受 work_mem 限制
CREATE OR REPLACE FUNCTION find_records_c(tablename text, wherecond text)
RETURNS SETOF record
AS 'sandbox', 'find_records'
LANGUAGE C VOLATILE;

-- 查找多条记录并返回（方式二）：多调用方式（SRF_FIRSTCALL_INIT + SRF_PERCALL_SETUP）
-- 分多次调用生成结果，每次调用返回一行（或一批）
CREATE OR REPLACE FUNCTION find_records_multi_call_c(tablename text, wherecond text)
RETURNS SETOF record
STRICT -- 严格模式，不处理 NULL 参数
PARALLEL SAFE -- 并行安全
AS 'sandbox', 'find_records_multi_call'
LANGUAGE C VOLATILE;

-- 删除表
CREATE OR REPLACE FUNCTION drop_table_c(tablename text)
RETURNS void
AS 'sandbox', 'drop_table'
LANGUAGE C STRICT;

-- ==================== SQL UDF ====================

-- 创建表前检查是否存在，存在则删除重建
CREATE OR REPLACE FUNCTION validate_table_sql(
    table_name TEXT,
    columns_def TEXT
) RETURNS VOID AS $$
BEGIN
    EXECUTE format('DROP TABLE IF EXISTS %I', table_name);
    EXECUTE format('CREATE TABLE %I (%s)', table_name, columns_def);
END;
$$ LANGUAGE plpgsql;

-- 插入一条数据并返回插入结果
CREATE OR REPLACE FUNCTION insert_record_sql(
    table_name TEXT,
    columns TEXT,
    values_arr ANYARRAY
) RETURNS SETOF RECORD AS $$
DECLARE
    values_list TEXT;
BEGIN
    -- 将数组元素转换为带引号的列表（如：'John', 50000）
    SELECT string_agg(quote_literal(v), ', ') INTO values_list
    FROM unnest(values_arr) AS v;

    -- 构建动态SQL，将值列表嵌入到VALUES子句中
    RETURN QUERY EXECUTE format(
        'INSERT INTO %I (%s) VALUES (%s) RETURNING *',
        table_name,
        columns,
        values_list
    );
END;
$$ LANGUAGE plpgsql;

-- 插入多条数据并返回插入结果
CREATE OR REPLACE FUNCTION insert_records_sql(
    table_name TEXT,
    columns TEXT,
    values_arr ANYARRAY  -- 参数仍保留ANYARRAY伪类型
) RETURNS SETOF RECORD AS $$
DECLARE
    values_list TEXT := '';
    row_values  TEXT[];  -- 明确声明为TEXT数组
    row_str     TEXT;
    i           INT;
BEGIN
    -- 遍历二维数组（假设输入格式为ARRAY[[row1], [row2], ...]）
    FOR i IN array_lower(values_arr, 1) .. array_upper(values_arr, 1) LOOP
        -- 提取单行并强制转换为TEXT[]
        row_values := values_arr[i:i]::TEXT[];

        -- 生成值列表（如：'John', 50000）
        SELECT string_agg(quote_literal(v), ', ') INTO row_str
        FROM unnest(row_values) AS v;

        -- 拼接多行VALUES
        IF i > array_lower(values_arr, 1) THEN
            values_list := values_list || ', ';
        END IF;
        values_list := values_list || '(' || row_str || ')';
    END LOOP;

    -- 构建动态SQL
    RETURN QUERY EXECUTE format(
        'INSERT INTO %I (%s) VALUES %s RETURNING *',
        table_name,
        columns,
        values_list
    );
END;
$$ LANGUAGE plpgsql;

-- 插入多条数据并返回插入结果（支持自动类型转换）
CREATE OR REPLACE FUNCTION insert_records_extend_sql(
    table_name TEXT,
    columns TEXT,
    values_arr TEXT[][]  -- 强制要求二维TEXT数组
) RETURNS SETOF RECORD AS $$
DECLARE
    values_list TEXT := '';
    row_values  TEXT[];
    row_str     TEXT;
BEGIN
    FOREACH row_values SLICE 1 IN ARRAY values_arr LOOP
        SELECT string_agg(quote_literal(v), ', ') INTO row_str
        FROM unnest(row_values) AS v;

        values_list := values_list ||
            CASE WHEN values_list = '' THEN '' ELSE ', ' END ||
            '(' || row_str || ')';
    END LOOP;

    RETURN QUERY EXECUTE format(
        'INSERT INTO %I (%s) VALUES %s RETURNING *',
        table_name,
        columns,
        values_list
    );
END;
$$ LANGUAGE plpgsql;

-- 读取满足条件的数据
CREATE OR REPLACE FUNCTION find_records_sql(
    table_name TEXT,
    where_cond TEXT DEFAULT 'TRUE'
) RETURNS SETOF RECORD AS $$
BEGIN
    RETURN QUERY EXECUTE format(
        'SELECT * FROM %I WHERE %s',
        table_name,
        where_cond
    );
END;
$$ LANGUAGE plpgsql;

-- 删除表
CREATE OR REPLACE FUNCTION drop_table_sql(table_name TEXT) RETURNS VOID AS $$
BEGIN
    EXECUTE format('DROP TABLE IF EXISTS %I', table_name);
END;
$$ LANGUAGE plpgsql;

-- ==================== 自定义聚集函数 ====================

-- C 语言

CREATE OR REPLACE FUNCTION median_agg_transfn(internal, numeric)
RETURNS internal
AS 'sandbox', 'median_agg_transfn'
LANGUAGE C;

CREATE OR REPLACE FUNCTION median_agg_finalfn(internal)
RETURNS numeric
AS 'sandbox', 'median_agg_finalfn'
LANGUAGE C;

-- 创建聚集函数（计算中位数）
CREATE AGGREGATE median_agg(numeric) (
  SFUNC = median_agg_transfn,
  STYPE = internal,
  FINALFUNC = median_agg_finalfn
);

-- PL/pgSQL

-- 创建一个状态转换函数
DROP FUNCTION IF EXISTS array_append_unique(anyarray, anyelement);
CREATE OR REPLACE FUNCTION array_append_unique(anyarray, anyelement)
RETURNS anyarray AS $$
BEGIN
    IF $2 IS NULL THEN
        RETURN $1;
    END IF;
    IF NOT $2 = ANY($1) THEN
        RETURN $1 || $2;
    END IF;
    RETURN $1;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- 创建聚集函数
DROP AGGREGATE IF EXISTS unique_array(anyelement);
CREATE AGGREGATE unique_array(anyelement) (
    SFUNC = array_append_unique,
    STYPE = anyarray,
    INITCOND = '{}'
);
