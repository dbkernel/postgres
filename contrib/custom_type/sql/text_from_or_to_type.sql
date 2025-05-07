-- ==================== 测试文本类型与任意数据类型的互相转换 ====================

---------------------------
-- 基础数值类型
---------------------------
-- 浮点数转换（支持科学计数法）
SELECT text_to_type('6.022e23', NULL::float8)::float8 AS avogadro,
       type_to_text(9.80665::float4) AS gravity_text;  -- 返回"9.80665"

-- 整型边界值测试
SELECT text_to_type('2147483647', NULL::int4)::int4 AS max_int4,
       text_to_type('9223372036854775807', NULL::int8)::int8 AS max_int8;

---------------------------
-- 字符串类型
---------------------------
-- 定长字符处理（自动填充空格）
SELECT text_to_type('abc', NULL::char(5))::char(5) AS padded_char,  -- 存储为'abc  '
       type_to_text('xyz  '::char(5)) AS trimmed_char_text;  -- 返回"xyz"

-- Unicode字符处理
SELECT text_to_type('🍜米饭', NULL::varchar)::varchar AS mifan,
       type_to_text('中国智造'::text) AS chinese_text;

---------------------------
-- 二进制数据
---------------------------
-- 二进制转换（十六进制和转义格式）
SELECT text_to_type(E'\\x48656C6C6F', NULL::bytea)::bytea AS hex_bytea,  -- Hello的hex
       text_to_type(E'\\000\\001\\127', NULL::bytea)::bytea AS escape_bytea;

-- 二进制往返验证
WITH bin_data AS (
  SELECT E'\\xDEADBEEF'::bytea AS original
)
SELECT original, 
       text_to_type(type_to_text(original), NULL::bytea)::bytea AS converted
FROM bin_data;

---------------------------
-- JSON/XML
---------------------------
-- 复杂JSON转换
SELECT text_to_type(
  '{"name":"Alice","age":30,"tags":["staff","admin"],"meta":{"id":123}}',
  NULL::jsonb
)::jsonb AS complex_json;

-- XML命名空间处理
SELECT text_to_type(
  '<svg xmlns="http://www.w3.org/2000/svg" width="100" height="100"></svg>',
  NULL::xml
)::xml AS svg_xml;

---------------------------
-- 时间类型
---------------------------
-- 时区敏感转换
SELECT text_to_type('2023-12-25 15:30:00+08', NULL::timestamptz)::timestamptz AS beijing_time,
       type_to_text(current_timestamp) AS current_ts_text;

-- 时间间隔转换
SELECT text_to_type('3 days 12:30:45', NULL::interval)::interval AS custom_interval,
       type_to_text(interval '1 month 3 days') AS interval_text;  -- 返回"1 mon 3 days"

---------------------------
-- 特殊类型
---------------------------
-- 几何类型（需PostGIS）
SELECT text_to_type('POINT Z (1 2 3)', NULL::geometry)::geometry AS 3d_point,
       type_to_text(ST_MakeLine(ST_Point(0,0), ST_Point(1,1))) AS line_text;

-- 网络地址类型
SELECT text_to_type('2001:db8::1/128', NULL::inet)::inet AS ipv6_address,
       type_to_text('192.168.1.0/24'::cidr) AS cidr_text;

---------------------------
-- 复合类型
---------------------------
-- 嵌套类型转换（创建自定义类型）
CREATE TYPE address AS (
  street  varchar(50),
  city    varchar(20),
  zipcode char(6)
);

SELECT text_to_type(
  '("南京路", "上海市", "200001")',
  NULL::address
)::address AS shanghai_address;

---------------------------
-- 数组类型
---------------------------
-- 多维数组转换
SELECT text_to_type(
  '{{1,2},{3,4}}',
  NULL::int[][]
)::int[][] AS matrix;

-- JSON数组转换
SELECT text_to_type(
  '[{"name":"A"},{"name":"B"}]',
  NULL::json[]
)::json[] AS json_array;

---------------------------
-- 枚举类型
---------------------------
-- 枚举类型支持（创建颜色枚举）
CREATE TYPE color AS ENUM ('red', 'green', 'blue');
SELECT text_to_type('green', NULL::color)::color AS enum_val,
       type_to_text('blue'::color) AS enum_text;

---------------------------
-- 大对象处理
---------------------------
-- 大文本处理（10KB数据测试）
SELECT text_to_type(
  repeat('PostgreSQL ', 1000),
  NULL::text
)::text AS long_text;

---------------------------
-- 异常处理示例
---------------------------
-- 类型不匹配错误（预期行为）
SELECT text_to_type('not_a_number', NULL::int4);  -- 抛出invalid_text_representation

-- 格式验证（XML格式校验）
SELECT text_to_type('<root><unclosed>', NULL::xml);  -- 抛出invalid_xml_content