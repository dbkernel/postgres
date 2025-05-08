/* contrib/omnitype/omnitype--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION omnitype" to load this file. \quit

---------------- 创建文本与任意类型的互相转换函数 ----------------

CREATE OR REPLACE FUNCTION text_to_type(text, anyelement)
RETURNS anyelement
AS 'omnitype', 'text_to_type'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION type_to_text(anyelement)
RETURNS text
AS 'omnitype', 'type_to_text'
LANGUAGE C STRICT;

---------------- 创建自定义操作符 ----------------

\echo Use "Create @@ Operator"

-- 注册函数
-- 写法一：效率低
-- CREATE FUNCTION abs_diff(numeric, numeric) RETURNS numeric
--     AS 'SELECT abs($1 -$2);'
--     LANGUAGE SQL;
-- 写法二：效率高
CREATE FUNCTION abs_diff(numeric, numeric) RETURNS numeric AS $$
BEGIN
    RETURN abs($1 - $2);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- 创建一个名为 @@ 的操作符，用于计算两个数值的差的绝对值
CREATE OPERATOR @@ (
    LEFTARG = numeric,
    RIGHTARG = numeric,
    PROCEDURE = abs_diff,
    COMMUTATOR = @@
);

-- 删除自定义操作符（在 DROP EXTENSION 时会自动删除，无法手动删除，此处仅做记录）
-- DROP OPERATOR @@ (numeric, numeric);
-- DROP FUNCTION abs_diff(numeric, numeric);

---------------- 创建自定义复数类型 complex、操作符及索引 ----------------

\echo Use "Create Complex Data Type"

-- 注册 C 函数
CREATE FUNCTION complex_in(cstring) RETURNS complex
    AS 'omnitype', 'complex_in'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION complex_out(complex) RETURNS cstring
    AS 'omnitype', 'complex_out'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION complex_add(complex, complex) RETURNS complex
    AS 'omnitype', 'complex_add'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION complex_op_lt(complex, complex) RETURNS boolean
    AS 'omnitype', 'complex_op_lt'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION complex_op_le(complex, complex) RETURNS boolean
    AS 'omnitype', 'complex_op_le'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION complex_op_ge(complex, complex) RETURNS boolean
    AS 'omnitype', 'complex_op_ge'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION complex_op_eq(complex, complex) RETURNS boolean
    AS 'omnitype', 'complex_op_eq'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION complex_op_gt(complex, complex) RETURNS boolean
    AS 'omnitype', 'complex_op_gt'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION complex_cmp(complex, complex) RETURNS integer
    AS 'omnitype', 'complex_cmp'
    LANGUAGE C IMMUTABLE STRICT;

-- 定义类型 complex
CREATE TYPE complex (
    INPUT = complex_in,
    OUTPUT = complex_out,
    INTERNALLENGTH = 16,
    PASSEDBYVALUE = false
);

-- 创建复数类型的操作符

CREATE OPERATOR < (
    LEFTARG = complex,
    RIGHTARG = complex,
    PROCEDURE = complex_op_lt,
    COMMUTATOR = >, -- 指定了操作符的对称操作符
    NEGATOR = >=, -- 指定了操作符的相反（否定）操作符
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel
);

CREATE OPERATOR <= (
    LEFTARG = complex,
    RIGHTARG = complex,
    PROCEDURE = complex_op_le,
    COMMUTATOR = >=,
    NEGATOR = >,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel
);

CREATE OPERATOR = (
    LEFTARG = complex,
    RIGHTARG = complex,
    PROCEDURE = complex_op_eq,
    COMMUTATOR = =,
    NEGATOR = <>,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);

CREATE OPERATOR >= (
    LEFTARG = complex,
    RIGHTARG = complex,
    PROCEDURE = complex_op_ge,
    COMMUTATOR = <=,
    NEGATOR = <,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel
);

CREATE OPERATOR > (
    LEFTARG = complex,
    RIGHTARG = complex,
    PROCEDURE = complex_op_gt,
    COMMUTATOR = <,
    NEGATOR = <=,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);

-- 创建复数类型的操作符类
CREATE OPERATOR CLASS complex_ops FOR TYPE complex USING btree AS
    OPERATOR 1 <, -- 定义操作符，其对应的函数返回值必须是 bool 类型
    OPERATOR 2 <=,
    OPERATOR 3 =,
    OPERATOR 4 >=,
    OPERATOR 5 >,
    FUNCTION 1 complex_cmp(complex, complex); -- 定义比较函数，其返回值必须是 int 类型

-- 删除 complex 类型相关对象（在 DROP EXTENSION 时会自动删除，无法手动删除，此处仅做记录）
-- DROP OPERATOR < (complex, complex);
-- DROP OPERATOR <= (complex, complex);
-- DROP OPERATOR = (complex, complex);
-- DROP OPERATOR >= (complex, complex);
-- DROP OPERATOR > (complex, complex);
-- DROP OPERATOR CLASS complex_ops USING btree;
-- DROP FUNCTION complex_in(cstring);
-- DROP FUNCTION complex_out(complex);
-- DROP FUNCTION complex_add(complex, complex);
-- DROP FUNCTION complex_op_lt(complex, complex);
-- DROP FUNCTION complex_op_le(complex, complex);
-- DROP FUNCTION complex_op_ge(complex, complex);
-- DROP FUNCTION complex_op_eq(complex, complex);
-- DROP FUNCTION complex_op_gt(complex, complex);
-- DROP FUNCTION complex_cmp(complex, complex);
-- DROP TYPE complex;

---------------- 创建自定义枚举类型 mood ----------------

-- 枚举类型 mood
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');

---------------- 创建自定义字符串类型 mytext、操作符及索引 ----------------

\echo Use "Create Mytext Data Type"

CREATE FUNCTION mytext_in(cstring) RETURNS mytext
    AS 'omnitype', 'mytext_in'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION mytext_out(mytext) RETURNS cstring
    AS 'omnitype', 'mytext_out'
    LANGUAGE C IMMUTABLE STRICT;

-- extended 支持 TOAST 存储可能导致失效, plain支持varchar, COLLATABLE = true 设置排序规则为默认排序规则
CREATE TYPE mytext (
    INPUT = mytext_in,
    OUTPUT = mytext_out,
    INTERNALLENGTH = VARIABLE,
    STORAGE = plain,
    COLLATABLE = true
);

-- 注册 mytext_cmp 函数
CREATE FUNCTION mytext_cmp(mytext, mytext) RETURNS integer
    AS 'omnitype', 'mytext_cmp'
    LANGUAGE C STRICT;

-- 注册操作符所用函数，返回值必须为 bool 类型
CREATE FUNCTION mytext_op_le(mytext, mytext) RETURNS boolean
    AS 'omnitype', 'mytext_op_le'
    LANGUAGE C STRICT;
 
CREATE FUNCTION mytext_op_lt(mytext, mytext) RETURNS boolean
    AS 'omnitype', 'mytext_op_lt'
    LANGUAGE C STRICT;

CREATE FUNCTION mytext_op_eq(mytext, mytext) RETURNS boolean
    AS 'omnitype', 'mytext_op_eq'
    LANGUAGE C STRICT;

CREATE FUNCTION mytext_op_gt(mytext, mytext) RETURNS boolean
    AS 'omnitype', 'mytext_op_gt'
    LANGUAGE C STRICT;

CREATE FUNCTION mytext_op_ge(mytext, mytext) RETURNS boolean
    AS 'omnitype', 'mytext_op_ge'
    LANGUAGE C STRICT;

-- 创建操作符
CREATE OPERATOR < (
    LEFTARG = mytext,
    RIGHTARG = mytext,
    PROCEDURE = mytext_op_lt,
    COMMUTATOR = >,
    NEGATOR = >=,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);

CREATE OPERATOR <= (
    LEFTARG = mytext,
    RIGHTARG = mytext,
    PROCEDURE = mytext_op_le,
    COMMUTATOR = >=,
    NEGATOR = >,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel
);

CREATE OPERATOR = (
    LEFTARG = mytext,
    RIGHTARG = mytext,
    PROCEDURE = mytext_op_eq,
    COMMUTATOR = =,
    NEGATOR = <>,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);

CREATE OPERATOR >= (
    LEFTARG = mytext,
    RIGHTARG = mytext,
    PROCEDURE = mytext_op_ge,
    COMMUTATOR = <=,
    NEGATOR = <,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel
);

CREATE OPERATOR > (
    LEFTARG = mytext,
    RIGHTARG = mytext,
    PROCEDURE = mytext_op_gt,
    COMMUTATOR = <,
    NEGATOR = <=,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);

-- 创建操作符族
CREATE OPERATOR CLASS mytext_ops default FOR TYPE mytext USING btree AS
    OPERATOR 1 < (mytext, mytext), -- 操作符与序号严格对应，比如 1 只能关联 < 操作符
    OPERATOR 2 <= (mytext, mytext), -- 定义操作符，其对应的函数返回值必须是 bool 类型
    OPERATOR 3 = (mytext, mytext),
    OPERATOR 4 >= (mytext, mytext),
    OPERATOR 5 > (mytext, mytext),
    FUNCTION 1 mytext_cmp(mytext, mytext); -- 定义比较函数，其返回值必须是 int 类型

-- 删除 mytext 类型相关对象（在 DROP EXTENSION 时会自动删除，无法手动删除，此处仅做记录）
-- DROP OPERATOR < (mytext, mytext);
-- DROP OPERATOR <= (mytext, mytext);
-- DROP OPERATOR = (mytext, mytext);
-- DROP OPERATOR >= (mytext, mytext);
-- DROP OPERATOR > (mytext, mytext);
-- DROP OPERATOR CLASS mytext_ops USING btree;
-- DROP FUNCTION mytext_in(cstring);
-- DROP FUNCTION mytext_out(mytext);
-- DROP FUNCTION mytext_cmp(mytext, mytext);
-- DROP FUNCTION mytext_op_le(mytext, mytext);
-- DROP FUNCTION mytext_op_lt(mytext, mytext);
-- DROP FUNCTION mytext_op_eq(mytext, mytext);
-- DROP FUNCTION mytext_op_gt(mytext, mytext);
-- DROP FUNCTION mytext_op_ge(mytext, mytext);
-- DROP TYPE mytext;


---------------- 创建自定义复合类型 composite、操作符及索引 ----------------

CREATE FUNCTION composite_in(cstring)
RETURNS composite
AS 'omnitype', 'composite_in'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION composite_out(composite)
RETURNS cstring
AS 'omnitype', 'composite_out'
LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE composite (
    INPUT = composite_in,
    OUTPUT = composite_out,
    INTERNALLENGTH = VARIABLE,
    STORAGE = EXTENDED
);
