#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/sortsupport.h"
#include "access/hash.h"
#include "common/string.h"
#include "libpq/pqformat.h"

/* 类型定义 */
// 由于本文件中的 tinyint 实现依赖 (tinyint, tinyint)、(tinyint, integer)、
// (integer, tinyint) 三种比较运算符，因此，类型名中包含 triple 字样。
typedef int32 tinyint_v2;

/* 输入函数：字符串 -> tinyint_v2 */
PG_FUNCTION_INFO_V1(tinyint_v2_in);
Datum
tinyint_v2_in(PG_FUNCTION_ARGS)
{
    char *str = PG_GETARG_CSTRING(0);
    tinyint_v2 val;
    char *endptr;

    val = strtoint(str, &endptr, 10);
    if (*endptr != '\0' || errno == ERANGE)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for tinyint_v2: \"%s\"", str)));

    /* 范围检查 */
    if (val < -128 || val > 127)
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("value %d is out of range for tinyint_v2", val),
                 errdetail("Tinyint values must be between -128 and 127.")));

    PG_RETURN_INT32(val);
}

/* 输出函数：TinyInt -> 字符串 */
PG_FUNCTION_INFO_V1(tinyint_v2_out);
Datum
tinyint_v2_out(PG_FUNCTION_ARGS)
{
    tinyint_v2 val = PG_GETARG_INT32(0);
    char *result = (char *)palloc(5); // 最多4字符+结束符

    snprintf(result, 5, "%d", val);
    PG_RETURN_CSTRING(result);
}

/* 接收函数（二进制输入） */
PG_FUNCTION_INFO_V1(tinyint_v2_recv);
Datum
tinyint_v2_recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
    tinyint_v2 val = pq_getmsgint(buf, sizeof(tinyint_v2));

    /* 二进制输入的范围检查 */
    if (val < -128 || val > 127)
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("binary value %d is out of range for tinyint_v2", val)));

    PG_RETURN_INT32(val);
}

/* 发送函数（二进制输出） */
PG_FUNCTION_INFO_V1(tinyint_v2_send);
Datum
tinyint_v2_send(PG_FUNCTION_ARGS)
{
    tinyint_v2 val = PG_GETARG_INT32(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendint32(&buf, val);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/* ===== 比较函数 ===== */

/* tinyint_v2 与 integer 的比较函数（正向） */

PG_FUNCTION_INFO_V1(tinyint_v2_lt_integer);
Datum tinyint_v2_lt_integer(PG_FUNCTION_ARGS) {
    tinyint_v2 a = PG_GETARG_INT32(0);
    int32 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a < b);
}

PG_FUNCTION_INFO_V1(tinyint_v2_le_integer);
Datum tinyint_v2_le_integer(PG_FUNCTION_ARGS) {
    tinyint_v2 a = PG_GETARG_INT32(0);
    int32 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a <= b);
}

PG_FUNCTION_INFO_V1(tinyint_v2_eq_integer);
Datum tinyint_v2_eq_integer(PG_FUNCTION_ARGS) {
    tinyint_v2 a = PG_GETARG_INT32(0);
    int32 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a == b);
}

PG_FUNCTION_INFO_V1(tinyint_v2_ne_integer);
Datum tinyint_v2_ne_integer(PG_FUNCTION_ARGS) {
    tinyint_v2 a = PG_GETARG_INT32(0);
    int32 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a != b);
}

PG_FUNCTION_INFO_V1(tinyint_v2_ge_integer);
Datum tinyint_v2_ge_integer(PG_FUNCTION_ARGS) {
    tinyint_v2 a = PG_GETARG_INT32(0);
    int32 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a >= b);
}

PG_FUNCTION_INFO_V1(tinyint_v2_gt_integer);
Datum tinyint_v2_gt_integer(PG_FUNCTION_ARGS) {
    tinyint_v2 a = PG_GETARG_INT32(0);
    int32 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a > b);
}

/* 比较函数（用于BTree排序） */
PG_FUNCTION_INFO_V1(tinyint_v2_cmp_integer);
Datum
tinyint_v2_cmp_integer(PG_FUNCTION_ARGS)
{
    tinyint_v2 a = PG_GETARG_INT32(0);
    int32 b = PG_GETARG_INT32(1);

    /* 范围检查确保 b 在 tinyint_v2 范围内 */
    if (b < -128 || b > 127) {
        elog(ERROR, "integer value %d is out of range for tinyint_v2", b);
    }

    int32 result = (a < b ? -1: (a > b ? 1 : 0));
    PG_RETURN_INT32(result);
}

/* integer 与 tinyint_v2 的比较函数（反向） */

PG_FUNCTION_INFO_V1(integer_lt_tinyint_v2);
Datum integer_lt_tinyint_v2(PG_FUNCTION_ARGS) {
    int32 a = PG_GETARG_INT32(0);
    tinyint_v2 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a < b);
}

PG_FUNCTION_INFO_V1(integer_le_tinyint_v2);
Datum integer_le_tinyint_v2(PG_FUNCTION_ARGS) {
    int32 a = PG_GETARG_INT32(0);
    tinyint_v2 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a <= b);
}

PG_FUNCTION_INFO_V1(integer_eq_tinyint_v2);
Datum integer_eq_tinyint_v2(PG_FUNCTION_ARGS) {
    int32 a = PG_GETARG_INT32(0);
    tinyint_v2 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a == b);
}

PG_FUNCTION_INFO_V1(integer_ne_tinyint_v2);
Datum integer_ne_tinyint_v2(PG_FUNCTION_ARGS) {
    int32 a = PG_GETARG_INT32(0);
    tinyint_v2 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a != b);
}

PG_FUNCTION_INFO_V1(integer_ge_tinyint_v2);
Datum integer_ge_tinyint_v2(PG_FUNCTION_ARGS) {
    int32 a = PG_GETARG_INT32(0);
    tinyint_v2 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a >= b);
}

PG_FUNCTION_INFO_V1(integer_gt_tinyint_v2);
Datum integer_gt_tinyint_v2(PG_FUNCTION_ARGS) {
    int32 a = PG_GETARG_INT32(0);
    tinyint_v2 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a > b);
}

/* 比较函数（用于BTree排序） */
PG_FUNCTION_INFO_V1(integer_cmp_tinyint_v2);
Datum
integer_cmp_tinyint_v2(PG_FUNCTION_ARGS)
{
    int32 a = PG_GETARG_INT32(0);
    tinyint_v2 b = PG_GETARG_INT32(1);

    /* 范围检查确保 b 在 tinyint_v2 范围内 */
    if (b < -128 || b > 127) {
        elog(ERROR, "integer value %d is out of range for tinyint_v2", b);
    }

    int32 result = (a < b ? -1: (a > b ? 1 : 0));
    PG_RETURN_INT32(result);
}

/* tinyint_v2 的比较函数 */

PG_FUNCTION_INFO_V1(tinyint_v2_lt);
Datum tinyint_v2_lt(PG_FUNCTION_ARGS) {
    tinyint_v2 a = PG_GETARG_INT32(0);
    tinyint_v2 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a < b);
}

PG_FUNCTION_INFO_V1(tinyint_v2_le);
Datum tinyint_v2_le(PG_FUNCTION_ARGS) {
    tinyint_v2 a = PG_GETARG_INT32(0);
    tinyint_v2 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a <= b);
}

PG_FUNCTION_INFO_V1(tinyint_v2_eq);
Datum tinyint_v2_eq(PG_FUNCTION_ARGS) {
    tinyint_v2 a = PG_GETARG_INT32(0);
    tinyint_v2 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a == b);
}

PG_FUNCTION_INFO_V1(tinyint_v2_ne);
Datum tinyint_v2_ne(PG_FUNCTION_ARGS) {
    tinyint_v2 a = PG_GETARG_INT32(0);
    tinyint_v2 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a != b);
}

PG_FUNCTION_INFO_V1(tinyint_v2_ge);
Datum tinyint_v2_ge(PG_FUNCTION_ARGS) {
    tinyint_v2 a = PG_GETARG_INT32(0);
    tinyint_v2 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a >= b);
}

PG_FUNCTION_INFO_V1(tinyint_v2_gt);
Datum tinyint_v2_gt(PG_FUNCTION_ARGS) {
    tinyint_v2 a = PG_GETARG_INT32(0);
    tinyint_v2 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a > b);
}

/* 比较函数（用于BTree排序） */
PG_FUNCTION_INFO_V1(tinyint_v2_cmp);
Datum
tinyint_v2_cmp(PG_FUNCTION_ARGS)
{
    tinyint_v2 a = PG_GETARG_INT32(0);
    tinyint_v2 b = PG_GETARG_INT32(1);

    /* 范围检查确保 b 在 tinyint_v2 范围内 */
    if (b < -128 || b > 127) {
        elog(ERROR, "integer value %d is out of range for tinyint_v2", b);
    }

    int32 result = (a < b ? -1: (a > b ? 1 : 0));
    PG_RETURN_INT32(result);
}

/* 哈希函数（用于Hash索引） */
PG_FUNCTION_INFO_V1(tinyint_v2_hash);
Datum
tinyint_v2_hash(PG_FUNCTION_ARGS)
{
    /* 直接使用int4哈希函数（兼容） */
    return hashint4(fcinfo);
}

/* int4 -> tinyint_v2 转换函数 */
PG_FUNCTION_INFO_V1(int4_to_tinyint_v2);
Datum
int4_to_tinyint_v2(PG_FUNCTION_ARGS)
{
    tinyint_v2 val = PG_GETARG_INT32(0);

    if (val < -128 || val > 127)
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("integer out of range for tinyint_v2")));

    PG_RETURN_INT32(val);
}