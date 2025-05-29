#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/sortsupport.h"
#include "access/hash.h"
#include "libpq/pqformat.h"

/* 类型定义 */
typedef int32 tinyint;

/* 输入函数：字符串 -> tinyint */
PG_FUNCTION_INFO_V1(tinyint_in);
Datum
tinyint_in(PG_FUNCTION_ARGS)
{
    char *str = PG_GETARG_CSTRING(0);
    tinyint val;
    char *endptr;

    val = strtoint(str, &endptr, 10);
    if (*endptr != '\0' || errno == ERANGE)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for tinyint: \"%s\"", str)));

    /* 范围检查 */
    if (val < -128 || val > 127)
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("value %d is out of range for tinyint", val),
                 errdetail("Tinyint values must be between -128 and 127.")));

    PG_RETURN_INT32(val);
}

/* 输出函数：TinyInt -> 字符串 */
PG_FUNCTION_INFO_V1(tinyint_out);
Datum
tinyint_out(PG_FUNCTION_ARGS)
{
    tinyint val = PG_GETARG_INT32(0);
    char *result = (char *)palloc(5); // 最多4字符+结束符

    snprintf(result, 5, "%d", val);
    PG_RETURN_CSTRING(result);
}

/* 接收函数（二进制输入） */
PG_FUNCTION_INFO_V1(tinyint_recv);
Datum
tinyint_recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
    tinyint val = pq_getmsgint(buf, sizeof(tinyint));

    /* 二进制输入的范围检查 */
    if (val < -128 || val > 127)
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("binary value %d is out of range for tinyint", val)));

    PG_RETURN_INT32(val);
}

/* 发送函数（二进制输出） */
PG_FUNCTION_INFO_V1(tinyint_send);
Datum
tinyint_send(PG_FUNCTION_ARGS)
{
    tinyint val = PG_GETARG_INT32(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendint32(&buf, val);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/* ===== 比较函数 ===== */

/* tinyint 与 integer 的比较函数（正向） */

PG_FUNCTION_INFO_V1(tinyint_lt_integer);
Datum tinyint_lt_integer(PG_FUNCTION_ARGS) {
    tinyint a = PG_GETARG_INT32(0);
    int32 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a < b);
}

PG_FUNCTION_INFO_V1(tinyint_le_integer);
Datum tinyint_le_integer(PG_FUNCTION_ARGS) {
    tinyint a = PG_GETARG_INT32(0);
    int32 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a <= b);
}

PG_FUNCTION_INFO_V1(tinyint_eq_integer);
Datum tinyint_eq_integer(PG_FUNCTION_ARGS) {
    tinyint a = PG_GETARG_INT32(0);
    int32 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a == b);
}

PG_FUNCTION_INFO_V1(tinyint_ne_integer);
Datum tinyint_ne_integer(PG_FUNCTION_ARGS) {
    tinyint a = PG_GETARG_INT32(0);
    int32 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a != b);
}

PG_FUNCTION_INFO_V1(tinyint_ge_integer);
Datum tinyint_ge_integer(PG_FUNCTION_ARGS) {
    tinyint a = PG_GETARG_INT32(0);
    int32 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a >= b);
}

PG_FUNCTION_INFO_V1(tinyint_gt_integer);
Datum tinyint_gt_integer(PG_FUNCTION_ARGS) {
    tinyint a = PG_GETARG_INT32(0);
    int32 b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a > b);
}

/* 比较函数（用于BTree排序） */
PG_FUNCTION_INFO_V1(tinyint_cmp_integer);
Datum
tinyint_cmp_integer(PG_FUNCTION_ARGS)
{
    tinyint a = PG_GETARG_INT32(0);
    int32 b = PG_GETARG_INT32(1);

    /* 范围检查确保 b 在 tinyint 范围内 */
    if (b < -128 || b > 127) {
        elog(ERROR, "integer value %d is out of range for tinyint", b);
    }

    int32 result = (a < b ? -1: (a > b ? 1 : 0));
    PG_RETURN_INT32(result);
}

/* integer 与 tinyint 的比较函数（反向） */

PG_FUNCTION_INFO_V1(integer_lt_tinyint);
Datum integer_lt_tinyint(PG_FUNCTION_ARGS) {
    int32 a = PG_GETARG_INT32(0);
    tinyint b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a < b);
}

PG_FUNCTION_INFO_V1(integer_le_tinyint);
Datum integer_le_tinyint(PG_FUNCTION_ARGS) {
    int32 a = PG_GETARG_INT32(0);
    tinyint b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a <= b);
}

PG_FUNCTION_INFO_V1(integer_eq_tinyint);
Datum integer_eq_tinyint(PG_FUNCTION_ARGS) {
    int32 a = PG_GETARG_INT32(0);
    tinyint b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a == b);
}

PG_FUNCTION_INFO_V1(integer_ne_tinyint);
Datum integer_ne_tinyint(PG_FUNCTION_ARGS) {
    int32 a = PG_GETARG_INT32(0);
    tinyint b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a != b);
}

PG_FUNCTION_INFO_V1(integer_ge_tinyint);
Datum integer_ge_tinyint(PG_FUNCTION_ARGS) {
    int32 a = PG_GETARG_INT32(0);
    tinyint b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a >= b);
}

PG_FUNCTION_INFO_V1(integer_gt_tinyint);
Datum integer_gt_tinyint(PG_FUNCTION_ARGS) {
    int32 a = PG_GETARG_INT32(0);
    tinyint b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a > b);
}

/* 比较函数（用于BTree排序） */
PG_FUNCTION_INFO_V1(integer_cmp_tinyint);
Datum
integer_cmp_tinyint(PG_FUNCTION_ARGS)
{
    int32 a = PG_GETARG_INT32(0);
    tinyint b = PG_GETARG_INT32(1);

    /* 范围检查确保 b 在 tinyint 范围内 */
    if (b < -128 || b > 127) {
        elog(ERROR, "integer value %d is out of range for tinyint", b);
    }

    int32 result = (a < b ? -1: (a > b ? 1 : 0));
    PG_RETURN_INT32(result);
}

/* tinyint 的比较函数 */

PG_FUNCTION_INFO_V1(tinyint_lt);
Datum tinyint_lt(PG_FUNCTION_ARGS) {
    tinyint a = PG_GETARG_INT32(0);
    tinyint b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a < b);
}

PG_FUNCTION_INFO_V1(tinyint_le);
Datum tinyint_le(PG_FUNCTION_ARGS) {
    tinyint a = PG_GETARG_INT32(0);
    tinyint b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a <= b);
}

PG_FUNCTION_INFO_V1(tinyint_eq);
Datum tinyint_eq(PG_FUNCTION_ARGS) {
    tinyint a = PG_GETARG_INT32(0);
    tinyint b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a == b);
}

PG_FUNCTION_INFO_V1(tinyint_ne);
Datum tinyint_ne(PG_FUNCTION_ARGS) {
    tinyint a = PG_GETARG_INT32(0);
    tinyint b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a != b);
}

PG_FUNCTION_INFO_V1(tinyint_ge);
Datum tinyint_ge(PG_FUNCTION_ARGS) {
    tinyint a = PG_GETARG_INT32(0);
    tinyint b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a >= b);
}

PG_FUNCTION_INFO_V1(tinyint_gt);
Datum tinyint_gt(PG_FUNCTION_ARGS) {
    tinyint a = PG_GETARG_INT32(0);
    tinyint b = PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a > b);
}

/* 比较函数（用于BTree排序） */
PG_FUNCTION_INFO_V1(tinyint_cmp);
Datum
tinyint_cmp(PG_FUNCTION_ARGS)
{
    tinyint a = PG_GETARG_INT32(0);
    tinyint b = PG_GETARG_INT32(1);

    /* 范围检查确保 b 在 tinyint 范围内 */
    if (b < -128 || b > 127) {
        elog(ERROR, "integer value %d is out of range for tinyint", b);
    }

    int32 result = (a < b ? -1: (a > b ? 1 : 0));
    PG_RETURN_INT32(result);
}

/* 哈希函数（用于Hash索引） */
PG_FUNCTION_INFO_V1(tinyint_hash);
Datum
tinyint_hash(PG_FUNCTION_ARGS)
{
    /* 直接使用int4哈希函数（兼容） */
    return hashint4(fcinfo);
}

/* int4 -> tinyint 转换函数 */
PG_FUNCTION_INFO_V1(int4_to_tinyint);
Datum
int4_to_tinyint(PG_FUNCTION_ARGS)
{
    tinyint val = PG_GETARG_INT32(0);

    if (val < -128 || val > 127)
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("integer out of range for tinyint")));

    PG_RETURN_INT32(val);
}