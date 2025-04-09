#include <stdio.h>

#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/varlena.h"
#include "catalog/pg_collation_d.h"

PG_MODULE_MAGIC;

/**********************************
 * complex type
 **********************************/

typedef struct Complex {
    double x;
    double y;
} Complex;

PG_FUNCTION_INFO_V1(complex_in);
PG_FUNCTION_INFO_V1(complex_out);

/* 输入函数：把字符串转为 Complex 结构体 */
Datum complex_in(PG_FUNCTION_ARGS) {
    char *str = PG_GETARG_CSTRING(0);
    Complex *result = (Complex *)palloc(sizeof(Complex));

    if (sscanf(str, "%lf+%lfi", &result->x, &result->y) != 2)
        ereport(ERROR, (errmsg("invalid input syntax for complex: \"%s\"", str)));

    PG_RETURN_POINTER(result);
}

/* 输出函数：把 Complex 转为字符串 */
Datum complex_out(PG_FUNCTION_ARGS) {
    Complex *complex = (Complex *)PG_GETARG_POINTER(0);
    char *result = psprintf("%.6g+%.6gi", complex->x, complex->y);
    PG_RETURN_CSTRING(result);
}

PG_FUNCTION_INFO_V1(complex_op_lt);
PG_FUNCTION_INFO_V1(complex_op_le);
PG_FUNCTION_INFO_V1(complex_op_eq);
PG_FUNCTION_INFO_V1(complex_op_ge);
PG_FUNCTION_INFO_V1(complex_op_gt);

// 小于操作符
Datum complex_op_lt(PG_FUNCTION_ARGS) {
    Complex *a = (Complex *)PG_GETARG_POINTER(0);
    Complex *b = (Complex *)PG_GETARG_POINTER(1);
    bool result = (a->x < b->x) || (a->x == b->x && a->y < b->y);
    return BoolGetDatum(result);
}

// 小于等于操作符
Datum complex_op_le(PG_FUNCTION_ARGS) {
    Complex *a = (Complex *)PG_GETARG_POINTER(0);
    Complex *b = (Complex *)PG_GETARG_POINTER(1);
    bool result = (a->x < b->x) || (a->x == b->x && a->y <= b->y);
    return BoolGetDatum(result);
}

// 等于操作符
Datum complex_op_eq(PG_FUNCTION_ARGS) {
    Complex *a = (Complex *)PG_GETARG_POINTER(0);
    Complex *b = (Complex *)PG_GETARG_POINTER(1);
    bool result = (a->x == b->x) && (a->y == b->y);
    return BoolGetDatum(result);
}

// 大于等于操作符
Datum complex_op_ge(PG_FUNCTION_ARGS) {
    Complex *a = (Complex *)PG_GETARG_POINTER(0);
    Complex *b = (Complex *)PG_GETARG_POINTER(1);
    bool result = (a->x > b->x) || (a->x == b->x && a->y >= b->y);
    return BoolGetDatum(result);
}

// 大于操作符
Datum complex_op_gt(PG_FUNCTION_ARGS) {
    Complex *a = (Complex *)PG_GETARG_POINTER(0);
    Complex *b = (Complex *)PG_GETARG_POINTER(1);
    bool result = (a->x > b->x) || (a->x == b->x && a->y > b->y);
    return BoolGetDatum(result);
}

// 比较函数
PG_FUNCTION_INFO_V1(complex_cmp);
Datum complex_cmp(PG_FUNCTION_ARGS) {
    Complex *a = (Complex *)PG_GETARG_POINTER(0);
    Complex *b = (Complex *)PG_GETARG_POINTER(1);
    bool result = (a->x < b->x) || (a->x == b->x && a->y < b->y);
    return Int32GetDatum(result);
}

// 复数相加函数
PG_FUNCTION_INFO_V1(complex_add);
Datum complex_add(PG_FUNCTION_ARGS) {
    // 获取输入参数
    Complex *a = (Complex *)PG_GETARG_POINTER(0);
    Complex *b = (Complex *)PG_GETARG_POINTER(1);

    // 创建结果复数
    Complex *result = palloc(sizeof(Complex));

    // 进行复数相加
    result->x = a->x + b->x;  // 实部相加
    result->y = a->y + b->y;  // 虚部相加

    // 返回结果
    PG_RETURN_POINTER(result);
}

/**********************************
 * mytext type
 **********************************/

// 定义比较类型的枚举
typedef enum {
    MYTEXT_COMPARE_LT = 1,  // 小于
    MYTEXT_COMPARE_LE,      // 小于等于
    MYTEXT_COMPARE_EQ,      // 等于
    MYTEXT_COMPARE_GT,      // 大于
    MYTEXT_COMPARE_GE       // 大于等于
} MyTextCompareType;

PG_FUNCTION_INFO_V1(mytext_in);
PG_FUNCTION_INFO_V1(mytext_out);
Datum mytext_in(PG_FUNCTION_ARGS) {
    char *str = PG_GETARG_CSTRING(0);
    int len = strlen(str);

    // 分配 varlena 空间（长度+实际字符串）
    struct varlena *result = (struct varlena *)palloc(len + VARHDRSZ);

    // 设置长度头部
    SET_VARSIZE(result, len + VARHDRSZ);

    // 拷贝数据
    memcpy(VARDATA(result), str, len);

    PG_RETURN_POINTER(result);
}

Datum mytext_out(PG_FUNCTION_ARGS) {
    struct varlena *mytxt = PG_GETARG_VARLENA_P(0);

    // 确保是非压缩版本
    mytxt = pg_detoast_datum(mytxt);

    int len = VARSIZE(mytxt) - VARHDRSZ;
    char *result = (char *)palloc(len + 1);

    memcpy(result, VARDATA(mytxt), len);
    result[len] = '\0';

    PG_RETURN_CSTRING(result);
}

static int mytext_internel_cmp(struct varlena *src, struct varlena *dst, Oid collid) {
    int s_len = VARSIZE_ANY_EXHDR(src);
    char *s_data = VARDATA_ANY(src);
    int d_len = VARSIZE_ANY_EXHDR(dst);
    char *d_data = VARDATA_ANY(dst);

    return varstr_cmp(s_data, s_len, d_data, d_len, collid);
}

PG_FUNCTION_INFO_V1(mytext_cmp);
Datum mytext_cmp(PG_FUNCTION_ARGS) {
    struct varlena *a = (struct varlena *)PG_GETARG_VARLENA_P(0);
    struct varlena *b = (struct varlena *)PG_GETARG_VARLENA_P(1);

    // 正确地使用 fcinfo
    Oid collation = PG_GET_COLLATION();
    if (collation == InvalidOid) {
        collation = DEFAULT_COLLATION_OID;  // 为 text 类型指定默认的排序规则
    }

    // 假设 mytext 是字符串类型，这里进行简单的字符串比较
    int result = mytext_internel_cmp(a, b, collation);

    PG_FREE_IF_COPY(a, 0);
    PG_FREE_IF_COPY(b, 1);

    return Int32GetDatum(result);
}

// 辅助函数用于比较
static bool mytext_compare(PG_FUNCTION_ARGS, MyTextCompareType comparison_type) {
  struct varlena *a = (struct varlena *)PG_GETARG_VARLENA_P(0);
  struct varlena *b = (struct varlena *)PG_GETARG_VARLENA_P(1);

    Oid collation = PG_GET_COLLATION();
    if (collation == InvalidOid) {
        collation = DEFAULT_COLLATION_OID;  // 为 text 类型指定默认的排序规则
    }
    int result = mytext_internel_cmp(a, b, collation);

    PG_FREE_IF_COPY(a, 0);
    PG_FREE_IF_COPY(b, 1);

    // 根据 comparison_type 返回布尔值
    switch (comparison_type) {
        case MYTEXT_COMPARE_LT:
        return result < 0;
        case MYTEXT_COMPARE_LE:
        return result <= 0;
        case MYTEXT_COMPARE_EQ:
        return result == 0;
        case MYTEXT_COMPARE_GT:
        return result > 0;
        case MYTEXT_COMPARE_GE:
        return result >= 0;
        default:
        return false;  // 默认返回 false
    }
}

PG_FUNCTION_INFO_V1(mytext_op_lt);
PG_FUNCTION_INFO_V1(mytext_op_le);
PG_FUNCTION_INFO_V1(mytext_op_eq);
PG_FUNCTION_INFO_V1(mytext_op_gt);
PG_FUNCTION_INFO_V1(mytext_op_ge);

Datum mytext_op_lt(PG_FUNCTION_ARGS) {
    bool result = mytext_compare(fcinfo, MYTEXT_COMPARE_LT);
    PG_RETURN_BOOL(result);
}

Datum mytext_op_le(PG_FUNCTION_ARGS) {
    bool result = mytext_compare(fcinfo, MYTEXT_COMPARE_LE);
    PG_RETURN_BOOL(result);
}

Datum mytext_op_eq(PG_FUNCTION_ARGS) {
    bool result = mytext_compare(fcinfo, MYTEXT_COMPARE_EQ);
    PG_RETURN_BOOL(result);
}

Datum mytext_op_gt(PG_FUNCTION_ARGS) {
    bool result = mytext_compare(fcinfo, MYTEXT_COMPARE_GT);
    PG_RETURN_BOOL(result);
}

Datum mytext_op_ge(PG_FUNCTION_ARGS) {
    bool result = mytext_compare(fcinfo, MYTEXT_COMPARE_GE);
    PG_RETURN_BOOL(result);
}
