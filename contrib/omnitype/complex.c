#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"

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
