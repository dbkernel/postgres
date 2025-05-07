#include <stdio.h>

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"      // 包含 NodeTag 类型定义
#include "nodes/pg_list.h"    // 包含 List 和 String 类型定义
#include "nodes/parsenodes.h" // 包含 String 结构体定义
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/jsonb.h"
#include "utils/varlena.h"
#include "utils/xml.h"
#include "utils/regproc.h"
#include "utils/lsyscache.h"
#include "catalog/pg_collation_d.h"

// #include "utils/varchar.h"
// #include "utils/bpchar.h"

PG_MODULE_MAGIC;

/*--------------------------------*
 *  文本转任意类型的通用转换函数  *
 *--------------------------------*/
PG_FUNCTION_INFO_V1(text_to_type);
Datum
text_to_type(PG_FUNCTION_ARGS)
{
    text       *text_arg = PG_GETARG_TEXT_PP(0);
    Oid         target_type;
    int32       typmod = -1;
    Oid         typinput;
    Oid         typioparam;
    char       *str;
    Datum       result;

    // TODO: 当前函数尚未调通，执行时返回一行，但结果为空

    /* 获取目标类型和类型修饰符 */
    target_type = get_fn_expr_argtype(fcinfo->flinfo, 1); // 从第二个参数获取类型

    // 根据目标类型设置 typmod
    // if (target_type == VARCHAROID) {
    //     typmod = 255; // 对于 varchar，设置最大长度
    // } else if (target_type == CHAROID) {
    //     typmod = 128; // 假设我们处理 char(128)
    // } else if (target_type == NUMERICOID) {
    //     typmod = (int32) (numeric_get_typmod(fcinfo->args[1])); // 从参数中获取 numeric 的 typmod
    // }

    if (!OidIsValid(target_type))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("The target type cannot be inferred"))); // 无法推断目标类型

    getTypeInputInfo(target_type, &typinput, &typioparam);
    str = text_to_cstring(text_arg);
    elog(WARNING, "input: %s", str);
    result = OidInputFunctionCall(typinput, str, typioparam, typmod);

    PG_RETURN_DATUM(result);
}

/*--------------------------------*
 *  任意类型转文本的通用转换函数  *
 *--------------------------------*/
PG_FUNCTION_INFO_V1(type_to_text);
Datum
type_to_text(PG_FUNCTION_ARGS)
{
    Datum       value = PG_GETARG_DATUM(0);
    Oid         val_type = get_fn_expr_argtype(fcinfo->flinfo, 0);
    Oid         typoutput;
    bool        typisvarlena;
    char       *str;

    /* 获取目标类型的输出函数 */
    getTypeOutputInfo(val_type, &typoutput, &typisvarlena);

    /* 调用类型输出函数 */
    str = OidOutputFunctionCall(typoutput, value);

    PG_RETURN_TEXT_P(cstring_to_text(str));
}

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

void _PG_init(void)
{
    MemoryContext oldctx = MemoryContextSwitchTo(TopMemoryContext);
    // 注册类型和函数
    MemoryContextSwitchTo(oldctx);
}

/**********************************
 * composite data type
 **********************************/

#define COMPOSITE_LENGTH 6 // Composite 结构的字段总数

/* 复合类型结构体定义 */
typedef struct Composite
{
    text        *f1_text;
    VarChar     *f2_varchar;
    BpChar      *f3_char;
    bytea       *f4_bytea; // 字符串以 \\x 开头
    Jsonb       *f5_json;
#ifdef USE_LIBXML // 编译时需启用 --with-libxml（依赖 libxml 库）
    xmltype     *f6_xml;
#endif
} Composite;

PG_FUNCTION_INFO_V1(composite_out);
Datum
composite_out(PG_FUNCTION_ARGS)
{
    Composite  *comp = (Composite *) PG_GETARG_POINTER(0);
    StringInfoData str;

    initStringInfo(&str);

    /* f1_text */
    appendStringInfoString(&str, TextDatumGetCString(PointerGetDatum(comp->f1_text)));
    appendStringInfoChar(&str, '|');

    /* f2_varchar(128) */
    Datum varchar_datum = DirectFunctionCall1(varcharout, PointerGetDatum(comp->f2_varchar));
    char *varchar_str = DatumGetCString(varchar_datum);
    appendStringInfoString(&str, varchar_str);
    pfree(varchar_str);
    appendStringInfoChar(&str, '|');

    /* f3_char(256) */
    Datum bpchar_datum = DirectFunctionCall1(bpcharout, PointerGetDatum(comp->f3_char));
    char *bpchar_str = DatumGetCString(bpchar_datum);
    appendStringInfoString(&str, bpchar_str);
    pfree(bpchar_str);
    appendStringInfoChar(&str, '|');

    /* f4_bytea */
    Datum bytea_datum = DirectFunctionCall1(byteaout, PointerGetDatum(comp->f4_bytea));
    char *bytea_str = DatumGetCString(bytea_datum);
    appendStringInfoString(&str, bytea_str);
    pfree(bytea_str);
    appendStringInfoChar(&str, '|');

    /* f5_json */
    Datum jsonb_datum = DirectFunctionCall1(jsonb_out, PointerGetDatum(comp->f5_json));
    char *json_str = DatumGetCString(jsonb_datum);
    appendStringInfoString(&str, json_str);
    pfree(json_str);

#ifdef USE_LIBXML // 编译时需启用 --with-libxml（依赖 libxml 库）
    appendStringInfoChar(&str, '|');

    // TODO: xml not work
    /* f6_xml */
    // if (comp->f6_xml)
    // {
    //     Datum xml_datum = DirectFunctionCall1(xml_out, PointerGetDatum(comp->f6_xml));
    //     char *xml_str = DatumGetCString(xml_datum);
    //     appendStringInfoString(&str, xml_str);
    //     pfree(xml_str);
    // }
#endif

    PG_RETURN_CSTRING(str.data);
}

PG_FUNCTION_INFO_V1(composite_in);
Datum
composite_in(PG_FUNCTION_ARGS)
{
    char       *input_str = PG_GETARG_CSTRING(0);
    Composite  *comp;
    List       *namelist = NIL;
    ListCell   *lc;
    char      **fields;
    int         nfields = 0;
    int         i = 0;

    /* 分割输入字符串 */
    if (!SplitIdentifierString(input_str, '|', &namelist))
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("invalid input syntax for composite type")));

    /* 验证字段数量 */
    nfields = list_length(namelist);
    if (nfields != COMPOSITE_LENGTH)
    {
        list_free_deep(namelist);
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("composite type requires exactly 7 fields")));
    }

    /* 转换 List 到字符数组 */
    fields = (char **) palloc(sizeof(char *) * COMPOSITE_LENGTH);
    foreach(lc, namelist)
    {
        if (i >= COMPOSITE_LENGTH)
            break;

#ifdef COMPOSITE_DEEPCOPY
        char *sval = (char *) lfirst(lc);
        fields[i] = pstrdup(sval); // 深拷贝方式
#else
        fields[i] = (char *) lfirst(lc); // 浅拷贝方式
#endif
        // 如果打开这行elog代码且设置 set client_min_messages=INFO; 后，会在 client 端输出如下上下文信息：
        // LINE 1: SELECT 'text|varchar|char|\xDEADBEEF|{"key":123}|<xml>data</...
        // elog(INFO, "Field %d: %s", i, fields[i]);
        i++;
    }

#ifdef COMPOSITE_DEEPCOPY
    list_free_deep(namelist); // 深拷贝方式的释放
#else
    list_free(namelist); // 浅拷贝方式的释放
#endif

    comp = (Composite *) palloc0(sizeof(Composite));

    /* f1_text */
    comp->f1_text = cstring_to_text(fields[0]);  // 直接分配

    /* f2_varchar(128) */
    Datum varchar_datum = DirectFunctionCall3(varcharin,
                                             CStringGetDatum(fields[1]),
                                             ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(128));
    VarChar *varchar = DatumGetVarCharPP(varchar_datum);
    if (VARSIZE_ANY_EXHDR(varchar) > 128)
    {
        pfree(varchar);
        ereport(ERROR, (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                        errmsg("value too long for varchar(128)")));
    }
    comp->f2_varchar = (VarChar *) palloc(VARSIZE(varchar));
    memcpy(comp->f2_varchar, varchar, VARSIZE(varchar));
    pfree(varchar);  // 释放临时对象

    /* f3_char(256) */
    Datum bpchar_datum = DirectFunctionCall3(bpcharin,
                                             CStringGetDatum(fields[2]),
                                             ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(strlen(fields[2] + 1)));
    BpChar *bpchar = DatumGetBpCharPP(bpchar_datum);
    comp->f3_char = (BpChar *) palloc(VARSIZE(bpchar));
    memcpy(comp->f3_char, bpchar, VARSIZE(bpchar));
    pfree(bpchar);

    /* f4_bytea */
    if (strncmp(fields[3], "\\x", 2) != 0) /* 格式检查，\\ 是一个字符 */
        ereport(ERROR, (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                        errmsg("bytea input must start with \\x")));
    Datum bytea_datum = DirectFunctionCall1(byteain, CStringGetDatum(fields[3]));
    bytea *bytea_val = DatumGetByteaPP(bytea_datum);
    comp->f4_bytea = (bytea *) palloc(VARSIZE(bytea_val));
    memcpy(comp->f4_bytea, bytea_val, VARSIZE(bytea_val));
    pfree(bytea_val);

    /* f5_json */
    Datum jsonb_datum = DirectFunctionCall1(jsonb_in, CStringGetDatum(fields[4]));
    comp->f5_json = (Jsonb *) palloc(VARSIZE(DatumGetPointer(jsonb_datum)));
    memcpy(comp->f5_json, DatumGetPointer(jsonb_datum), VARSIZE(DatumGetPointer(jsonb_datum)));

#ifdef USE_LIBXML // 编译时需启用 --with-libxml（依赖 libxml 库）
    // TODO: xml not work
    /* f6_xml */
    if (strlen(fields[5]) > 0)
    {
    // #if 0
    //     Datum xml_datum = DirectFunctionCall1(xml_in, CStringGetDatum(fields[5]));
    //     xmltype *xml_val = DatumGetXmlP(xml_datum);
    //     comp->f6_xml = xmlCopy(xml_val);  // 假设存在深拷贝函数
    // #else
    //     /* 步骤1：将 XML 转换为字符串 */
    //     Datum xml_datum = DirectFunctionCall1(xml_in, CStringGetDatum(fields[5]));
    //     xmltype *xml_val = DatumGetXmlP(xml_datum);
    //     char *xml_str = DatumGetCString(DirectFunctionCall1(xml_out, XmlPGetDatum(xml_val)));

    //     /* 步骤2：重新解析字符串生成新对象 */
    //     Datum new_xml_datum = DirectFunctionCall1(xml_in, CStringGetDatum(xml_str));
    //     comp->f6_xml = DatumGetXmlP(new_xml_datum);

    //     pfree(xml_str);
    //     pfree(xml_val);
    // #endif
    // elog(INFO, "f6_xml: %s",  DatumGetXmlP(comp->f6_xml)); //  ==> DEBUG
    }
    else
    {
        comp->f6_xml = NULL;
    }
#endif

    /* 清理临时字段数组 */
    for (i = 0; i < COMPOSITE_LENGTH; i++)
        pfree(fields[i]); // 释放 fields[1] 时会报错 =======================================================
    pfree(fields);

    PG_RETURN_POINTER(comp);
}

void
composite_free(Composite *comp)
{
    if (comp->f1_text) pfree(comp->f1_text);
    if (comp->f2_varchar) pfree(comp->f2_varchar);
    if (comp->f3_char) pfree(comp->f3_char);
    if (comp->f4_bytea) pfree(comp->f4_bytea);
    if (comp->f5_json) pfree(comp->f5_json);
#ifdef USE_LIBXML // 编译时需启用 --with-libxml（依赖 libxml 库）
    if (comp->f6_xml) pfree(comp->f6_xml);
#endif
    pfree(comp);
}
