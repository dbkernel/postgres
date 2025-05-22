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
// #include "utils/bpchar.h"
// #include "utils/varchar.h"
#include "utils/bytea.h"
#include "utils/jsonb.h"
#include "utils/varlena.h"
#include "utils/xml.h"
#include "utils/regproc.h"
#include "utils/lsyscache.h"
#include "catalog/pg_collation_d.h"

PG_MODULE_MAGIC;

/* _PG_init、_PG_fini 必须唯一，通常放在插件主 .c 文件中 */
void _PG_init(void);
void _PG_init(void)
{
    MemoryContext oldctx = MemoryContextSwitchTo(TopMemoryContext);

    // 注册类型和函数

    MemoryContextSwitchTo(oldctx);
}

void _PG_fini(void);
void
_PG_fini(void)
{
}

/**********************************
 * composite data type
 **********************************/

#define COMPOSITE_LENGTH 6 // Composite 结构的字段总数
// #define COMPOSITE_DEEPCOPY 1 // 是否使用深拷贝

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

    /* f6_xml */
    if (comp->f6_xml)
    {
        Datum xml_datum = DirectFunctionCall1(xml_out, PointerGetDatum(comp->f6_xml));
        char *xml_str = DatumGetCString(xml_datum);
        appendStringInfoString(&str, xml_str);
        pfree(xml_str);
    }
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
    /* 注意：SplitIdentifierString 函数中，namelist 中各个元素的内容是通过 nextp 指针
       直接在输入字符串 rawstring 上进行定位的，并没有使用动态内存分配函数来分配内存，
       因此，后面无需使用深拷贝释放这部分内存（强行释放会导致crash）。 */
    if (!SplitIdentifierString(input_str, '|', &namelist))
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("invalid input syntax for composite type")));

    /* 验证字段数量 */
    nfields = list_length(namelist);
    if (nfields != COMPOSITE_LENGTH)
    {
        list_free(namelist);
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("composite type requires exactly %d fields", COMPOSITE_LENGTH)));
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

    list_free(namelist); // 浅拷贝方式的释放

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
    Datum bpchar_datum = DirectFunctionCall3(
        bpcharin, CStringGetDatum(fields[2]), ObjectIdGetDatum(InvalidOid),
        Int32GetDatum(-1));  // 若 typmod 为 256，可能会输出很多空格，
                             // 而 -1 表示 bpcharin 函数根据 strlen 计算。
    BpChar *bpchar = DatumGetBpCharPP(bpchar_datum);
    if (VARSIZE_ANY_EXHDR(bpchar) > 256) // 长度校验
    {
        pfree(bpchar);
        ereport(ERROR, (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                        errmsg("value too long for bpchar(256)")));
    }
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
    /* f6_xml */
    if (strlen(fields[5]) > 0)
    {
        Datum xml_datum = DirectFunctionCall1(xml_in, CStringGetDatum(fields[5]));
        comp->f6_xml = (xmltype *) palloc(VARSIZE(DatumGetPointer(xml_datum)));
        memcpy(comp->f6_xml, DatumGetPointer(xml_datum), VARSIZE(DatumGetPointer(xml_datum)));
    }
    else
    {
        comp->f6_xml = NULL;
    }
#endif

    /* 清理临时字段数组 */
#ifdef COMPOSITE_DEEPCOPY
    for (i = 0; i < COMPOSITE_LENGTH; i++)
    {
        elog(INFO, "Freeing field %d: %s", i, fields[i]);
        pfree(fields[i]);
    }
#endif
    pfree(fields);

    PG_RETURN_POINTER(comp);
}
