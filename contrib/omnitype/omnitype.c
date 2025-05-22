#include <libxml/parser.h>
#include <libxml/c14n.h>

#include "postgres.h"
#include "fmgr.h"

#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"      // 包含 NodeTag 类型定义
#include "nodes/pg_list.h"    // 包含 List 和 String 类型定义
#include "nodes/parsenodes.h" // 包含 String 结构体定义
#include "utils/builtins.h"
#include "utils/regproc.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "common/int128.h"
#include "utils/bytea.h"
#include "utils/jsonb.h"
#include "utils/varlena.h"
#ifdef USE_LIBXML
#include "utils/xml.h"
#endif
#include "catalog/pg_collation_d.h"
#include "catalog/pg_namespace_d.h"
#include "tsearch/ts_locale.h"
#include "tsearch/ts_utils.h"
#include "utils/array.h"
#include "utils/date.h"
#include "utils/float.h"
#include "utils/inet.h"
#include "utils/rangetypes.h"
#include "utils/timestamp.h"
#include "utils/uuid.h"
#include "utils/varbit.h"

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
// 必须要考虑对齐问题，否则会出现越界、错位访问等一系列问题
#define PADDING_TO_ALIGN 1

typedef enum CompositeIndex {
    COM_TEXT = 0,
    COM_VARCHAR,
    COM_CHAR,
    COM_BYTEA,
    COM_JSON,
    COM_XML,
    COM_INET,
    COM_BIT,
    COM_TSVECTOR,
    COM_UUID,
    COM_DOUBLE,
    COM_TIME,
    COM_DATE,
    COM_INT,
    COM_LEN // 总字段数
} CompositeIndex;

#ifndef USE_LIBXML // 编译时需启用 --with-libxml（依赖 libxml 库）
typedef struct varlena xmltype;
#endif

/*
 * 复合类型定义
 *
 * 编码要求：
 * 1. 必须使用类似 struct varlena 的结构，即第一个字段必须是 vl_len_ ，后续的数据部分
 *    必须位于同一段连续的内存中；原因是在使用 INSERT 语句插入数据时，首先会调用
 *    composite_in 函数，之后调用 fill_val 函数（src/backend/access/common/heaptuple.c文件）
 *    构建元组值，而在 fill_val 函数中，会调用
 *        data_length = VARATT_CONVERTED_SHORT_SIZE(val);
 *    获取字段值，之后调用 memcpy 拷贝整个字段值。若不位于连续内存中，会出现内存越界导致crash。
 * 2. 为了性能需要考虑内存对齐，这一点从自定义类型 mytext(位于mytext.c) 中可以看到。
 *
 * 解码要求：
 * 1. 在解码时，也需要考虑内存对齐，以准确读取到相应的字段，避免内存错位。
 */

// TODO: 支持如下类型：
// ArrayType   *f15_array; // 数组
// RangeType   *f16_range; // 范围类型
// 这两种类型的 in 函数都强依赖 fcinfo->flinfo->fn_extra 信息，实现复杂
typedef struct Composite {
    int32       vl_len_; // varlena 头（长度字段）

    // 变长字段（按顺序严格对应枚举）
    text        f1_text;
    VarChar     f2_varchar;
    BpChar      f3_char;
    bytea       f4_bytea;
    Jsonb       f5_json;
    xmltype     f6_xml;
    inet        f7_net;
    VarBit      f8_bit;
    TSVector    f9_tsvec;
    pg_uuid_t   f10_uuid;

    // // 标量字段（按顺序严格对应枚举）
    double      f11_double;
    Timestamp   f12_time;
    DateADT     f13_date;
    int32       f14_int;
} Composite;

bool field_is_null(char **fields, CompositeIndex pos)
{
    if (strlen(fields[pos]) == 0 || strcmp(fields[pos], "NULL") == 0)
        return true;
    return false;
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
    char       *ptr;

    /* 分割输入字符串 */
    /* 注意：
       1. SplitIdentifierString 函数中，namelist 中各个元素的内容是通过 nextp 指针
          直接在输入字符串 rawstring 上进行定位的，并没有使用动态内存分配函数来分配内存，
          因此，后面无需使用深拷贝释放这部分内存（强行释放会导致crash）。
       2. 包含空格的子串必须以 "" 标注。该函数默认会将空格及其他空白字符视为分隔符的一部分，
          这是因为该函数的设计初衷是解析 SQL 标识符，而 SQL 标识符通常不允许包含空格。 */
    if (!SplitIdentifierString(input_str, '|', &namelist))
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("invalid input syntax for composite type")));

    /* 验证字段数量 */
    nfields = list_length(namelist);
    if (nfields != COM_LEN) {
        list_free(namelist);
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("composite type requires exactly %d fields", COM_LEN)));
    }

    /* 转换 List 到字符数组 */
    fields = (char **) palloc(sizeof(char *) * COM_LEN);
    foreach(lc, namelist) {
        if (i >= COM_LEN)
            break;
        fields[i] = (char *) lfirst(lc); // 浅拷贝方式，而深拷贝方式需要使用 pstrdup
        i++;
    }

    list_free(namelist); // 浅拷贝方式的释放

    /* 处理每个字段，计算总大小 */

    struct {
        Datum datum;
        void *ptr;
        Size size;
    } field_data[COM_LEN]; // 变长字段临时存储

    // 计算总内存大小（包含 vl_len_ 自身）
    Size total_size = VARHDRSZ; // 初始化为 vl_len_ 的大小
#ifdef PADDING_TO_ALIGN
    total_size = MAXALIGN(total_size); // 在 vl_len_ 后面填充4个字节以对齐
#endif

    // 处理变长字段
    elog(DEBUG1, "composite_in fields ...");
    for (i = 0; i < COM_LEN; i++) {
        // 如果打开这行elog代码且设置 set client_min_messages=INFO; 后，会在 client 端输出如下上下文信息：
        // LINE 1: SELECT 'text|varchar|char|\xDEADBEEF|{"key":123}|<xml>data</...
        elog(DEBUG1, "field[%d]: %s", i, fields[i]);

        if (i >= COM_DOUBLE) break; // 标量字段之后处理
        switch (i) {
            case COM_TEXT:
                field_data[i].datum = DirectFunctionCall3(varcharin, CStringGetDatum(fields[i]),
                                                ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
                field_data[i].ptr = DatumGetPointer(field_data[i].datum);
                field_data[i].size = VARSIZE_ANY(field_data[i].ptr);
#ifdef PADDING_TO_ALIGN
                total_size += MAXALIGN(field_data[i].size);
#endif
                break;
            case COM_VARCHAR:
                // 可指定 typmod=128 并校验长度，会在字符串中补充很多空格
                field_data[i].datum = DirectFunctionCall3(varcharin, CStringGetDatum(fields[i]),
                                                ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
                field_data[i].ptr = DatumGetPointer(field_data[i].datum);
                field_data[i].size = VARSIZE_ANY(field_data[i].ptr);
#ifdef PADDING_TO_ALIGN
                total_size += MAXALIGN(field_data[i].size);
#endif
                if (VARSIZE_ANY_EXHDR(field_data[i].ptr) > 128) {
                    ereport(ERROR, (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                                    errmsg("value too long for varchar(128)")));
                }
                break;
            case COM_CHAR:
                // 若 typmod 为 256，可能会输出很多空格，而 -1 表示 bpcharin 函数根据 strlen 计算。
                field_data[i].datum = DirectFunctionCall3(bpcharin, CStringGetDatum(fields[i]),
                                                ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
                field_data[i].ptr = DatumGetPointer(field_data[i].datum);
                field_data[i].size = VARSIZE_ANY(field_data[i].ptr);
#ifdef PADDING_TO_ALIGN
                total_size += MAXALIGN(field_data[i].size);
#endif
                break;
            case COM_BYTEA:
                field_data[i].datum = DirectFunctionCall1(byteain, CStringGetDatum(fields[i]));
                field_data[i].ptr = DatumGetPointer(field_data[i].datum);
                field_data[i].size = VARSIZE_ANY(field_data[i].ptr);
#ifdef PADDING_TO_ALIGN
                total_size += MAXALIGN(field_data[i].size);
#endif
                break;
            case COM_JSON:
                field_data[i].datum = DirectFunctionCall1(jsonb_in, CStringGetDatum(fields[i]));
                field_data[i].ptr = DatumGetPointer(field_data[i].datum);
                field_data[i].size = VARSIZE_ANY(field_data[i].ptr);
#ifdef PADDING_TO_ALIGN
                total_size += MAXALIGN(field_data[i].size);
#endif
                break;
            case COM_XML:
#ifdef USE_LIBXML // 编译时需启用 --with-libxml（依赖 libxml 库）
                field_data[i].datum = DirectFunctionCall1(xml_in, CStringGetDatum(fields[i]));
#else
                Datum xml_datum = DirectFunctionCall3(
                    varcharin, CStringGetDatum(fields[i]),
                    ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
#endif
                field_data[i].ptr = DatumGetPointer(field_data[i].datum);
                field_data[i].size = VARSIZE_ANY(field_data[i].ptr);
#ifdef PADDING_TO_ALIGN
                total_size += MAXALIGN(field_data[i].size);
#endif
                break;
            case COM_INET:
                field_data[i].datum = DirectFunctionCall1(inet_in, CStringGetDatum(fields[i]));
                field_data[i].ptr = DatumGetPointer(field_data[i].datum);
                field_data[i].size = VARSIZE_ANY(field_data[i].ptr);
#ifdef PADDING_TO_ALIGN
                total_size += MAXALIGN(field_data[i].size);
#endif
                break;
            case COM_BIT:
                field_data[i].datum = DirectFunctionCall3(varbit_in, CStringGetDatum(fields[i]),
                                                ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
                field_data[i].ptr = DatumGetPointer(field_data[i].datum);
                field_data[i].size = VARSIZE_ANY(field_data[i].ptr);
#ifdef PADDING_TO_ALIGN
                total_size += MAXALIGN(field_data[i].size);
#endif
                break;
            case COM_TSVECTOR:
                field_data[i].datum = DirectFunctionCall1(tsvectorin, CStringGetDatum(fields[i]));
                field_data[i].ptr = DatumGetPointer(field_data[i].datum);
                field_data[i].size = VARSIZE_ANY(field_data[i].ptr);
#ifdef PADDING_TO_ALIGN
                total_size += MAXALIGN(field_data[i].size);
#endif
                break;
            case COM_UUID:
                field_data[i].datum = DirectFunctionCall1(uuid_in, CStringGetDatum(fields[i]));
                field_data[i].ptr = DatumGetPointer(field_data[i].datum);
                field_data[i].size = UUID_LEN; // sizeof(pg_uuid_t)
#ifdef PADDING_TO_ALIGN
                total_size += MAXALIGN(field_data[i].size);
#endif
                break;
            default:
                break;
        }
    }

    // 标量字段总大小及填充
#ifdef PADDING_TO_ALIGN
    total_size = MAXALIGN(total_size); // 对齐到8字节(x64)边界
#endif
    total_size += sizeof(double) + sizeof(Timestamp) + sizeof(DateADT) + sizeof(int32); // f11 - f14

    comp = (Composite *)palloc0(total_size);
    SET_VARSIZE(comp, total_size); // 设置 vl_len_ 的值
    ptr = (char *)comp + VARHDRSZ; // 开始填充 类 varlena 结构的 vl_dat 部分
#ifdef PADDING_TO_ALIGN
    // ptr = (char *) MAXALIGN(ptr); // 跳过在 vl_len_ 后面填充的4个字节
#endif

    // 复制变长字段并填充
    for (i = 0; i < COM_LEN; i++) {
        if (i >= COM_DOUBLE) break; // 标量字段之后处理
        switch (i) {
            case COM_TEXT:
            case COM_VARCHAR:
            case COM_CHAR:
            case COM_BYTEA:
            case COM_JSON:
            case COM_XML:
            case COM_INET:
            case COM_BIT:
            case COM_TSVECTOR:
            case COM_UUID:
                memcpy(ptr, field_data[i].ptr, field_data[i].size);
                ptr += field_data[i].size;
#ifdef PADDING_TO_ALIGN
                ptr = (char *) MAXALIGN(ptr); // 对齐到8字节(x64)
#endif
                pfree(field_data[i].ptr); // 释放临时对象
                break;
            default:
                break;
        }
    }

    // 处理标量字段
#ifdef PADDING_TO_ALIGN
    ptr = (char *) MAXALIGN(ptr); // 确保8字节(x64)对齐
#endif

    double *f11_double = (double *)ptr;
    *f11_double = DatumGetFloat8(DirectFunctionCall1(float8in, CStringGetDatum(fields[COM_DOUBLE])));
    ptr += sizeof(double);

    Timestamp *f12_time = (Timestamp *)ptr;
    if (strcmp(fields[COM_TIME], "infinity") == 0) {
        *f12_time = DT_NOEND;
    } else {
        *f12_time = DatumGetTimestamp(DirectFunctionCall3(timestamp_in, CStringGetDatum(fields[COM_TIME]),
                                      ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1)));
    }
    ptr += sizeof(Timestamp);

    DateADT *f13_date = (DateADT *)ptr;
    *f13_date = DatumGetDateADT(DirectFunctionCall1(date_in, CStringGetDatum(fields[COM_DATE])));
    ptr += sizeof(DateADT);

    int32 *f14_int = (int32 *)ptr;
    *f14_int = DatumGetInt32(DirectFunctionCall1(int4in, CStringGetDatum(fields[COM_INT])));
    ptr += sizeof(int32);

    /* 清理临时字段数组 */
    pfree(fields);

    // PG_RETURN_POINTER(PG_DETOAST_DATUM(PointerGetDatum(comp))); // 禁止 TOAST 压缩
    PG_RETURN_POINTER(comp); // 默认，可能会使用 TOAST 压缩
}

PG_FUNCTION_INFO_V1(composite_out);
Datum
composite_out(PG_FUNCTION_ARGS)
{
#ifdef PADDING_TO_ALIGN
    struct varlena *mycomp = PG_DETOAST_DATUM_COPY(PG_GETARG_DATUM(0));
#else
    // Composite  *comp = (Composite *)PG_GETARG_POINTER(0); // 只获取指针，不做解压缩
    struct varlena *mycomp = PG_GETARG_VARLENA_P(0); // 实际调用的是 PG_DETOAST_DATUM
#endif
    Composite  *comp = (Composite *)mycomp->vl_dat; // 也可以是 mycom + VARHDRSZ，以跳过 vl_len_
    char *ptr = (char *) comp;

#ifdef PADDING_TO_ALIGN
    // ptr = (char *) MAXALIGN(ptr); // 跳过在 vl_len_ 后面填充的4个字节
#endif

    StringInfoData str;
    initStringInfo(&str);

    /* 解析非标量字段，即变长字段（顺序必须与 composite_in 完全一致，且不为空） */

    text *f1_text = (text *)ptr; // 其中包含了 vl_len_
    appendStringInfoString(&str, TextDatumGetCString(f1_text));
    ptr += VARSIZE_ANY(f1_text);
#ifdef PADDING_TO_ALIGN
    ptr = (char *)MAXALIGN(ptr); // 对齐到8字节(x64)
#endif

    VarChar *f2_varchar = (VarChar *)ptr;
    appendStringInfoChar(&str, '|');
    appendStringInfoString(&str, TextDatumGetCString(f2_varchar));
    ptr += VARSIZE_ANY(f2_varchar);
#ifdef PADDING_TO_ALIGN
    ptr = (char *)MAXALIGN(ptr);
#endif

    BpChar *f3_char = (BpChar *)ptr;
    appendStringInfoChar(&str, '|');
    appendStringInfoString(&str, TextDatumGetCString(f3_char));
    ptr += VARSIZE_ANY(f3_char);
#ifdef PADDING_TO_ALIGN
    ptr = (char *)MAXALIGN(ptr);
#endif

    bytea *f4_bytea = (bytea *)ptr;
    Datum bytea_datum = DirectFunctionCall1(byteaout, PointerGetDatum(f4_bytea));
    char *bytea_str = DatumGetCString(bytea_datum);
    appendStringInfoChar(&str, '|');
    appendStringInfoString(&str, bytea_str);
    pfree(bytea_str);
    ptr += VARSIZE_ANY(f4_bytea);
#ifdef PADDING_TO_ALIGN
    ptr = (char *)MAXALIGN(ptr);
#endif

    Jsonb *f5_json = (Jsonb *)ptr;
    Datum jsonb_datum = DirectFunctionCall1(jsonb_out, PointerGetDatum(f5_json));
    char *json_str = DatumGetCString(jsonb_datum);
    appendStringInfoChar(&str, '|');
    appendStringInfoString(&str, json_str);
    pfree(json_str);
    ptr += VARSIZE_ANY(f5_json);
#ifdef PADDING_TO_ALIGN
    ptr = (char *)MAXALIGN(ptr);
#endif

    xmltype *f6_xml = (xmltype *)ptr;
    appendStringInfoChar(&str, '|');
#ifdef USE_LIBXML // 编译时需启用 --with-libxml（依赖 libxml 库）
    Datum xml_datum = DirectFunctionCall1(xml_out, PointerGetDatum(f6_xml));
#else
    Datum xml_datum = DirectFunctionCall1(varcharout, PointerGetDatum(f6_xml));
#endif
    char *xml_str = DatumGetCString(xml_datum);
    appendStringInfoString(&str, xml_str);
    pfree(xml_str);
    ptr += VARSIZE_ANY(f6_xml);
#ifdef PADDING_TO_ALIGN
    ptr = (char *)MAXALIGN(ptr);
#endif

    inet *f7_net = (inet *)ptr;
    Datum net_datum = DirectFunctionCall1(inet_out, PointerGetDatum(f7_net));
    appendStringInfoChar(&str, '|');
    appendStringInfoString(&str, DatumGetCString(net_datum));
    pfree(DatumGetPointer(net_datum));
    ptr += VARSIZE_ANY(f7_net);
#ifdef PADDING_TO_ALIGN
    ptr = (char *)MAXALIGN(ptr);
#endif

    VarBit *f8_bit = (VarBit *)ptr;
    Datum bit_datum = DirectFunctionCall1(bit_out, PointerGetDatum(f8_bit));
    appendStringInfoChar(&str, '|');
    appendStringInfoString(&str, DatumGetCString(bit_datum));
    pfree(DatumGetPointer(bit_datum));
    ptr += VARSIZE_ANY(f8_bit);
#ifdef PADDING_TO_ALIGN
    ptr = (char *)MAXALIGN(ptr);
#endif

    TSVector *f9_tsvec = (TSVector *)ptr;
    Datum ts_datum = DirectFunctionCall1Coll(tsvectorout, PG_GET_COLLATION(), // 显式指定 collation
                                             TSVectorGetDatum(f9_tsvec));
    appendStringInfoChar(&str, '|');
    appendStringInfoString(&str, DatumGetCString(ts_datum));
    pfree(DatumGetPointer(ts_datum));
    ptr += VARSIZE_ANY(f9_tsvec);
#ifdef PADDING_TO_ALIGN
    ptr = (char *)MAXALIGN(ptr);
#endif

    pg_uuid_t *f10_uuid = (pg_uuid_t *)ptr;
    Datum uuid_datum = DirectFunctionCall1(uuid_out, PointerGetDatum(f10_uuid));
    appendStringInfoChar(&str, '|');
    appendStringInfoString(&str, DatumGetCString(uuid_datum));
    pfree(DatumGetPointer(uuid_datum));
    ptr += UUID_LEN; // sizeof(*f10_uuid), 16 bytes
#ifdef PADDING_TO_ALIGN
    ptr = (char *)MAXALIGN(ptr); // 确保对齐，准备解析标量字段
#endif

    // 解析标量字段（确保对齐）
    double f11_double = *((double *) ptr);
    appendStringInfoChar(&str, '|');
    appendStringInfo(&str, "%g", f11_double);
    ptr += sizeof(double);

    Timestamp f12_time = *((Timestamp *) ptr);
    appendStringInfoChar(&str, '|');
    appendStringInfoString(&str, DatumGetCString(DirectFunctionCall1(timestamp_out, TimestampGetDatum(f12_time))));
    ptr += sizeof(Timestamp);

    DateADT f13_date = *((DateADT *) ptr);
    appendStringInfoChar(&str, '|');
    appendStringInfoString(&str, DatumGetCString(DirectFunctionCall1(date_out, DateADTGetDatum(f13_date))));
    ptr += sizeof(DateADT);

    int32 f14_int = *((int32 *) ptr);
    appendStringInfoChar(&str, '|');
    appendStringInfo(&str, "%d", f14_int); // 也可以调用 int4out（会调用 atoi）得到 Datum，并通过 DatumGetCString 转为字符串
    ptr += sizeof(int32);

    PG_RETURN_CSTRING(str.data);
}

/* text_cmp()
 * Internal comparison function for text strings.
 * Returns -1, 0 or 1
 */
static int
text_cmp(text *arg1, text *arg2, Oid collid)
{
	char	   *a1p,
			   *a2p;
	int			len1,
				len2;

	a1p = VARDATA_ANY(arg1);
	a2p = VARDATA_ANY(arg2);

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	return varstr_cmp(a1p, len1, a2p, len2, collid);
}

static text *
xml_canonicalize(text *xml_input)
{
    xmlDocPtr doc = NULL;
    xmlChar *canonicalized = NULL;
    int ret;

    // 解析 XML
    doc = xmlParseMemory(VARDATA(xml_input), VARSIZE(xml_input) - VARHDRSZ);
    if (!doc)
        ereport(ERROR, (errcode(ERRCODE_INVALID_XML_DOCUMENT), errmsg("invalid XML document")));

    // 规范化 XML（示例：排除注释，保留空白）
    ret = xmlC14NDocDumpMemory(
        doc,
        NULL,  // 不排除任何命名空间
        XML_C14N_1_0,
        NULL,   // 不包含注释
        1,      // 格式化输出（如缩进）
        &canonicalized
    );

    if (ret < 0 || !canonicalized)
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("XML canonicalization failed")));

    // 将规范化后的文本转为 text 类型
    text *result = cstring_to_text_with_len((char *)canonicalized, strlen((char *)canonicalized));

    // 清理内存
    xmlFree(canonicalized);
    xmlFreeDoc(doc);

    return result;
}

PG_FUNCTION_INFO_V1(composite_cmp);
Datum
composite_cmp(PG_FUNCTION_ARGS)
{
    /*
     * 当执行带有比较操作符的 SQL 语句，比如 select * from t1 where c2 > 10;
     * 那么，在调用比较函数时，比如 cmp(a, b)，那么从表里读取的数据为 a，而查询中附带的
     * 值为 b，比如本例中的 10。
     * gdb跟踪发现，与 composite_out 类似，其取到的值不是8字节对齐，而是错位4字节，
     * 我分析原因是上层调用中取值时又做了一层变长封装，而变长头为 VARHDRSZ（4字节）。
     *
     * 同样，当执行 SELECT * FROM comp_test ORDER BY data; 时，在调用 cmp(a,b) 比较时，
     * 表中的字段都可能为 a 或 b，具体来说：
     * 1. 升序排列时，较小的值先作为 a，较大的值作为 b；
     * 2. 降序排列时，较大的值先作为 a，较小的值作为 b。
     */
    // 通过拷贝方式强行8字节对齐，不能直接使用 PG_GETARG_VARLENA_PP(1);
    Composite  *a = (Composite *) PG_DETOAST_DATUM_COPY(PG_GETARG_DATUM(0));
    Composite  *b = (Composite *) PG_DETOAST_DATUM_COPY(PG_GETARG_DATUM(1));
    char       *ptr_a = (char *) a + VARHDRSZ;
    char       *ptr_b = (char *) b + VARHDRSZ;
    int         cmp_result = 0;

    /* 按字段顺序逐个比较 */
    elog(DEBUG1, "composite_cmp fields ...");
    for (CompositeIndex i = COM_TEXT; i < COM_LEN; i++)
    {
        if (i == COM_DOUBLE) // 在开始处理标量字段前，先对齐内存
        {
#ifdef PADDING_TO_ALIGN
            ptr_a = (char *) MAXALIGN(ptr_a);
            ptr_b = (char *) MAXALIGN(ptr_b);
#endif
        }

        switch (i)
        {
            /* --------------- 变长字段处理 --------------- */
            // text、varchar、char 本质都是变长结构 struct varlena
            case COM_TEXT:
            case COM_VARCHAR:
            case COM_CHAR:
            {
                text *ta = (text *) ptr_a;
                text *tb = (text *) ptr_b;
                // 无法使用 DirectFunctionCall2Coll 函数调用 text_cmp，因此，手动实现。
                cmp_result = text_cmp(ta, tb, PG_GET_COLLATION());
                ptr_a += VARSIZE_ANY(ta);
                ptr_b += VARSIZE_ANY(tb);
                elog(DEBUG1, "ta: %s, tb: %s",
                     TextDatumGetCString(PointerGetDatum(ta)),
                     TextDatumGetCString(PointerGetDatum(tb)));
                break;
            }
            case COM_BYTEA:
            {
                bytea *ba = (bytea *) ptr_a;
                bytea *bb = (bytea *) ptr_b;
                cmp_result = DatumGetInt32(
                    DirectFunctionCall2(byteacmp, PointerGetDatum(ba), PointerGetDatum(bb))
                );
                ptr_a += VARSIZE_ANY(ba);
                ptr_b += VARSIZE_ANY(bb);
                elog(DEBUG1, "ba: %s, bb: %s",
                     DatumGetCString(DirectFunctionCall1(byteaout, PointerGetDatum(ba))),
                     DatumGetCString(DirectFunctionCall1(byteaout, PointerGetDatum(bb))));
                break;
            }
            case COM_JSON:
            {
                Jsonb *ja = (Jsonb *) ptr_a;
                Jsonb *jb = (Jsonb *) ptr_b;
                cmp_result = DatumGetInt32(
                    DirectFunctionCall2(jsonb_cmp, PointerGetDatum(ja), PointerGetDatum(jb))
                );
                ptr_a += VARSIZE_ANY(ja);
                ptr_b += VARSIZE_ANY(jb);
                elog(DEBUG1, "ja: %s, jb: %s",
                     DatumGetCString(DirectFunctionCall1(jsonb_out, PointerGetDatum(ja))),
                     DatumGetCString(DirectFunctionCall1(jsonb_out, PointerGetDatum(jb))));
                break;
            }
            case COM_XML:
            {
                text *xa = (text *) ptr_a;
                text *xb = (text *) ptr_b;
#ifdef USE_LIBXML
                // 启用 LIBXML：规范化后比较
                text *canon_a = xml_canonicalize(xa);
                text *canon_b = xml_canonicalize(xb);
                cmp_result = text_cmp(canon_a, canon_b, PG_GET_COLLATION());
                pfree(canon_a);
                pfree(canon_b);
#else
                // 未启用 LIBXML：直接比较原始文本
                cmp_result = text_cmp(xa, xb, PG_GET_COLLATION());
#endif
                ptr_a += VARSIZE_ANY(ptr_a);
                ptr_b += VARSIZE_ANY(ptr_b);
                elog(DEBUG1, "xa: %s, xb: %s",
                     TextDatumGetCString(PointerGetDatum(xa)),
                     TextDatumGetCString(PointerGetDatum(xb)));
                break;
            }
            case COM_INET:
            {
                inet *ia = (inet *) ptr_a;
                inet *ib = (inet *) ptr_b;
                cmp_result = DatumGetInt32(
                    DirectFunctionCall2(network_cmp, PointerGetDatum(ia), PointerGetDatum(ib))
                );
                ptr_a += VARSIZE_ANY(ia);
                ptr_b += VARSIZE_ANY(ib);
                elog(DEBUG1, "ia: %s, ib: %s",
                     DatumGetCString(DirectFunctionCall1(inet_out, PointerGetDatum(ia))),
                     DatumGetCString(DirectFunctionCall1(inet_out, PointerGetDatum(ib))));
                break;
            }
            case COM_BIT:
            {
                VarBit *ba = (VarBit *) ptr_a;
                VarBit *bb = (VarBit *) ptr_b;
                cmp_result = DatumGetInt32(
                    DirectFunctionCall2(bitcmp, PointerGetDatum(ba), PointerGetDatum(bb))
                );
                ptr_a += VARSIZE_ANY(ba);
                ptr_b += VARSIZE_ANY(bb);
                elog(DEBUG1, "ba: %s, bb: %s",
                     DatumGetCString(DirectFunctionCall1(bit_out, PointerGetDatum(ba))),
                     DatumGetCString(DirectFunctionCall1(bit_out, PointerGetDatum(bb))));
                break;
            }
            case COM_TSVECTOR:
            {
                TSVector *ta = (TSVector *) ptr_a;
                TSVector *tb = (TSVector *) ptr_b;
                cmp_result = DatumGetInt32(
                    DirectFunctionCall2(tsvector_cmp, PointerGetDatum(ta), PointerGetDatum(tb))
                );
                ptr_a += VARSIZE_ANY(ta);
                ptr_b += VARSIZE_ANY(tb);
                elog(DEBUG1, "ta: %s, tb: %s",
                     DatumGetCString(DirectFunctionCall1Coll(tsvectorout, PG_GET_COLLATION(), TSVectorGetDatum(ta))),
                     DatumGetCString(DirectFunctionCall1Coll(tsvectorout, PG_GET_COLLATION(), TSVectorGetDatum(tb))));
                break;
            }
            case COM_UUID:
            {
                pg_uuid_t *ua = (pg_uuid_t *) ptr_a;
                pg_uuid_t *ub = (pg_uuid_t *) ptr_b;
                cmp_result = DatumGetInt32(
                    DirectFunctionCall2(uuid_cmp, PointerGetDatum(ua), PointerGetDatum(ub))
                );
                ptr_a += UUID_LEN;
                ptr_b += UUID_LEN;
                elog(DEBUG1, "ua: %s, ub: %s",
                     DatumGetCString(DirectFunctionCall1(uuid_out, PointerGetDatum(ua))),
                     DatumGetCString(DirectFunctionCall1(uuid_out, PointerGetDatum(ub))));
                break;
            }

            /* --------------- 标量字段处理 --------------- */
            case COM_DOUBLE:
            {
                double da = *((double *) ptr_a);
                double db = *((double *) ptr_b);
                cmp_result = (da < db) ? -1 : (da > db) ? 1 : 0;
                ptr_a += sizeof(double);
                ptr_b += sizeof(double);
                elog(DEBUG1, "da: %f, db: %f", da, db);
                break;
            }
            case COM_TIME:
            {
                Timestamp ta = *((Timestamp *) ptr_a);
                Timestamp tb = *((Timestamp *) ptr_b);
                cmp_result = DatumGetInt32(
                    DirectFunctionCall2(timestamp_cmp, TimestampGetDatum(ta), TimestampGetDatum(tb))
                );
                ptr_a += sizeof(Timestamp);
                ptr_b += sizeof(Timestamp);
                elog(DEBUG1, "ta: %s, tb: %s",
                     DatumGetCString(DirectFunctionCall1(timestamp_out, TimestampGetDatum(ta))),
                     DatumGetCString(DirectFunctionCall1(timestamp_out, TimestampGetDatum(tb))));
                break;
            }
            case COM_DATE:
            {
                DateADT da = *((DateADT *) ptr_a);
                DateADT db = *((DateADT *) ptr_b);
                cmp_result = (da < db) ? -1 : (da > db) ? 1 : 0;
                ptr_a += sizeof(DateADT);
                ptr_b += sizeof(DateADT);
                elog(DEBUG1, "da: %s, db: %s",
                     DatumGetCString(DirectFunctionCall1(date_out, DateADTGetDatum(da))),
                     DatumGetCString(DirectFunctionCall1(date_out, DateADTGetDatum(db))));
                break;
            }
            case COM_INT:
            {
                int32 ia = *((int32 *) ptr_a);
                int32 ib = *((int32 *) ptr_b);
                cmp_result = (ia < ib) ? -1 : (ia > ib) ? 1 : 0;
                ptr_a += sizeof(int32);
                ptr_b += sizeof(int32);
                elog(DEBUG1, "ia: %d, ib: %d", ia, ib);
                break;
            }
            default:
                elog(ERROR, "未处理的字段类型: %d", i);
        }

        /* 内存对齐（若启用） */
#ifdef PADDING_TO_ALIGN
        if (i < COM_DOUBLE) // 变长字段后对齐
        {
            ptr_a = (char *) MAXALIGN(ptr_a);
            ptr_b = (char *) MAXALIGN(ptr_b);
        }
#endif

        /* 如果当前字段比较结果非零，提前返回 */
        if (cmp_result != 0)
            break;
    }

    PG_FREE_IF_COPY(a, 0);
    PG_FREE_IF_COPY(b, 1);
    PG_RETURN_INT32(cmp_result);
}

/* 等于 */
PG_FUNCTION_INFO_V1(composite_eq);
Datum
composite_eq(PG_FUNCTION_ARGS)
{
    int cmp = DatumGetInt32(composite_cmp(fcinfo));
    PG_RETURN_BOOL(cmp == 0);
}

/* 小于 */
PG_FUNCTION_INFO_V1(composite_lt);
Datum
composite_lt(PG_FUNCTION_ARGS)
{
    int cmp = DatumGetInt32(composite_cmp(fcinfo));
    PG_RETURN_BOOL(cmp < 0);
}

/* 小于等于 */
PG_FUNCTION_INFO_V1(composite_le);
Datum
composite_le(PG_FUNCTION_ARGS)
{
    int cmp = DatumGetInt32(composite_cmp(fcinfo));
    PG_RETURN_BOOL(cmp <= 0);
}

/* 大于等于 */
PG_FUNCTION_INFO_V1(composite_ge);
Datum
composite_ge(PG_FUNCTION_ARGS)
{
    int cmp = DatumGetInt32(composite_cmp(fcinfo));
    PG_RETURN_BOOL(cmp >= 0);
}

/* 大于 */
PG_FUNCTION_INFO_V1(composite_gt);
Datum
composite_gt(PG_FUNCTION_ARGS)
{
    int cmp = DatumGetInt32(composite_cmp(fcinfo));
    PG_RETURN_BOOL(cmp > 0);
}
