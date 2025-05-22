#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "catalog/pg_collation.h"
#include "catalog/namespace.h"

#include "access/brin_internal.h"
#include "access/brin_tuple.h"
#include "utils/acl.h"

/**********************************
 * mytext type
 **********************************/

typedef struct varlena *mytext;

PG_FUNCTION_INFO_V1(mytext_in);
Datum mytext_in(PG_FUNCTION_ARGS) {
    char *str = PG_GETARG_CSTRING(0);
    int len = strlen(str);

    // 分配 varlena 空间（长度+实际字符串）
    mytext *result = (mytext *)palloc(len + VARHDRSZ);

    // 设置长度头部
    SET_VARSIZE(result, len + VARHDRSZ);

    // 拷贝数据
    memcpy(VARDATA(result), str, len);

    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(mytext_out);
Datum mytext_out(PG_FUNCTION_ARGS) {
    mytext *mytxt = PG_GETARG_VARLENA_P(0);

    // 确保是非压缩版本
    mytxt = pg_detoast_datum(mytxt);

    int len = VARSIZE(mytxt) - VARHDRSZ;
    char *result = (char *)palloc(len + 1);

    memcpy(result, VARDATA(mytxt), len);
    result[len] = '\0';

    PG_RETURN_CSTRING(result);
}

static int mytext_cmp_internal(mytext *src, mytext *dst, Oid collid) {
    int s_len = VARSIZE_ANY_EXHDR(src);
    char *s_data = VARDATA_ANY(src);
    int d_len = VARSIZE_ANY_EXHDR(dst);
    char *d_data = VARDATA_ANY(dst);

    return varstr_cmp(s_data, s_len, d_data, d_len, collid);
}

PG_FUNCTION_INFO_V1(mytext_cmp);
Datum mytext_cmp(PG_FUNCTION_ARGS) {
    mytext *a = (mytext *)PG_GETARG_VARLENA_P(0);
    mytext *b = (mytext *)PG_GETARG_VARLENA_P(1);

    // 正确地使用 fcinfo
    Oid collation = PG_GET_COLLATION();
    if (collation == InvalidOid) {
        collation = DEFAULT_COLLATION_OID;  // 为 text 类型指定默认的排序规则
    }

    // 假设 mytext 是字符串类型，这里进行简单的字符串比较
    int result = mytext_cmp_internal(a, b, collation);

    PG_FREE_IF_COPY(a, 0);
    PG_FREE_IF_COPY(b, 1);

    return Int32GetDatum(result);
}

PG_FUNCTION_INFO_V1(mytext_op_lt);
Datum mytext_op_lt(PG_FUNCTION_ARGS) {
    PG_RETURN_BOOL(DatumGetInt32(mytext_cmp(fcinfo)) < 0);
}

PG_FUNCTION_INFO_V1(mytext_op_le);
Datum mytext_op_le(PG_FUNCTION_ARGS) {
    PG_RETURN_BOOL(DatumGetInt32(mytext_cmp(fcinfo)) <= 0);
}

PG_FUNCTION_INFO_V1(mytext_op_eq);
Datum mytext_op_eq(PG_FUNCTION_ARGS) {
    PG_RETURN_BOOL(DatumGetInt32(mytext_cmp(fcinfo)) == 0);
}

PG_FUNCTION_INFO_V1(mytext_op_ge);
Datum mytext_op_ge(PG_FUNCTION_ARGS) {
    PG_RETURN_BOOL(DatumGetInt32(mytext_cmp(fcinfo)) >= 0);
}

PG_FUNCTION_INFO_V1(mytext_op_gt);
Datum mytext_op_gt(PG_FUNCTION_ARGS) {
    PG_RETURN_BOOL(DatumGetInt32(mytext_cmp(fcinfo)) > 0);
}

// 哈希函数
PG_FUNCTION_INFO_V1(mytext_hash);
Datum mytext_hash(PG_FUNCTION_ARGS) {
    mytext *txt = PG_GETARG_VARLENA_PP(0); // 获取参数（自动处理 TOAST 压缩）
    Datum hash_datum = DirectFunctionCall1(hashvarlena, PointerGetDatum(txt));
    uint32 hash_value = DatumGetUInt32(hash_datum); // 提取哈希值（确保返回无符号整数）
    elog(DEBUG1, "hash_value: %u", hash_value);
    PG_RETURN_UINT32(hash_value); // 返回结果（无需手动释放内存）
    // PG_RETURN_UINT32(hash_value ^ 0xDEADBEEF);  // 简单修改哈希值
}

// 获取mytext类型的OID
static Oid get_mytext_type_oid(void) {
    Oid type_oid;

    // 通过类型名称查找OID
    type_oid = TypenameGetTypid("mytext");
    if (!OidIsValid(type_oid)) {
        elog(ERROR, "mytext type not found");
    }

    return type_oid;
}

// 初始化函数
PG_FUNCTION_INFO_V1(mytext_brin_minmax_opcinfo);
Datum mytext_brin_minmax_opcinfo(PG_FUNCTION_ARGS) {
    BrinOpcInfo *result;
    Oid mytext_type_oid;
    int i;

    // 获取mytext类型的OID（假设通过自定义函数获取）
    mytext_type_oid = get_mytext_type_oid();  // 需实现此函数

    // 一次性分配结构体+柔性数组的内存
    result = (BrinOpcInfo *)palloc(sizeof(BrinOpcInfo) + 2 * sizeof(TypeCacheEntry *)); // 2个缓存项

    // 初始化基本参数
    result->oi_nstored = 2; // 存储最小/最大值
    result->oi_opaque = NULL;

    // 初始化类型缓存项
    for (i = 0; i < 2; i++) {
        result->oi_typcache[i] = lookup_type_cache(
            mytext_type_oid,
            TYPECACHE_CMP_PROC_FINFO |  // 获取比较函数信息
            TYPECACHE_EQ_OPR |          // 获取相等操作符
            TYPECACHE_HASH_PROC         // 获取哈希函数
        );

        if (!result->oi_typcache[i]) {
            elog(ERROR, "Failed to initialize type cache entry for attribute %d", i);
        }
    }

    PG_RETURN_POINTER(result);
}

// 添加值到摘要
PG_FUNCTION_INFO_V1(mytext_brin_minmax_add_value);
Datum mytext_brin_minmax_add_value(PG_FUNCTION_ARGS) {
    BrinDesc *bdesc = (BrinDesc *)PG_GETARG_POINTER(0);
    BrinValues *column = (BrinValues *)PG_GETARG_POINTER(1);
    Datum newval = PG_GETARG_DATUM(2);
    bool isnull = PG_GETARG_BOOL(3);
    Oid collid = PG_GET_COLLATION();

    // 防御性检查
    if (column == NULL) {
        elog(ERROR, "BrinValues is not initialized");
    }

    // 首次添加值时初始化bv_values数组
    if (column->bv_values == NULL) {
        column->bv_values = (Datum *)palloc(2 * sizeof(Datum));
        // column->bv_attno = 2;
        column->bv_hasnulls = true;  // 初始时认为有NULL值
    }

    if (!isnull) {
        mytext *min_val = (mytext *)DatumGetPointer(column->bv_values[0]);
        mytext *max_val = (mytext *)DatumGetPointer(column->bv_values[1]);

        if (column->bv_hasnulls || (min_val == NULL && max_val == NULL)) {
            elog(DEBUG1, "min_val and max_val are NULL, cur_val=%s",
                 TextDatumGetCString((mytext *)DatumGetPointer(newval)));

            // 首次初始化摘要
            column->bv_values[0] = newval;  // 最小值
            column->bv_values[1] = newval;  // 最大值
            column->bv_hasnulls = false;
        } else {
            mytext *cur_val = (mytext *)DatumGetPointer(newval);

            elog(DEBUG1, "min_val=%s, max_val=%s, cur_val=%s",
                 TextDatumGetCString(min_val), TextDatumGetCString(max_val),
                 TextDatumGetCString(cur_val));

            // 更新最小/最大值
            if (mytext_cmp_internal(min_val, cur_val, collid) > 0)
                column->bv_values[0] = newval;
            if (mytext_cmp_internal(max_val, cur_val, collid) < 0)
                column->bv_values[1] = newval;
        }
    } else {
        // 处理NULL值
        column->bv_hasnulls = true;
    }

    PG_RETURN_BOOL(false); // 摘要未改变
}

// 执行一致性检查，判断数据块是否可能包含满足查询条件的记录
PG_FUNCTION_INFO_V1(mytext_brin_minmax_consistent);
Datum mytext_brin_minmax_consistent(PG_FUNCTION_ARGS) {
    BrinDesc *bdesc = (BrinDesc *)PG_GETARG_POINTER(0);
    BrinValues *column = (BrinValues *)PG_GETARG_POINTER(1);
    ScanKey key = (ScanKey)PG_GETARG_POINTER(2); // 仅3个参数

    bool result = false;
    Oid collid = PG_GET_COLLATION();
    mytext *min_val = NULL;
    mytext *max_val = NULL;

    // 安全获取最小/最大值
    if (column->bv_values[0] != NULL) {
        min_val = (mytext *)DatumGetPointer(column->bv_values[0]);
    }
    if (column->bv_values[1] != NULL) {
        max_val = (mytext *)DatumGetPointer(column->bv_values[1]);
    }

    // 检查是否有有效数据
    if (min_val && max_val && !column->bv_hasnulls) {
        Datum query = key->sk_argument;
        mytext *query_val = (mytext *)DatumGetPointer(query);

        switch (key->sk_strategy) {
            case BTLessStrategyNumber:         // <
                result = (mytext_cmp_internal(query_val, max_val, collid) < 0);
                break;
            case BTLessEqualStrategyNumber:    // <=
                result = (mytext_cmp_internal(query_val, max_val, collid) <= 0);
                break;
            case BTEqualStrategyNumber:        // =
                result = (mytext_cmp_internal(query_val, min_val, collid) >= 0 &&
                          mytext_cmp_internal(query_val, max_val, collid) <= 0);
                break;
            case BTGreaterStrategyNumber:      // >
                result = (mytext_cmp_internal(query_val, max_val, collid) > 0);
                break;
            case BTGreaterEqualStrategyNumber: // >=
                result = (mytext_cmp_internal(query_val, min_val, collid) >= 0);
                break;
            default:
                result = false;
        }
    }

    PG_RETURN_BOOL(result);
}

// 合并多个数据块的摘要信息，生成更广泛的范围摘要
PG_FUNCTION_INFO_V1(mytext_brin_minmax_union);
Datum mytext_brin_minmax_union(PG_FUNCTION_ARGS) {
    BrinValues *col_a = (BrinValues *)PG_GETARG_POINTER(1);
    BrinValues *col_b = (BrinValues *)PG_GETARG_POINTER(2);
    Oid collid = PG_GET_COLLATION();

    if (!col_a->bv_hasnulls && !col_b->bv_hasnulls) {
        // 合并最小/最大值
        if (mytext_cmp_internal(col_a->bv_values[0], col_b->bv_values[0], collid) > 0)
            col_a->bv_values[0] = col_b->bv_values[0];
        if (mytext_cmp_internal(col_a->bv_values[1], col_b->bv_values[1], collid) < 0)
            col_a->bv_values[1] = col_b->bv_values[1];
    } else if (col_b->bv_hasnulls) {
        col_a->bv_hasnulls = true;
    }

    PG_RETURN_VOID();
}

// 计算索引扫描的代价（ penalties ），用于查询优化器选择执行计划
PG_FUNCTION_INFO_V1(mytext_brin_minmax_penalty);
Datum mytext_brin_minmax_penalty(PG_FUNCTION_ARGS) {
    BrinValues *orig = (BrinValues *)PG_GETARG_POINTER(0);
    BrinValues *current = (BrinValues *)PG_GETARG_POINTER(1);
    Oid collid = PG_GET_COLLATION();
    float4 penalty = 1.0; // 默认代价因子

    // 示例：根据数据范围跨度计算代价（跨度越大，代价越高）
    if (!orig->bv_hasnulls && !current->bv_hasnulls) {
        mytext *orig_min = (mytext *)DatumGetPointer(orig->bv_values[0]);
        mytext *orig_max = (mytext *)DatumGetPointer(orig->bv_values[1]);
        mytext *curr_min = (mytext *)DatumGetPointer(current->bv_values[0]);
        mytext *curr_max = (mytext *)DatumGetPointer(current->bv_values[1]);

        int min_diff = mytext_cmp_internal(orig_min, curr_min, collid);
        int max_diff = mytext_cmp_internal(orig_max, curr_max, collid);

        // 假设跨度越大，代价越高（示例逻辑，需根据实际类型调整）
        if (min_diff != 0 || max_diff != 0) {
            penalty += fabs(min_diff) + fabs(max_diff);
        }
    }

    PG_RETURN_FLOAT4(penalty);
}

// 处理索引创建时的选项参数，如存储方式、压缩策略等（可选）
PG_FUNCTION_INFO_V1(mytext_brin_minmax_options);
Datum mytext_brin_minmax_options(PG_FUNCTION_ARGS) {
    BrinOpcInfo *info = (BrinOpcInfo *)PG_GETARG_POINTER(0);
    Datum options = PG_GETARG_DATUM(1);
    bool isnull = PG_GETARG_BOOL(2);

    if (isnull) {
        // 无选项时返回默认设置
        PG_RETURN_POINTER(info);
    }

    // 示例：解析选项（如压缩模式）
    // 实际需根据输入的 options 数据结构进行解析
    AclResult result = 0;
    // ... 选项解析逻辑 ...

    PG_RETURN_VOID();
}
