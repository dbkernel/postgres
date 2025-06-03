#include <math.h>
#include <unistd.h>
#include <sys/mman.h>

#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/pg_crc.h"
#include "utils/acl.h"
#include "utils/varlena.h"

#include "access/brin_internal.h"
#include "access/brin_tuple.h"
#include "access/gin.h"
#include "access/gin_private.h"
#include "access/gist.h"
#include "access/skey.h"
#include "access/stratnum.h"
#include "catalog/pg_collation.h"
#include "catalog/namespace.h"

#include "regex/regex.h"

#define olog(level, message, ...) \
    elog(level, "[%s]\t " message, __func__, ##__VA_ARGS__)

/**********************************
 * mytext type
 **********************************/

typedef struct varlena mytext;

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
    mytext *mytxt = (mytext *)PG_GETARG_VARLENA_P(0); // 确保是非压缩版本
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

    int result = varstr_cmp(s_data, s_len, d_data, d_len, collid);
    result = ((result > 0) ? 1 : ((result < 0) ? -1 : 0));
    olog(DEBUG1, "src=%s, dst=%s, result=%d", text_to_cstring(src),
         text_to_cstring(dst), result);
    return result;
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

    olog(DEBUG1, "a=%s, b=%s, result=%d", text_to_cstring(a),
         text_to_cstring(b), result);

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
    mytext *txt = (mytext *)PG_GETARG_VARLENA_P(0); // 获取参数（自动处理 TOAST 压缩）
    Datum hash_datum = DirectFunctionCall1(hashvarlena, PointerGetDatum(txt));
    uint32 hash_value = DatumGetUInt32(hash_datum); // 提取哈希值（确保返回无符号整数）
    olog(DEBUG1, "hash_value=%u", hash_value);
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
    // BrinDesc *bdesc = (BrinDesc *)PG_GETARG_POINTER(0); // 暂时无用，仅占位
    BrinValues *column = (BrinValues *)PG_GETARG_POINTER(1);
    mytext *newval = PG_DETOAST_DATUM(PG_GETARG_DATUM(2));
    bool isnull = PG_GETARG_BOOL(3);
    Oid collid = PG_GET_COLLATION();
    bool updated = false;

    // 防御性检查
    if (column == NULL) {
        elog(ERROR, "BrinValues is not initialized");
    }

    // 首次添加值时初始化bv_values数组
    if (column->bv_values == NULL) {
        column->bv_values = (Datum *)palloc0(2 * sizeof(Datum));
        column->bv_attno = 2;
        column->bv_hasnulls = true;  // 初始时认为有NULL值
        column->bv_allnulls = true; // 当前范围块的所有值都是 NULL
        column->bv_values[0] = (Datum)NULL; // 最小值设为NULL
        column->bv_values[1] = (Datum)NULL; // 最大值设为NULL
    }

    if (!isnull) {
        mytext *cur_val = newval;
        Datum copied_datum = PointerGetDatum(cur_val);
        mytext *min_val = (mytext *)DatumGetPointer(column->bv_values[0]);
        mytext *max_val = (mytext *)DatumGetPointer(column->bv_values[1]);

        if (column->bv_hasnulls || (min_val == NULL && max_val == NULL)) {
            olog(DEBUG1, "min_val and max_val are NULL, cur_val=%s",
                 TextDatumGetCString(cur_val));

            // 首次初始化摘要，存储深拷贝后的值
            column->bv_values[0] = copied_datum;  // 最小值
            column->bv_values[1] = copied_datum;  // 最大值
            column->bv_hasnulls = false; // 当前范围块的所有值存在 NULL 值
            column->bv_allnulls = false; // 当前范围块的所有值并不都是 NULL
            updated = true;
        } else {
            olog(DEBUG1, "before update: min_val=%s, max_val=%s, cur_val=%s",
                 TextDatumGetCString(min_val), TextDatumGetCString(max_val),
                 TextDatumGetCString(cur_val));

            // 更新最小/最大值
            if (mytext_cmp_internal(min_val, cur_val, collid) > 0) {
                column->bv_values[0] = copied_datum;
                updated = true;
            }
            if (mytext_cmp_internal(max_val, cur_val, collid) < 0) {
                column->bv_values[1] = copied_datum;
                updated = true;
            }

            olog(DEBUG1, "after update: bv_values[0]=%s, bv_values[1]=%s",
                 TextDatumGetCString(column->bv_values[0]),
                 TextDatumGetCString(column->bv_values[1]));
        }
    } else {
        // 处理NULL值
        if (!column->bv_hasnulls) {
            column->bv_hasnulls = true;
            updated = true;
            olog(DEBUG1, "mark the current data block as containing NULL values");
        }
    }

    PG_RETURN_BOOL(updated); // 返回摘要是否改变
}

// 执行一致性检查，判断数据块是否可能包含满足查询条件的记录
PG_FUNCTION_INFO_V1(mytext_brin_minmax_consistent);
Datum mytext_brin_minmax_consistent(PG_FUNCTION_ARGS) {
    // BrinDesc *bdesc = (BrinDesc *)PG_GETARG_POINTER(0); // 暂时无用，仅占位
    BrinValues *column = (BrinValues *)PG_GETARG_POINTER(1);
    ScanKey key = (ScanKey)PG_GETARG_POINTER(2); // 仅3个参数

    bool result = false;
    Oid collid = PG_GET_COLLATION();
    mytext *min_val = NULL;
    mytext *max_val = NULL;

    // 安全获取最小/最大值
    if (!column->bv_hasnulls) {
        if (column->bv_values[0] != (Datum)0)
            min_val = (mytext *)DatumGetPointer(column->bv_values[0]);
        if (column->bv_values[1] != (Datum)0)
            max_val = (mytext *)DatumGetPointer(column->bv_values[1]);
    }

    olog(DEBUG1, "min_val=%s, max_val=%s", text_to_cstring(min_val),
         text_to_cstring(max_val));

    // 检查是否有有效数据
    if (min_val && max_val && !column->bv_hasnulls) {
        Datum query = key->sk_argument;
        // mytext *query_val = (mytext *)DatumGetPointer(query);
        mytext *query_val = (mytext *)PG_DETOAST_DATUM(query);

        // 这两种比较方式本质一样
        // 方式一
        // switch (key->sk_strategy) {
        //     case BTLessStrategyNumber:         // <
        //         result = (mytext_cmp_internal(query_val, max_val, collid) < 0);
        //         break;
        //     case BTLessEqualStrategyNumber:    // <=
        //         result = (mytext_cmp_internal(query_val, max_val, collid) <= 0);
        //         break;
        //     case BTEqualStrategyNumber:        // =
        //         result = (mytext_cmp_internal(query_val, max_val, collid) <= 0 &&
        //                   mytext_cmp_internal(query_val, min_val, collid) >= 0);
        //         break;
        //     case BTGreaterEqualStrategyNumber: // >=
        //         result = (mytext_cmp_internal(query_val, min_val, collid) >= 0);
        //         break;
        //     case BTGreaterStrategyNumber:      // >
        //         result = (mytext_cmp_internal(query_val, min_val, collid) > 0);
        //         break;
        //     default:
        //         result = false;
        // }

        // 方式二
        switch (key->sk_strategy) {
            case BTLessStrategyNumber: // <
                result = (mytext_cmp_internal(min_val, query_val, collid) < 0);
                break;
            case BTLessEqualStrategyNumber: // <=
                result = (mytext_cmp_internal(min_val, query_val, collid) <= 0);
                break;
            case BTEqualStrategyNumber: // =
                result = (mytext_cmp_internal(min_val, query_val, collid) <= 0 &&
                          mytext_cmp_internal(max_val, query_val, collid) >= 0);
                break;
            case BTGreaterEqualStrategyNumber: // >=
                result = (mytext_cmp_internal(max_val, query_val, collid) >= 0);
                break;
            case BTGreaterStrategyNumber: // >
                result = (mytext_cmp_internal(max_val, query_val, collid) > 0);
                break;
            default:
                result = false;
        }

        olog(DEBUG1, "BRIN check: strategy=%d, query='%s', min='%s', max='%s', result=%d",
             key->sk_strategy, text_to_cstring(query_val), text_to_cstring(min_val),
             text_to_cstring(max_val), result);
    } else if (column->bv_hasnulls) { // 如果有NULL值，根据操作符类型判断
        switch (key->sk_strategy) {
            case BTEqualStrategyNumber:
                // 只有查询条件明确包含IS NULL时才返回true
                result = false;
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
    bool updated = false;

    // 合并最小/最大值
    if (!col_a->bv_hasnulls && !col_b->bv_hasnulls) {
        // 确保值是可比较的
        mytext *a_min = (mytext *)DatumGetPointer(col_a->bv_values[0]);
        mytext *a_max = (mytext *)DatumGetPointer(col_a->bv_values[1]);
        mytext *b_min = (mytext *)DatumGetPointer(col_b->bv_values[0]);
        mytext *b_max = (mytext *)DatumGetPointer(col_b->bv_values[1]);

        // 更新最小值
        if (mytext_cmp_internal(a_min, b_min, collid) > 0) {
            col_a->bv_values[0] = col_b->bv_values[0];
            updated = true;
        }

        // 更新最大值
        if (mytext_cmp_internal(a_max, b_max, collid) < 0) {
            col_a->bv_values[1] = col_b->bv_values[1];
            updated = true;
        }
    } else if (col_b->bv_hasnulls) {
        if (!col_a->bv_hasnulls) {
            col_a->bv_hasnulls = true;
            updated = true;
        }
    }

    PG_RETURN_BOOL(updated);
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
    AclResult result = ACLCHECK_OK;

    /* 新增：使用 options 变量进行选项解析 */
    List *option_list = (List *)DatumGetPointer(options);
    ListCell *lc;

    foreach(lc, option_list) {
        Node *node = (Node *)lfirst(lc);

        /* 根据实际选项类型进行处理 */
        if (IsA(node, DefElem)) {
            DefElem *def = (DefElem *)node;

            if (strcmp(def->defname, "compress_mode") == 0) {
                /* 处理压缩模式选项 */
            }
            /* 其他选项处理... */
        }
    }

    Assert(result == ACLCHECK_OK);

    PG_RETURN_VOID();
}

/*
 * GiST 一致性检查函数
 * 检查查询是否与索引项匹配
 */
PG_FUNCTION_INFO_V1(mytext_gist_consistent);
Datum mytext_gist_consistent(PG_FUNCTION_ARGS) {
    GISTENTRY *entry = (GISTENTRY *)PG_GETARG_POINTER(0);
    mytext *query = PG_GETARG_TEXT_P(1);
    StrategyNumber strategy = (StrategyNumber)PG_GETARG_UINT16(2);
    bool *recheck = (bool *)PG_GETARG_POINTER(4);
    mytext *key = (mytext *)DatumGetPointer(entry->key);
    int cmp;

    // 总是需要重新检查，因为 GiST 提供的是近似结果
    *recheck = true;

    if (GIST_LEAF(entry)) {
        // 叶子节点：存储实际值
        switch (strategy) {
            case BTLessStrategyNumber:           // 策略号 1: <
                cmp = mytext_cmp_internal(key, query, PG_GET_COLLATION());
                PG_RETURN_BOOL(cmp < 0);
            case BTLessEqualStrategyNumber:      // 策略号 2: <=
                cmp = mytext_cmp_internal(key, query, PG_GET_COLLATION());
                PG_RETURN_BOOL(cmp <= 0);
            case BTEqualStrategyNumber:          // 策略号 3: =
                cmp = mytext_cmp_internal(key, query, PG_GET_COLLATION());
                PG_RETURN_BOOL(cmp == 0);
            case BTGreaterEqualStrategyNumber:   // 策略号 4: >=
                cmp = mytext_cmp_internal(key, query, PG_GET_COLLATION());
                PG_RETURN_BOOL(cmp >= 0);
            case BTGreaterStrategyNumber:        // 策略号 5: >
                cmp = mytext_cmp_internal(key, query, PG_GET_COLLATION());
                PG_RETURN_BOOL(cmp > 0);
            default:
                elog(ERROR, "Unsupported strategy number: %d", strategy);
                PG_RETURN_BOOL(false);
        }
    } else {
        // 内部节点：总是返回 true 让执行器进行精确检查
        PG_RETURN_BOOL(true);
    }
}

/*
 * GiST 联合函数
 * 合并多个索引项
 */
PG_FUNCTION_INFO_V1(mytext_gist_union);
Datum mytext_gist_union(PG_FUNCTION_ARGS) {
    GistEntryVector *entryvec = (GistEntryVector *)PG_GETARG_POINTER(0);
    GISTENTRY *ent = entryvec->vector;
    int numranges = entryvec->n;
    mytext *min = NULL, *max = NULL;
    int i;

    for (i = 0; i < numranges; i++) {
        mytext *key = (mytext *)DatumGetPointer(ent[i].key);

        if (min == NULL || mytext_cmp_internal(key, min, PG_GET_COLLATION()) < 0) {
            min = key;
        }
        if (max == NULL || mytext_cmp_internal(key, max, PG_GET_COLLATION()) > 0) {
            max = key;
        }
    }

    // 返回范围的最小值作为联合结果
    // 注意：实际应用中可能需要更复杂的逻辑
    PG_RETURN_POINTER(min);
}

/*
 * GiST 压缩函数
 * 将数据转换为索引内部格式
 */
PG_FUNCTION_INFO_V1(mytext_gist_compress);
Datum mytext_gist_compress(PG_FUNCTION_ARGS) {
    GISTENTRY *entry = (GISTENTRY *)PG_GETARG_POINTER(0);

    if (entry->leafkey) {
        // 叶子节点：解压 TOAST 并存储原始值
        mytext *raw_value = (mytext *)DatumGetPointer(entry->key);
        mytext *detoasted = PG_DETOAST_DATUM_PACKED(raw_value);

        // 创建新的 GISTENTRY，存储解压后的值
        GISTENTRY *retval = (GISTENTRY *)palloc(sizeof(GISTENTRY));
        gistentryinit(*retval, PointerGetDatum(detoasted),
                      entry->rel, entry->page, entry->offset, false);
        PG_RETURN_POINTER(retval);
    }

    // 非叶子节点直接返回
    PG_RETURN_POINTER(entry);
}

/*
 * GiST 解压缩函数
 * 从索引格式还原数据
 */
PG_FUNCTION_INFO_V1(mytext_gist_decompress);
Datum mytext_gist_decompress(PG_FUNCTION_ARGS) {
    // 我们直接存储 mytext 值，无需额外处理
    PG_RETURN_POINTER(PG_GETARG_POINTER(0));
}

/*
 * GiST 惩罚函数
 * 计算将新项插入到已有项中的代价
 */
PG_FUNCTION_INFO_V1(mytext_gist_penalty);
Datum mytext_gist_penalty(PG_FUNCTION_ARGS) {
    GISTENTRY *origentry = (GISTENTRY *)PG_GETARG_POINTER(0);
    GISTENTRY *newentry = (GISTENTRY *)PG_GETARG_POINTER(1);
    float *penalty = (float *)PG_GETARG_POINTER(2);
    mytext *orig = (mytext *)DatumGetPointer(origentry->key);
    mytext *new = (mytext *)DatumGetPointer(newentry->key);

    // 简单实现：基于字符串长度的差异
    int orig_len = VARSIZE_ANY_EXHDR(orig);
    int new_len = VARSIZE_ANY_EXHDR(new);
    *penalty = abs(orig_len - new_len) * 0.1f;

    PG_RETURN_POINTER(penalty);
}

/*
 * GiST 分裂函数
 * 决定如何分裂一个满的索引页
 */
PG_FUNCTION_INFO_V1(mytext_gist_picksplit);
Datum mytext_gist_picksplit(PG_FUNCTION_ARGS) {
    GistEntryVector *entryvec = (GistEntryVector *)PG_GETARG_POINTER(0);
    GIST_SPLITVEC *v = (GIST_SPLITVEC *)PG_GETARG_POINTER(1);
    OffsetNumber maxoff = entryvec->n - 1;
    OffsetNumber *left, *right;
    int nleft, nright;
    int i;

    // 简单实现：按字母顺序分成两半
    left = v->spl_left = (OffsetNumber *)palloc(maxoff * sizeof(OffsetNumber));
    right = v->spl_right = (OffsetNumber *)palloc(maxoff * sizeof(OffsetNumber));
    nleft = nright = 0;

    for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i)) {
        mytext *key = (mytext *)DatumGetPointer(entryvec->vector[i].key);

        // 简单分裂：根据字符串首字母
        char first_char = VARDATA(key)[0];

        if (first_char < 'n') {
            left[nleft++] = i;
        } else {
            right[nright++] = i;
        }
    }

    v->spl_nleft = nleft;
    v->spl_nright = nright;

    // 选择第一个和最后一个作为左右集合的代表
    if (nleft > 0) {
        v->spl_ldatum = entryvec->vector[left[0]].key;
    }
    if (nright > 0) {
        v->spl_rdatum = entryvec->vector[right[0]].key;
    }

    PG_RETURN_POINTER(v);
}

/*
 * GiST 相等函数
 * 比较两个键值是否相等
 */
PG_FUNCTION_INFO_V1(mytext_gist_same);
Datum mytext_gist_same(PG_FUNCTION_ARGS) {
    mytext *key1 = (mytext *)PG_GETARG_POINTER(0);
    mytext *key2 = (mytext *)PG_GETARG_POINTER(1);
    bool *result = (bool *)PG_GETARG_POINTER(2);

    *result = (mytext_cmp_internal(key1, key2, PG_GET_COLLATION()) == 0);
    olog(DEBUG1, "result=%d", *result);
    PG_RETURN_POINTER(result);
}

/*
 * GiST 提取函数
 * 从索引项中提取原始值（仅用于索引扫描）
 */
PG_FUNCTION_INFO_V1(mytext_gist_fetch);
Datum mytext_gist_fetch(PG_FUNCTION_ARGS) {
    GISTENTRY *entry = (GISTENTRY *)PG_GETARG_POINTER(0);
    mytext *key = (mytext *)DatumGetPointer(entry->key);
    GISTENTRY *retval = (GISTENTRY *)palloc(sizeof(GISTENTRY));

    gistentryinit(*retval, PointerGetDatum(key),
                  entry->rel, entry->page, entry->offset, false);

    PG_RETURN_POINTER(retval);
}

typedef enum {
    GIN_CMP_DEFAULT = 0,
    GIN_CMP_INCLUDED,
    GIN_CMP_EQUAL_TO,
    GIN_CMP_LIKE,
    GIN_CMP_REGULAR
}GinCmpStrategy;

/*
 * GIN 提取值函数
 * 从文档中提取键值
 */
PG_FUNCTION_INFO_V1(mytext_gin_extract_value);
Datum mytext_gin_extract_value(PG_FUNCTION_ARGS) {
    mytext *value = PG_GETARG_TEXT_P(0);
    int32 *nkeys = (int32 *)PG_GETARG_POINTER(1);
    bool **nullFlags = (bool **)PG_GETARG_POINTER(2);

    // 确保输入有效
    if (value == NULL) {
        *nkeys = 0;
        *nullFlags = NULL;
        PG_RETURN_POINTER(NULL);
    }

    // 分配键数组
    Datum *keys = (Datum *)palloc(sizeof(Datum));
    *nkeys = 1;

    // 分配空标志数组
    *nullFlags = (bool *)palloc(sizeof(bool));
    (*nullFlags)[0] = false;

    // 直接存储整个值作为键
    keys[0] = PointerGetDatum(value);
    olog(DEBUG1, "nkeys=%d", *nkeys);

    PG_RETURN_POINTER(keys);
}

// 提取LIKE模式中的关键词
static int extract_like_keywords(text *pattern, Datum **keys) {
    char *pat = text_to_cstring(pattern);
    int count = 0;
    char *p = pat;

    // 特殊处理：如果模式以通配符开头
    // bool startsWithWildcard = (*p == '%');

    while (*p) {
        // 跳过通配符
        while (*p == '%' || *p == '_') p++;

        // 查找下一个通配符或字符串结束
        char *start = p;
        while (*p && *p != '%' && *p != '_') p++;

        // 提取关键词
        int len = p - start;
        if (len >= 2) { // 只考虑长度大于等于2的关键词
            char *keyword = palloc(len + 1);
            strncpy(keyword, start, len);
            keyword[len] = '\0';

            *keys = repalloc(*keys, (count + 1) * sizeof(Datum));
            (*keys)[count] = PointerGetDatum(cstring_to_text(keyword));
            count++;

            olog(DEBUG1, "Extracted LIKE key: %s", keyword);
        }
    }

    // 如果没有提取到关键词，使用整个模式作为关键词
    if (count == 0 && strlen(pat) > 0) {
        // 移除前导通配符
        char *cleanPat = pat;
        while (*cleanPat == '%' || *cleanPat == '_') cleanPat++;

        if (strlen(cleanPat) > 0) {
            *keys = (Datum *)palloc(sizeof(Datum));
            (*keys)[0] = PointerGetDatum(cstring_to_text(cleanPat));
            count = 1;
            olog(DEBUG1, "Using full LIKE pattern as key: %s", cleanPat);
        }
    }

    pfree(pat);
    return count;
}

// 提取正则表达式中的关键元素
static int extract_regex_keys(text *regex, Datum **keys) {
    char *pattern = text_to_cstring(regex);
    int count = 0;
    char *p = pattern;

    // 跳过正则表达式的特殊字符
    while (*p && (ispunct((unsigned char)*p) && *p != ' ')) {
        p++;
    }

    // 提取连续的字母数字序列
    while (*p) {
        if (isalnum((unsigned char)*p) || *p == ' ') {
            char buf[256];
            int len = 0;

            // 提取连续字母数字和空格
            while ((isalnum((unsigned char)*p) || *p == ' ') && len < 255) {
                buf[len++] = *p++;
            }
            buf[len] = '\0';

            // 添加长度大于等于2的关键词
            if (len >= 2) {
                *keys = repalloc(*keys, (count + 1) * sizeof(Datum));
                (*keys)[count] = PointerGetDatum(cstring_to_text(buf));
                count++;

                olog(DEBUG1, "Extracted regex key: %s", buf);
            }
        } else {
            p++;
        }
    }

    // 如果没有提取到关键词，使用整个正则表达式作为关键词
    if (count == 0 && strlen(pattern) > 0) {
        *keys = (Datum *)palloc(sizeof(Datum));
        (*keys)[0] = PointerGetDatum(cstring_to_text(pattern));
        count = 1;
        olog(DEBUG1, "Using full regex as key: %s", pattern);
    }

    pfree(pattern);
    return count;
}

/*************************************************************
GIN 索引调用堆栈：

一、创建索引：CREATE INDEX mytext_value_gin_idx ON test_mytext USING gin (value);
ginbuild
|-- table_index_build_scan
    |-- heapam_index_build_range_scan
    	|-- ginBuildCallback
            // 多次调用如下流程
    		|-- ginHeapTupleBulkInsert
    			|-- ginExtractEntries // 从单个索引列的值中提取需要构建索引的键（key）列表
    			|	|-- FunctionCall3Coll
    			|		|-- mytext_gin_extract_value
    			|-- ginInsertBAEntries // 将键（key）列表逐个插入 GIN 索引
    				|-- ginInsertBAEntry
    					|-- rbt_insert
    						|-- cmpEntryAccumulator // 比较关键词以确定其在 B-tree 中的位置
    							|-- ginCompareAttEntries
    								|-- ginCompareEntries
										|-- FunctionCall2Coll
											|-- mytext_gin_compare
// while 循环调用
|-- ginEntryInsert // 将内存数据持久化到索引结构
    |-- entryLocateLeafEntry
    	|-- ginCompareAttEntries
    		|-- ginCompareEntries
    			|-- FunctionCall2Coll
    				|-- mytext_gin_compare

二、查询：SELECT * FROM test_mytext WHERE value ~~ '%sql%'; -- LIKE操作符
（1）提取查询条件（planner）：
exec_simple_query
|-- pg_plan_queries
	|-- pg_plan_query
        |-- planner
			|-- pgss_planner
        		|-- standard_planner
        			|-- subquery_planner
        				|-- grouping_planner
        					|-- query_planner
query_planner
|-- make_one_rel
	|-- set_base_rel_pathlists
        |-- set_rel_pathlist
			|-- set_plain_rel_pathlist
        		|-- create_index_paths
        			|-- get_index_paths
						|-- build_index_paths
        					|-- create_index_path
        						|-- cost_index
        							|-- gincostestimate
        								|-- gincost_opexpr
        									|-- gincost_pattern
        										|-- FunctionCall7Coll
        											|-- mytext_gin_extract_query
（2）调用操作符（executor）：
exec_simple_query
|-- PortalRun
    |-- PortalRunSelect
        |-- ExecutorRun
            |-- pgss_ExecutorRun
                |-- standard_ExecutorRun
                    |-- ExecutePlan
                        |-- ExecProcNode
                            |-- ExecProcNodeFirst
                                |-- ExecSeqScan
                                    |- ExecScan
                                        |-- ExecQual
                                            |-- ExecEvalExprSwitchContext
                                                |-- ExecInterpExprStillValid
                                                    |-- ExecInterpExpr
                                                        |-- mytext_gin_like
 *************************************************************/

/*
 * GIN 提取查询函数
 * 从查询中提取键值
 */
PG_FUNCTION_INFO_V1(mytext_gin_extract_query);
Datum mytext_gin_extract_query(PG_FUNCTION_ARGS) {
    mytext *query = PG_GETARG_TEXT_P(0); // 查询值
    int32 *nkeys = (int32 *)PG_GETARG_POINTER(1); // 输出键数指针
    StrategyNumber strategy = PG_GETARG_UINT16(2); // 策略号
    bool **partialMatch = (bool **)PG_GETARG_POINTER(3); // 部分匹配标志指针
#if 0
    Pointer **extraData = (Pointer **)PG_GETARG_POINTER(4); // 额外数据指针
#endif
    Datum *keys = NULL;

    olog(DEBUG1, "query=%s, strategy=%d", text_to_cstring(query), strategy);

    *nkeys = 0;
    switch (strategy) {
        case GIN_CMP_INCLUDED: // @> 包含
        case GIN_CMP_EQUAL_TO: // == 等于
            *nkeys = 1;
            keys = (Datum *)palloc(sizeof(Datum));
            keys[0] = PointerGetDatum(query);
            break;
        case GIN_CMP_LIKE: // ~~ LIKE
            keys = (Datum *)palloc(0); // 初始为空数组
            *nkeys = extract_like_keywords(query, &keys);
            break;
        case GIN_CMP_REGULAR: // ~ 正则
            keys = (Datum *)palloc(0); // 初始为空数组
            *nkeys = extract_regex_keys(query, &keys);
            break;
        default:
            elog(ERROR, "Unsupported strategy number: %d", strategy);
    }

    olog(DEBUG1, "nkeys=%d", *nkeys);

    // 设置部分匹配标志
    if (partialMatch && *nkeys > 0) {
        *partialMatch = (bool *)palloc(sizeof(bool) * *nkeys);
        for (int i = 0; i < *nkeys; i++) {
            (*partialMatch)[i] = (strategy == GIN_CMP_LIKE || strategy == GIN_CMP_REGULAR); // LIKE和正则需要部分匹配
        }
    }

#if 0
    // TODO: 无法通过 fcinfo->flinfo->fn_extra（总为 NULL）将策略号传递给 mytext_gin_compare 函数

    // 设置额外数据 - 存储策略号
    if (extraData && *nkeys > 0) {
        *extraData = (Pointer *)palloc(sizeof(Pointer) * *nkeys);
        for (int i = 0; i < *nkeys; i++) {
            StrategyNumber *snp = (StrategyNumber *)palloc(sizeof(StrategyNumber));
            *snp = strategy;
            (*extraData)[i] = (Pointer)snp;
        }
    }
#endif

    PG_RETURN_POINTER(keys);
}

/*
 * GIN 一致性检查函数
 * 检查查询是否与索引项匹配
 */
PG_FUNCTION_INFO_V1(mytext_gin_consistent);
Datum mytext_gin_consistent(PG_FUNCTION_ARGS) {
    bool *check = (bool *)PG_GETARG_POINTER(0);
    StrategyNumber strategy = PG_GETARG_UINT16(1);
    mytext *query = PG_GETARG_TEXT_P(2);
    int32 nkeys = PG_GETARG_INT32(3); // 注意：这里是值传递，不是指针
    // Pointer *extra_data = (Pointer *)PG_GETARG_POINTER(4); // 暂不使用，仅占位
    // true: 索引扫描返回的是"可能匹配"的结果，需要进一步验证；
    // false: 索引扫描返回的是精确匹配的结果，无需额外验证
    bool *recheck = (bool *)PG_GETARG_POINTER(5);
    bool res;

    if(recheck) *recheck = true;

    // 确保参数有效
    if (check == NULL || nkeys < 0)
        PG_RETURN_BOOL(false);
    if (nkeys == 0)
        PG_RETURN_BOOL(true);

    switch (strategy) {
        case GIN_CMP_INCLUDED: // @> 包含
            if (recheck) *recheck = true; // 需要重新检查
            // 只要有一个键匹配就返回true
            for (int i = 0; i < nkeys; i++) {
                if (check[i]) {
                    res = true;
                    break;
                }
            }
            break;
        case GIN_CMP_EQUAL_TO: // == 等于
            if (recheck) *recheck = false; // 等值查询不需要重新检查
            res = (nkeys > 0 && check[0]); // 第一个键匹配即可
            break;
        case GIN_CMP_LIKE: // ~~ LIKE
            if (recheck) *recheck = true; // 需要重新检查
            res = true; // 所有键必须存在
            for (int i = 0; i < nkeys; i++) {
                if (!check[i]) {
                    res = false;
                    break;
                }
            }
            break;
        case GIN_CMP_REGULAR: // ~ 正则
            if (recheck) *recheck = true; // 需要重新检查
            res = true; // 至少一个键存在
            for (int i = 0; i < nkeys; i++) {
                if (check[i]) {
                    res = true;
                    break;
                }
            }
            break;
        default:
            elog(ERROR, "Unsupported GIN strategy number: %d", strategy);
    }
    olog(DEBUG1, "query=%s, strategy=%d, nkeys=%d, res=%d", text_to_cstring(query), strategy, nkeys, res);

    PG_RETURN_BOOL(res);
}

/*
 * GIN 比较函数
 * 比较两个键值
 */
PG_FUNCTION_INFO_V1(mytext_gin_compare);
Datum mytext_gin_compare(PG_FUNCTION_ARGS) {
    mytext *a = (mytext *)PG_GETARG_TEXT_P(0);
    mytext *b = (mytext *)PG_GETARG_TEXT_P(1);
    int cmp = 0;

#if 0
    // TODO: 比较函数内部无法获取策略号，也无法通过 fcinfo->flinfo->fn_extra（总为 NULL）传递

    // 获取策略号（从 extra_data）
    StrategyNumber strategy = GIN_CMP_INCLUDED;
    if (fcinfo->flinfo && fcinfo->flinfo->fn_extra) {
        GinScanKey key = (GinScanKey) fcinfo->flinfo->fn_extra;
        if (key->extra_data) {
            strategy = *((StrategyNumber *)key->extra_data);
        }
    }

    olog(DEBUG1, "strategy: %d" strategy);

    // 根据不同操作符使用不同比较逻辑
    switch (strategy) {
        case GIN_CMP_INCLUDED: // @> 包含
        case GIN_CMP_LIKE: // ~~ LIKE
        case GIN_CMP_REGULAR: // ~ 正则
            // 对于这些操作符，只需要检查键是否存在，不需要精确比较
            cmp = 0; // 总是返回"相等"
            break;
        case GIN_CMP_EQUAL_TO: // == 等于
            // 精确比较
            cmp = mytext_cmp_internal(a, b, PG_GET_COLLATION());
            break;
        default:
            // 默认精确比较
            cmp = mytext_cmp_internal(a, b, PG_GET_COLLATION());
    }
#endif

    olog(DEBUG1, "a=%s, b=%s, cmp=%d", text_to_cstring(a), text_to_cstring(b), cmp);

    PG_RETURN_INT32(cmp);
}

/*
 * 包含操作符函数
 * 实现 @> 操作符的功能
 */
PG_FUNCTION_INFO_V1(mytext_gin_contains);
Datum mytext_gin_contains(PG_FUNCTION_ARGS) {
    mytext *str = PG_GETARG_TEXT_P(0);
    mytext *substr = PG_GETARG_TEXT_P(1);
    char *str_data = VARDATA_ANY(str);
    char *substr_data = VARDATA_ANY(substr);
    int str_len = VARSIZE_ANY_EXHDR(str);
    int substr_len = VARSIZE_ANY_EXHDR(substr);
    char *found;

    // 处理空字符串
    if (substr_len == 0) {
        PG_RETURN_BOOL(true);
    }

    if (str_len < substr_len) {
        PG_RETURN_BOOL(false);
    }

    // 处理特殊情况：str_data = 'hello', substr_data = 'hell@', substr_len = 4
    char *substr_data2 = (char *)palloc0(substr_len + 1);
    strncpy(substr_data2, substr_data, substr_len);
    found = strstr(str_data, substr_data2);
    olog(DEBUG1, "str=%s, substr=%s, address=%p", text_to_cstring(str),
         text_to_cstring(substr), found);
    pfree(substr_data2);

    PG_RETURN_BOOL(found != NULL);
}

/* 等于操作符实现 */
PG_FUNCTION_INFO_V1(mytext_gin_equals);
Datum mytext_gin_equals(PG_FUNCTION_ARGS) {
    mytext *a = PG_GETARG_TEXT_P(0);
    mytext *b = PG_GETARG_TEXT_P(1);

    int cmp = mytext_cmp_internal(a, b, PG_GET_COLLATION());
    olog(DEBUG1, "a=%s, b=%s, cmp=%d", text_to_cstring(a), text_to_cstring(b), cmp);

    PG_RETURN_BOOL(cmp == 0);
}

/* LIKE操作符实现（使用内置 textlike 函数） */
PG_FUNCTION_INFO_V1(mytext_gin_like);
Datum mytext_gin_like(PG_FUNCTION_ARGS) {
    mytext *str = PG_GETARG_TEXT_P(0);
    mytext *pattern = PG_GETARG_TEXT_P(1);

    // 使用内置 textlike 函数
    Datum res = DirectFunctionCall2Coll(textlike,
                                        PG_GET_COLLATION(),
                                        PointerGetDatum(str),
                                        PointerGetDatum(pattern));
    olog(DEBUG1, "str=%s, pattern=%s, res=%d", text_to_cstring(str),
         text_to_cstring(pattern), DatumGetBool(res));

    PG_RETURN_BOOL(DatumGetBool(res));
}

/* 正则匹配操作符实现（使用内置 regexp_match 函数） */
PG_FUNCTION_INFO_V1(mytext_gin_regex);
Datum mytext_gin_regex(PG_FUNCTION_ARGS) {
    mytext *str = PG_GETARG_TEXT_P(0);
    mytext *pattern = PG_GETARG_TEXT_P(1);

    // 使用内置 regexp_match 函数
    Datum res = DirectFunctionCall2Coll(regexp_match,
                                        PG_GET_COLLATION(),
                                        PointerGetDatum(str),
                                        PointerGetDatum(pattern));
    olog(DEBUG1, "str=%s, pattern=%s, res=%d", text_to_cstring(str),
         text_to_cstring(pattern), DatumGetBool(res));

    PG_RETURN_BOOL(DatumGetBool(res));
}
