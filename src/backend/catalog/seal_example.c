/*-------------------------------------------------------------------------
 *
 * seal_example.c
 *	  routines to support manipulation of the seal_example relation
 *
 * IDENTIFICATION
 *	  src/backend/catalog/seal_example.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/table.h"
#include "access/genam.h"
#include "access/heapam.h"

#include "catalog/catalog.h"
#include "catalog/seal_example.h"
#include "nodes/execnodes.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"

/*
 * 注意事项：
 * 1. SysCache 要求缓存的索引列必须具备唯一约束；
 * 2. 本文中函数示例只实现了增删改查一条元组。
 */

void CacheLookupSealExampleTuple(int32 number, Form_seal_example **form_ptr,
                                 HeapTuple **tuple_ptr, bool release_tuple);

/*
 * 向seal_example表中插入一条记录，
 * 其调用的CatalogTupleInsert函数不涉及SysCache系统表缓存，除非需要查询依赖的其他系统表记录。
 */
void InsertSealExampleTuple(int32 number, char *tname) {
    Relation rel;
    bool nulls[Natts_seal_example] = {0};
    Datum values[Natts_seal_example];
    int i = 0;

    values[i++] = GetNewObjectId();
    values[i++] = number;
    values[i++] = CStringGetTextDatum(tname);

    rel = table_open(SealExampleRelationId, RowExclusiveLock);
    HeapTuple tuple = heap_form_tuple(rel->rd_att, values, nulls);

    // 该函数内部会查找Buffer读取及可见性处理，也会标记缓存失效，调用关系为：
    // CatalogTupleInsert --> heap_insert --> CacheInvalidateHeapTuple(relation, heaptup, NULL);
    CatalogTupleInsert(rel, tuple);

    elog(INFO, "[%s] insert one tuple[oid = %d, number = %d, tname = %s] in seal_example [%d] success",
         __func__, values[0], number, tname, SealExampleRelationId);

#ifdef CACHE_INVALID // 无需主动调用，仅记录
#ifdef CACHE_INVALID_RELATION
    CacheInvalidateCatalog(SealExampleRelationId); // 同步缓存：表级别
#else
    CacheInvalidateRelcacheByTuple(tuple); // 同步缓存：行级别
#endif
#endif

    heap_freetuple(tuple);
    table_close(rel, RowExclusiveLock);
}

/*
 * 在seal_example系统表中查找number列等于入参number的记录，将其number列更新为new_number。
 * 本函数适用于更新多条记录的场景，但由于number列有唯一约束，因此只会更新一条记录。
 * 本函数未使用SysCache加速记录的查找，而是直接扫描表数据文件。
 */
void UpdateSealExampleTupleByTableScan(int32 number, int32 new_number) {
    Relation rel;
    HeapTuple tuple, new_tuple;
    SysScanDesc scan;
    ScanKeyData key[1];

    rel = table_open(SealExampleRelationId, RowExclusiveLock);

    // 扫描条件：number列 等于 入参number
    ScanKeyInit(&key[0], Anum_seal_example_number,
                BTEqualStrategyNumber /* Equality strategy number */,
                F_INT4EQ /* Comparison operator */, number);
    scan = systable_beginscan(rel, SealExampleNumberIndexId, true, NULL, 1, key);
    // 若表中存在多行满足查找条件的记录，可使用 while 循环持续查找；本示例中 number 列有唯一约束，只会调用一次。
    while (HeapTupleIsValid((tuple = systable_getnext(scan)))) {
        // 拷贝新 tuple，修改其值，然后更新
        new_tuple = heap_copytuple(tuple); // 原 tuple 无需 free
        Form_seal_example form = (Form_seal_example)GETSTRUCT(new_tuple);
        form->number = new_number;

        // 该函数内部会查找 Buffer 读取及可见性判断，也会标记缓存失效，调用关系为：
        // CatalogTupleUpdate --> simple_heap_update --> heap_update --> CacheInvalidateHeapTuple(relation, &oldtup, heaptup);
        CatalogTupleUpdate(rel, &new_tuple->t_self, new_tuple);

        elog(INFO, "[%s] update one tuple [old_number = %d, new_number = %d] in seal_example [%d] success",
             __func__, number, new_number, SealExampleRelationId);

#ifdef CACHE_INVALID // 无需主动调用，仅记录
#ifndef CACHE_INVALID_RELATION
    CacheInvalidateRelcacheByTuple(tuple); // 同步缓存：行级别
#endif
#endif

        heap_freetuple(new_tuple);
    }

#ifdef CACHE_INVALID // 无需主动调用，仅记录
#ifdef CACHE_INVALID_RELATION
    CacheInvalidateCatalog(SealExampleRelationId); // 同步缓存：表级别
#endif
#endif

    systable_endscan(scan);
    table_close(rel, RowExclusiveLock);
}

/*
 * 从seal_example系统表中删除number列等于入参number的记录。
 * 本函数适用于删除多条记录的场景，但由于number列有唯一约束，因此只会删除一条记录。
 * 本函数未使用SysCache加速记录的查找，而是直接扫描表数据文件。
 */
void DeleteSealExampleTupleByTableScan(int32 number) {
    HeapTuple tuple;
    Relation rel;
    SysScanDesc scan;
    ScanKeyData key[1];

    rel = table_open(SealExampleRelationId, RowExclusiveLock);

    // 扫描条件：number列等于入参number
    ScanKeyInit(&key[0], Anum_seal_example_number,
                BTEqualStrategyNumber /* Equality strategy number */,
                F_INT4EQ /* Comparison operator */, number);
    scan = systable_beginscan(rel, SealExampleNumberIndexId, true, NULL, 1, key); // snapshot为NULL，函数内部会自动生成快照信息
    // 若表中存在多行满足查找条件的记录，可使用 while 循环持续查找；本示例中 number 列有唯一约束，只会调用一次。
    while (HeapTupleIsValid((tuple = systable_getnext(scan)))) {
        // 该函数内部会查找 Buffer 读取及可见性判断，也会标记缓存失效，调用关系为：
        // CatalogTupleDelete --> simple_heap_delete --> heap_delete --> CacheInvalidateHeapTuple(relation, &tp, NULL);
        CatalogTupleDelete(rel, &tuple->t_self);

        elog(INFO, "[%s] delete one tuple by number [%d] in seal_example [%d] success",
             __func__, number, SealExampleRelationId);

#ifdef CACHE_INVALID // 无需主动调用，仅记录
#ifndef CACHE_INVALID_RELATION
    CacheInvalidateRelcacheByTuple(tuple); // 同步缓存：行级别
#endif
#endif
    }

#ifdef CACHE_INVALID // 无需主动调用，仅记录
#ifdef CACHE_INVALID_RELATION
    CacheInvalidateCatalog(SealExampleRelationId); // 同步缓存：表级别
#endif
#endif

    systable_endscan(scan);
    table_close(rel, RowExclusiveLock);
}

/*
 * 在seal_example系统表中查找number列等于入参number的记录，将其number列更新为new_number。
 * 本函数使用SysCache加速记录的查找，若缓存未命中，会扫描表的磁盘文件，并将读到的行数据加入SysCache缓存。
 * 只有满足唯一性约束的列才能注册到SysCache，因此本函数只能更新一条记录，
 * 若需更新多条记录，请使用或借鉴UpdateSealExampleTupleByTableScan函数。
 */
void UpdateSealExampleTupleByCacheLookup(int32 number, int32 new_number) {
    HeapTuple tuple = NULL;
    Form_seal_example form = NULL;
    CacheLookupSealExampleTuple(number, &form, &tuple, false);
    if (!form) { // 缓存未命中，且表中不存在该记录
        elog(ERROR, "[%s] cache lookup seal_example by number [%d] failed, not found", __func__, number);
        return;
    }

    Relation rel = table_open(SealExampleRelationId, RowExclusiveLock);
    if (HeapTupleIsValid(tuple)) {
        // 拷贝新 tuple，修改其值，然后更新
        HeapTuple new_tuple = heap_copytuple(tuple); // 原 tuple 无需 free
        Form_seal_example form = (Form_seal_example)GETSTRUCT(new_tuple);
        form->number = new_number;

        // 该函数内部会查找 Buffer 读取及可见性判断，也会标记缓存失效，调用关系为：
        // CatalogTupleUpdate --> simple_heap_update --> heap_update --> CacheInvalidateHeapTuple(relation, &oldtup, heaptup);
        CatalogTupleUpdate(rel, &new_tuple->t_self, new_tuple);

        elog(INFO, "[%s] update one tuple [old_number = %d, new_number = %d] in seal_example [%d] by cache lookup success",
             __func__, number, new_number, SealExampleRelationId);

#ifdef CACHE_INVALID // 无需主动调用，仅记录
#ifndef CACHE_INVALID_RELATION
    CacheInvalidateRelcacheByTuple(tuple); // 同步缓存：行级别
#endif
#endif
        ReleaseSysCache(tuple); // 通过查找缓存获取的tuple，需要Release减少其引用计数
        heap_freetuple(new_tuple);
    }

#ifdef CACHE_INVALID // 无需主动调用，仅记录
#ifdef CACHE_INVALID_RELATION
    CacheInvalidateCatalog(SealExampleRelationId); // 同步缓存：表级别
#endif
#endif
    table_close(rel, RowExclusiveLock);
}

/*
 * 从seal_example系统表中删除number列等于入参number的记录。
 * 本函数使用SysCache加速记录的查找，若缓存未命中，会扫描表的磁盘文件，并将读到的行数据加入SysCache缓存。
 * 只有满足唯一性约束的列才能注册到SysCache，因此本函数只能删除一条记录，
 * 若需删除多条记录，请使用或借鉴DeleteSealExampleTupleByTableScan函数。
 */
void DeleteSealExampleTupleByCacheLookup(int32 number) {
    HeapTuple tuple = NULL;
    Form_seal_example form = NULL;
    CacheLookupSealExampleTuple(number, &form, &tuple, false);
    if (!form) { // 缓存未命中，且表中不存在该记录
        elog(ERROR, "[%s] cache lookup seal_example by number [%d] failed, not found", __func__, number);
        return;
    }

    Relation rel = table_open(SealExampleRelationId, RowExclusiveLock);
    if (HeapTupleIsValid(tuple)) {
        // 该函数内部会查找 Buffer 读取及可见性判断，也会标记缓存失效，调用关系为：
        // CatalogTupleDelete --> simple_heap_delete --> heap_delete --> CacheInvalidateHeapTuple(relation, &tp, NULL);
        CatalogTupleDelete(rel, &tuple->t_self);
        ReleaseSysCache(tuple); // 通过缓存查找的tuple，需要Release减少其引用计数
        // heap_freetuple(tuple); // 通过表扫描获取的tuple，不需要Release

        elog(INFO, "[%s] delete one tuple [number = %d] in seal_example [%d] success",
             __func__, number, SealExampleRelationId);

#ifdef CACHE_INVALID // 无需主动调用，仅记录
#ifndef CACHE_INVALID_RELATION
    CacheInvalidateRelcacheByTuple(tuple); // 同步缓存：行级别
#endif
#endif
    }

#ifdef CACHE_INVALID // 无需主动调用，仅记录
#ifdef CACHE_INVALID_RELATION
    CacheInvalidateCatalog(SealExampleRelationId); // 同步缓存：表级别
#endif
#endif
    table_close(rel, RowExclusiveLock);
}

void CacheLookupSealExampleTuple(int32 number, Form_seal_example **form_ptr,
                                 HeapTuple **tuple_ptr, bool release_tuple) {
    if (RelationHasSysCache(SealExampleRelationId)) { // 当前系统表是否位于缓存中
        // SearchSysCache1 函数中，如果缓存未命中，会调用 SearchCatCacheMiss 函数扫描磁盘中的表数据，
        // 其中，会调用 CatalogCacheCreateEntry 函数将新读取的数据缓存到内存中。
        HeapTuple tuple = SearchSysCache1(SEALEXAMPLEOID, Int32GetDatum(number)); // 查找缓存中number列等于入参number的元组
        if(!HeapTupleIsValid(tuple)) {
            elog(WARNING, "[%s] cache lookup seal_example [%d] by number [%d] failed",
                 __func__, SealExampleRelationId, number);
        } else {
            if (form_ptr) {
                Form_seal_example form = (Form_seal_example) GETSTRUCT(tuple);
                *form_ptr = form;
            }
            if (tuple_ptr)
                *tuple_ptr = tuple;
            if (release_tuple)
                ReleaseSysCache(tuple);
            elog(INFO, "[%s] cache lookup seal_example [%d] by number [%d] successful",
                 __func__, SealExampleRelationId, number);
        }
    }
}

/*
 * 从seal_example系统表中查找number列等于入参number的一行记录；
 * 由于number列有唯一约束，因此，本函数仅返回一条记录。
 */
Form_seal_example SearchSealExampleTuple(int32 number) {
    HeapTuple tuple = NULL;
    Form_seal_example form = NULL;

    CacheLookupSealExampleTuple(number, &form, &tuple, true);
    if (form)
        return form;

    Relation rel;
    SysScanDesc scan;
    ScanKeyData key[1];

    rel = table_open(SealExampleRelationId, RowShareLock);

    // 扫描条件：number列等于入参number
    ScanKeyInit(&key[0], Anum_seal_example_number,
                BTEqualStrategyNumber /* Equality strategy number */,
                F_INT4EQ /* Comparison operator */, number);
    scan = systable_beginscan(rel, SealExampleNumberIndexId, true, NULL, 1, key);
    // 若表中存在多行满足查找条件的记录，可使用 while 循环持续查找；本示例中 number 列有唯一约束，只需查找一次。
    if (HeapTupleIsValid((tuple = systable_getnext(scan)))) {
        form = (Form_seal_example) GETSTRUCT(tuple);
        elog(INFO, "[%s] tablescan lookup seal_example [%d] by number [%d] successful",
             __func__, SealExampleRelationId, number);
    }

    systable_endscan(scan);
    table_close(rel, RowShareLock);
    return form;
}

/*
pg_proc.dat 文件中需要添加如下内容：
# seal_example test
{ oid => '8130', descr => 'he3 example test',
  proname => 'seal_example_test', prorows => '1', proretset => 't',
  provolatile => 'v', prorettype => 'record', proargtypes => '',
  proallargtypes => '{int8,int8}', proargmodes => '{o,o}',
  proargnames => '{case_num_current,case_num_next}',
  prosrc => 'seal_example_test' },

当宏 ENABLE_SEALDB_V1 未启用时，seal_example.c不会被编译，那么，为了避免导致 pg_proc.dat
中的 he3_example_test 函数无效的问题，可将本函数定义放置在其他文件中。
 */
Datum
seal_example_test(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	Datum values[2];
	bool nulls[2];

	InitMaterializedSRF(fcinfo, 0);

    static int case_num = 0, case_max = 5;

    values[0] = Int16GetDatum(case_num);
	values[1] = Int64GetDatum((case_num == case_max) ? 0 : case_num + 1);

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
// #ifdef ENABLE_SEALDB_V1
    switch (case_num) {
        case 0:
            InsertSealExampleTuple(100, "Wang");
            InsertSealExampleTuple(500, "Li");
            InsertSealExampleTuple(300, "Zhao");
            InsertSealExampleTuple(200, "Sun");
            InsertSealExampleTuple(900, "Li");
            InsertSealExampleTuple(600, "Wang");
            InsertSealExampleTuple(800, "Han");
            ++case_num;
            break;
        case 1:
            UpdateSealExampleTupleByTableScan(300, 301);
            UpdateSealExampleTupleByTableScan(500, 501);
            ++case_num;
            break;
        case 2:
            DeleteSealExampleTupleByTableScan(501);
            ++case_num;
            break;
        case 3:
            SearchSealExampleTuple(600);
            UpdateSealExampleTupleByCacheLookup(600, 601);
            UpdateSealExampleTupleByCacheLookup(900, 901);
            ++case_num;
            break;
        case 4:
            DeleteSealExampleTupleByCacheLookup(601);
            ++case_num;
            break;
        case 5:
            DeleteSealExampleTupleByCacheLookup(100);
            DeleteSealExampleTupleByCacheLookup(200);
            DeleteSealExampleTupleByCacheLookup(301);
            DeleteSealExampleTupleByCacheLookup(800);
            DeleteSealExampleTupleByCacheLookup(901);
            case_num = 0;
            break;
        default:
            break;
    }
// #endif

	return (Datum)0;
}