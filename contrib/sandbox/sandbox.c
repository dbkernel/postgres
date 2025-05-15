#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "sandbox_guc.h"

#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "miscadmin.h"
#include "utils/wait_event.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"

PG_MODULE_MAGIC;

/**********************
 * 启动新进程
 **********************/
void _PG_init(void);
void start_sandbox_worker_internal(bool is_dynamic);
void sandbox_worker_main(Datum main_arg);

void _PG_init(void) {
    sandbox_guc_init();
    start_sandbox_worker_internal(false);
}

void start_sandbox_worker_internal(bool is_dynamic)
{
    char suffix[BGW_MAXLEN] = "static";
    BackgroundWorker worker;

    memset(&worker, 0, sizeof(BackgroundWorker));
    if (is_dynamic) snprintf(suffix, BGW_MAXLEN, "%s", "dynamic");

    ereport(LOG, (errmsg("sandbox worker %s is starting......", suffix)));

    /* 关键字段配置 */
    snprintf(worker.bgw_name, BGW_MAXLEN, "sandbox worker in %s", suffix); // 进程名称
    snprintf(worker.bgw_type, BGW_MAXLEN, "sandbox worker in %s", suffix); // 进程类型描述
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION; // 共享内存访问权限，使用数据库连接权限
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished; // 启动时机
    worker.bgw_restart_time = BGW_NEVER_RESTART; // 不自动重启
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "sandbox"); // 动态库
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "sandbox_worker_main"); // 函数名配置
    worker.bgw_main_arg = CStringGetDatum(suffix); // 传递给主函数的参数
    worker.bgw_notify_pid = 0; // 无需通知PID

    /* 注册后台工作进程 */
    if (is_dynamic) {
        // 动态方式，运行时动态调用（如 UDF 执行期间）
        BackgroundWorkerHandle *handle;
        RegisterDynamicBackgroundWorker(&worker, &handle);
    } else {
        // shared_preload_libraries 的作用是在 PostgreSQL 启动时加载插件的动态库，
        // 但插件功能需要通过 CREATE EXTENSION 命令显式启用。
        // 静态方式，只有在启动数据库、加载插件的动态库时（如 _PG_init 函数），才会创建通过RegisterBackgroundWorker注册的进程。
        // 反之，若通过 CREATE EXTENSION 命令启用插件，之后即使重启，也不会创建通过RegisterBackgroundWorker注册的进程。
        RegisterBackgroundWorker(&worker);
    }
}

// ================================================
// 以动态进程方式启动 sandbox worker 进程
// ================================================
PG_FUNCTION_INFO_V1(start_sandbox_worker);
Datum
start_sandbox_worker(PG_FUNCTION_ARGS)
{
    start_sandbox_worker_internal(true);
    PG_RETURN_BOOL(true);
}

void sandbox_worker_main(Datum main_arg) {
    char *suffix = DatumGetCString(main_arg);
    Latch *MyLatch = &MyProc->procLatch; // 使用当前进程的 Latch，无需初始化
    // static Latch MyLatch; // 使用自定义的全局 Latch
    // InitLatch(&MyLatch);

    ereport(LOG, (errmsg("sandbox worker %s is running...", suffix)));

    /* 注册信号处理函数 */
    // pqsignal(SIGHUP, SignalHandlerForConfigReload);
    // pqsignal(SIGTERM, SignalHandlerForShutdownRequest);

    BackgroundWorkerUnblockSignals();
    InitializeLatchSupport(); // 初始化Latch支持

    /* 初始化数据库连接 */
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);

    /* 设置应用名称以便在 pg_stat_activity 中识别 */
    char appname[BGW_MAXLEN];
    sprintf(appname, "sandbox_worker_in_%s", suffix);
    set_config_option("application_name", appname, PGC_USERSET, PGC_S_SESSION,
                      GUC_ACTION_SAVE, false, 0, false);

    /* 主循环 */
    while (true) {
        int rc;

        /* 关键点：检查终止请求（官方推荐方式） */
        CHECK_FOR_INTERRUPTS(); // 处理挂起的中断

        /* 如果收到终止信号（如 SIGTERM），退出循环 */
        if (ProcDiePending || InterruptPending) { // InterruptPending 与 WL_EXIT_ON_PM_DEATH 二选一
            break;
        }

        /* 执行主任务 */

        /* 使用 WaitLatch 检测退出事件 */
        rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                       10 * 1000L /* 10秒 */, PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);

        /* 如果主进程退出，直接终止 */
        if (rc & WL_EXIT_ON_PM_DEATH) // 自动处理退出
            break;
    }

    proc_exit(0);
}

/**********************
 * UDF 函数
 **********************/

// 自定义字符串转义函数
static char *
escape_literal(const char *src)
{
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfoChar(&buf, '\'');
    for (const char *p = src; *p; p++) {
        if (*p == '\'')
            appendStringInfoString(&buf, "''");
        else
            appendStringInfoChar(&buf, *p);
    }
    appendStringInfoChar(&buf, '\'');
    return buf.data;
}

// ================================================
// 检查表是否存在，若存在则删除；创建新表
// ================================================
PG_FUNCTION_INFO_V1(validate_table);
Datum
validate_table(PG_FUNCTION_ARGS)
{
    text *tablename_arg = PG_GETARG_TEXT_PP(0);
    char *tablename = text_to_cstring(tablename_arg);
    int ret;

    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");

    StringInfoData query;
    initStringInfo(&query);

    // 检查表是否存在
    char *escaped_tablename = escape_literal(tablename);
    appendStringInfo(&query,
                     "SELECT 1 FROM pg_catalog.pg_class "
                     "WHERE relname = %s AND relkind = 'r'",
                     escaped_tablename);
    pfree(escaped_tablename);

    ret = SPI_execute(query.data, true, 0); // true: 只读
    if (ret != SPI_OK_SELECT)
        elog(ERROR, "SPI_execute failed: %s", SPI_result_code_string(ret));

    if (SPI_processed > 0) {
        // 删除表
        resetStringInfo(&query);
        appendStringInfo(&query, "DROP TABLE %s", quote_identifier(tablename));
        ret = SPI_execute(query.data, false, 0); // false: 可写
        if (ret != SPI_OK_UTILITY)
            elog(ERROR, "Failed to drop table: %s", SPI_result_code_string(ret));
    }

    // 创建表
    resetStringInfo(&query);
    appendStringInfo(&query,
                     "CREATE TABLE %s (id SERIAL PRIMARY KEY, data TEXT)",
                     quote_identifier(tablename));
    ret = SPI_execute(query.data, false, 0); // false: 可写
    if (ret != SPI_OK_UTILITY)
        elog(ERROR, "Failed to create table: %s", SPI_result_code_string(ret));

    SPI_finish();
    PG_RETURN_VOID();
}

// ================================================
// 插入多行数据并返回结果
// ================================================
PG_FUNCTION_INFO_V1(insert_records);
Datum
insert_records(PG_FUNCTION_ARGS)
{
    text *tablename_arg = PG_GETARG_TEXT_PP(0);
    ArrayType *data_array = PG_GETARG_ARRAYTYPE_P(1);
    char *tablename = text_to_cstring(tablename_arg);

    // 不再需要 SRF 上下文管理
    int ret;
    Datum *data_values;
    bool *data_nulls;
    int data_count;

    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");

    // 解析数组参数
    deconstruct_array(data_array, TEXTOID, -1, false, 'i',
                      &data_values, &data_nulls, &data_count);

    // 构建 INSERT 语句（去掉 RETURNING）
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query, "INSERT INTO %s (data) VALUES ", quote_identifier(tablename));
    for (int i = 0; i < data_count; i++) {
        appendStringInfoString(&query, "(");
        if (data_nulls[i]) {
            appendStringInfoString(&query, "NULL");
        } else {
            char *str = TextDatumGetCString(data_values[i]);
            char *escaped = escape_literal(str);
            appendStringInfoString(&query, escaped);
            pfree(escaped);
        }
        appendStringInfoString(&query, ")");
        if (i < data_count - 1) appendStringInfoString(&query, ", ");
    }

    // 执行插入（不再需要返回结果）
    ret = SPI_execute(query.data, false, 0);
    if (ret != SPI_OK_INSERT)
        elog(ERROR, "SPI_execute failed: code=%d", ret);

    SPI_finish();
    PG_RETURN_BOOL(true);
}

// ================================================
// 只返回一行demo数据
// ================================================
PG_FUNCTION_INFO_V1(find_record_demo);
Datum
find_record_demo(PG_FUNCTION_ARGS)
{
    #define COL_NUMS 4

	ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	Datum values[COL_NUMS];
	bool nulls[COL_NUMS];

	InitMaterializedSRF(fcinfo, 0);
#ifdef RETURN_NULL
	values[0] = Int64GetDatum(0);
	values[1] = CStringGetDatum("");
	values[2] = CStringGetTextDatum("");
	values[3] = TimestampTzGetDatum(GetCurrentTimestamp());
#else
	values[0] = Int64GetDatum(100);
	values[1] = CStringGetDatum("Li Lei");
	values[2] = CStringGetTextDatum("Jiang Su");
	values[3] = TimestampTzGetDatum(GetCurrentTimestamp());
#endif
	tuplestore_putvalues(rsinfo->setResult,rsinfo->setDesc,values,nulls);

    PG_RETURN_INT32(0);
}

// ================================================
// 只返回一行满足条件的数据
// ================================================
PG_FUNCTION_INFO_V1(find_record);
Datum
find_record(PG_FUNCTION_ARGS)
{
    text *tablename_arg = PG_GETARG_TEXT_PP(0);
    text *where_arg = PG_GETARG_TEXT_PP(1);
    char *tablename_str = text_to_cstring(tablename_arg);
    char *where_str = text_to_cstring(where_arg);

    int ret, validAttrCount = 0;
    int row = 0;
    Datum *values = NULL;
    bool *nulls = NULL;
    TupleDesc spiTupDesc = NULL;

    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

    InitMaterializedSRF(fcinfo, 0);

    /* 连接 SPI */
    if ((ret = SPI_connect()) != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed: %d", ret);

    /* 构建查询语句 */
    StringInfoData query = {0};
    initStringInfo(&query);
    appendStringInfo(&query, "SELECT * FROM %s", quote_identifier(tablename_str));
    if (strlen(where_str) > 0 && strcmp(where_str, "NULL") != 0)
        appendStringInfo(&query, " WHERE %s", where_str);

    /* 执行查询 */
    ret = SPI_execute(query.data, true, 1);
    pfree(query.data); // 释放StringInfo分配的内存
    if (ret != SPI_OK_SELECT)
        elog(ERROR, "SPI_execute failed: %s", SPI_result_code_string(ret));

    /* 检查行数 */
    if (SPI_processed == 0)
        goto end;

    /* 处理结果集 */
    spiTupDesc = SPI_tuptable->tupdesc;
    for (int i = 0; i < spiTupDesc->natts; i++) {
        if(spiTupDesc->attrs[i].attisdropped) // 跳过删除的字段
            continue;
        validAttrCount++;
    }

    values = palloc0(validAttrCount * sizeof(Datum));
    nulls = palloc0(validAttrCount * sizeof(bool));
    for (row = 0; row < SPI_processed; row++) {
        if (row == 1) break;
        HeapTuple spiTuple = SPI_tuptable->vals[row];
        int atts = 0;
        elog(INFO, "Row %lu", row + 1); // 行号从1开始
        for (int i = 0; i < spiTupDesc->natts; i++) {
            //跳过删除的字段
            if(spiTupDesc->attrs[i].attisdropped)
                continue;

            //获取字段值
        #if 0
            char *col = SPI_getvalue(spiTuple, spiTupDesc, i + 1);
            elog(INFO, "Column %lu: %s", i+1, col); // 列号从1开始
            values[atts] = CStringGetDatum(col); // crash
            pfree(col);
        #else
            bool isnull = false;
            Datum col = SPI_getbinval(spiTuple, spiTupDesc, i + 1, &isnull); // 列号从1开始
            if (isnull)
                nulls[atts++] = col;
            else
                values[atts++] = col;
        #endif
        }
	    tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
    }

end:
    SPI_finish(); /* 清理 SPI 资源 */
    PG_RETURN_INT32(0);
}

// ================================================
// 一次性读取全表数据（方式一）：Materialized SRF (Set Returning Function)
// 特点：
// 1. 一次性处理：在函数执行期间，一次性将所有结果行存入 tuplestore 中；
// 2. 内存敏感：所有结果需同时存于内存，受 work_mem 限制。当前实现人为限制1000行上限；
// 3. 简单直接：实现更简单，无需管理多轮调用状态。
// ================================================
PG_FUNCTION_INFO_V1(find_records);
Datum
find_records(PG_FUNCTION_ARGS)
{
    text *tablename_arg = PG_GETARG_TEXT_PP(0);
    text *where_arg = PG_GETARG_TEXT_PP(1);
    char *tablename_str = text_to_cstring(tablename_arg);
    char *where_str = text_to_cstring(where_arg);
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    const int MAX_ROWS = 1000; // 当前设置上限为 1000 行
    int ret = 0;

    if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use raw page functions")));

    InitMaterializedSRF(fcinfo, 0);

    /* 连接 SPI */
    if ((ret = SPI_connect()) != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed: %s", SPI_result_code_string(ret));

    /* 构建查询语句 */
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query, "SELECT * FROM %s", quote_identifier(tablename_str));
    if (where_str != NULL && strlen(where_str) > 0 && strcasecmp(where_str, "NULL") != 0)
        appendStringInfo(&query, " WHERE %s", where_str);

    /* 执行查询 */
    ret = SPI_execute(query.data, true, MAX_ROWS);
    pfree(query.data); // 释放StringInfo分配的内存
    if (ret != SPI_OK_SELECT)
        elog(ERROR, "SPI_execute failed: %s", SPI_result_code_string(ret));

    /* 检查结果集大小 */
    if (SPI_processed > MAX_ROWS)
        elog(ERROR, "Result exceeds %d rows (found %lld)", MAX_ROWS, (long long)SPI_processed);
    if (SPI_processed == 0) {
        SPI_finish();
        PG_RETURN_DATUM(0);
    }

    /* 创建过滤后的元组描述符（跳过已删除列） */
    TupleDesc spiTupDesc = SPI_tuptable->tupdesc;
    int validAttrCount = 0;
    for (int i = 0; i < spiTupDesc->natts; i++) {
        if (!spiTupDesc->attrs[i].attisdropped) validAttrCount++;
    }

    /* 将 SPI 结果转换并存入元组存储 */
    for (uint64 row = 0; row < SPI_processed; row++) {
        HeapTuple spiTuple = SPI_tuptable->vals[row];
        Datum *values;
        bool *nulls;
        int validCol = 0;

        values = palloc0(validAttrCount * sizeof(Datum));
        nulls = palloc0(validAttrCount * sizeof(bool));

        for (int i = 0; i < spiTupDesc->natts; i++) {
            if (spiTupDesc->attrs[i].attisdropped) continue;

            bool isnull;
            Datum value = SPI_getbinval(spiTuple, spiTupDesc, i + 1, &isnull);
            values[validCol] = value;
            nulls[validCol] = isnull;
            validCol++;
        }

        tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
    }

    SPI_finish(); /* 清理 SPI 资源 */
    return (Datum) 0;
}

// ================================================
// 一次性读取全表数据（方式二）：Multi-call SRF (Set Returning Function)
// 特点：
// 1. 流式处理：函数会被多次调用，每次返回一行结果；
// 2. 内存高效：无需一次性存储所有结果，适合处理大量数据；
// 3. 复杂度高：需要管理函数上下文状态，实现更复杂。
// ================================================
PG_FUNCTION_INFO_V1(find_records_multi_call);
Datum
find_records_multi_call(PG_FUNCTION_ARGS)
{
    text *tablename_arg = PG_GETARG_TEXT_PP(0);
    text *where_arg = PG_GETARG_TEXT_PP(1);
    char *tablename_str = text_to_cstring(tablename_arg);
    char *where_str = text_to_cstring(where_arg);

    FuncCallContext *funcctx;
    MemoryContext oldcontext;
    const int MAX_ROWS = 1000;

    /* 首次调用初始化 */
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc spiTupDesc = NULL, retTupDesc = NULL;
        SPITupleTable *spi_tuptable = NULL;
        Tuplestorestate *tupstore = NULL;
        MemoryContext oldctx;
        int ret;

        /* 初始化 SRF 上下文 */
        funcctx = SRF_FIRSTCALL_INIT();

        /* 连接 SPI */
        if ((ret = SPI_connect()) != SPI_OK_CONNECT)
            elog(ERROR, "SPI_connect failed: %s", SPI_result_code_string(ret));

        /* 构建查询语句 */
        StringInfoData query;
        initStringInfo(&query);
        appendStringInfo(&query, "SELECT * FROM %s", quote_identifier(tablename_str));
        if (where_str != NULL && strlen(where_str) > 0 && strcasecmp(where_str, "NULL") != 0)
            appendStringInfo(&query, " WHERE %s", where_str);

        /* 执行查询 */
        ret = SPI_execute(query.data, true, MAX_ROWS);
        pfree(query.data); // 释放StringInfo分配的内存
        if (ret != SPI_OK_SELECT)
            elog(ERROR, "SPI_execute failed: %s", SPI_result_code_string(ret));

        /* 检查结果集大小 */
        if (SPI_processed > MAX_ROWS)
            elog(ERROR, "Result exceeds %d rows (found %lld)", MAX_ROWS, (long long)SPI_processed);

        if (SPI_processed == 0) {
            SPI_finish();
            SRF_RETURN_DONE(funcctx); // 直接返回空结果
        }

        /* 创建过滤后的元组描述符（跳过已删除列） */
        int validAttrCount = 0;
        retTupDesc = SPI_tuptable->tupdesc;
        for (int i = 0; i < retTupDesc->natts; i++) {
            if (!retTupDesc->attrs[i].attisdropped) validAttrCount++;
        }

        // 关键：后续需要在 SRF 多次调用中被访问的结构必须在 multi_call_memory_ctx 上下文中分配
        oldctx = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        TupleDesc finalTupDesc = CreateTemplateTupleDesc(validAttrCount);
        validAttrCount = 0;
        for (int i = 0; i < retTupDesc->natts; i++) {
            if (retTupDesc->attrs[i].attisdropped)
                continue;

            Form_pg_attribute attr = &retTupDesc->attrs[i];
            elog(INFO, "Attribute %d: name=%s, type=%u, len=%d",
                 i, NameStr(attr->attname), attr->atttypid, attr->attlen);
            char *attrName = pstrdup(NameStr(attr->attname)); // 关键：必须在multi_call_memory_ctx上下文中复制属性名
            TupleDescInitEntry(finalTupDesc, (AttrNumber)(validAttrCount + 1),
                               attrName, attr->atttypid, attr->atttypmod,
                               attr->attnotnull);
            validAttrCount++;
        }

        finalTupDesc = BlessTupleDesc(finalTupDesc); // 关键：必须在 multi_call_memory_ctx 上下文中祝福元组描述符
        PinTupleDesc(finalTupDesc);  // 关键：防止被缓存回收
        elog(INFO, "Created TupleDesc pinned at %p, with %d attributes", finalTupDesc, finalTupDesc->natts);
        funcctx->tuple_desc = finalTupDesc;

        /* 初始化元组存储 */
        tupstore = tuplestore_begin_heap(true, false, work_mem); // 关键：必须在 multi_call_memory_ctx 上下文中构建 tupstore
        funcctx->user_fctx = tupstore;
        elog(INFO, "Created tuplestore at %p", tupstore);

        MemoryContextSwitchTo(oldctx);

        /* 将 SPI 结果转换并存入元组存储 */
        for (uint64 row = 0; row < SPI_processed; row++) {
            HeapTuple spiTuple = SPI_tuptable->vals[row];
            Datum *values;
            bool *nulls;
            int validCol = 0;

            values = palloc0(finalTupDesc->natts * sizeof(Datum));
            nulls = palloc0(finalTupDesc->natts * sizeof(bool));

            for (int i = 0; i < retTupDesc->natts; i++) {
                if (retTupDesc->attrs[i].attisdropped) continue;

                bool isnull;
                Datum value = SPI_getbinval(spiTuple, retTupDesc, i + 1, &isnull);
                values[validCol] = value;
                nulls[validCol] = isnull;
                validCol++;
            }

            HeapTuple tuple = heap_form_tuple(finalTupDesc, values, nulls);
            tuplestore_puttuple(tupstore, tuple);

            pfree(values);
            pfree(nulls);
            heap_freetuple(tuple);
        }

        SPI_finish(); // 清理 SPI 资源
    }

    /* 逐行返回结果 */
    funcctx = SRF_PERCALL_SETUP();
    Tuplestorestate *tupstore = funcctx->user_fctx;
    elog(INFO, "Returned TupleDesc at %p, with %d attributes", funcctx->tuple_desc, funcctx->tuple_desc->natts);
    elog(INFO, "Returned tuplestore at %p", tupstore);

    /* 使用 Minimal Tuple 槽或 Virtual Tuple 槽 */
    TupleTableSlot *slot = MakeSingleTupleTableSlot(funcctx->tuple_desc, &TTSOpsMinimalTuple);

    if (tuplestore_gettupleslot(tupstore, true, false, slot)) {
        Datum result = ExecFetchSlotHeapTupleDatum(slot); // 自动处理类型转换
        ExecDropSingleTupleTableSlot(slot); // 释放Slot
        SRF_RETURN_NEXT(funcctx, result);
    } else {
        elog(INFO, "tuplestore at %p which coming to an end", tupstore); // 后续调用日志
        tuplestore_end(tupstore); // 释放元组存储
        ExecDropSingleTupleTableSlot(slot); // 释放Slot
        SRF_RETURN_DONE(funcctx);
    }
}

// ================================================
// 删除表
// ================================================
PG_FUNCTION_INFO_V1(drop_table);
Datum
drop_table(PG_FUNCTION_ARGS)
{
    text *tablename_arg = PG_GETARG_TEXT_PP(0);
    char *tablename = text_to_cstring(tablename_arg);
    int ret;

    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");

    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query, "DROP TABLE IF EXISTS %s", quote_identifier(tablename));

    ret = SPI_execute(query.data, false, 0);
    if (ret != SPI_OK_UTILITY)
        elog(ERROR, "Failed to drop table: %s", SPI_result_code_string(ret));

    SPI_finish();
    PG_RETURN_VOID();
}

/**********************
 * 自定义聚合函数
 **********************/

typedef struct {
    Datum* values;
    int count;
    bool sorted;
} MedianState;

PG_FUNCTION_INFO_V1(median_agg_transfn);
Datum median_agg_transfn(PG_FUNCTION_ARGS) {
    MemoryContext aggctx;
    MedianState* state;
    Datum value = PG_GETARG_DATUM(1);

    if (!AggCheckCallContext(fcinfo, &aggctx))
        elog(ERROR, "median_agg_transfn called in non-aggregate context");

    if (PG_ARGISNULL(0)) {
        state = (MedianState*)MemoryContextAllocZero(aggctx, sizeof(MedianState));
        state->values = (Datum*)MemoryContextAlloc(aggctx, sizeof(Datum));
        state->values[0] = value;
        state->count = 1;
        state->sorted = false;
    } else {
        state = (MedianState*)PG_GETARG_POINTER(0);
        state->values = (Datum*)repalloc(state->values, (state->count + 1) * sizeof(Datum));
        state->values[state->count] = value;
        state->count++;
        state->sorted = false;
    }

    PG_RETURN_POINTER(state);
}

static int compare_datum(const void* a, const void* b) {
    Datum da = *((const Datum*)a);
    Datum db = *((const Datum*)b);
    return DatumGetInt32(DirectFunctionCall2(numeric_cmp, da, db));
}

PG_FUNCTION_INFO_V1(median_agg_finalfn);
Datum median_agg_finalfn(PG_FUNCTION_ARGS) {
    MedianState* state = (MedianState*)PG_GETARG_POINTER(0);
    Datum result;

    if (state == NULL || state->count == 0)
        PG_RETURN_NULL();

    if (!state->sorted) {
        qsort(state->values, state->count, sizeof(Datum), compare_datum);
        state->sorted = true;
    }

    if (state->count % 2 == 0) {
        // 偶数个元素：手动计算中间两个值的平均值
        Datum val1 = state->values[state->count/2 - 1];
        Datum val2 = state->values[state->count/2];

        // 相加
        Datum sum = DirectFunctionCall2(numeric_add, val1, val2);
        // 除以2
        Datum divisor = Float8GetDatum(2.0);
        result = DirectFunctionCall2(numeric_div, sum,
            DirectFunctionCall3(numeric_in, CStringGetDatum("2"),
                                ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1)));
    } else {
        // 奇数个元素：取中间值
        result = state->values[state->count/2];
    }

    PG_RETURN_DATUM(result);
}