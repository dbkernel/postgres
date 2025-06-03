#include "postgres.h"
#include "fmgr.h"
#include "utils/guc.h"
#include "sandbox_guc.h"

/* GUC 参数变量定义 */
int      sandbox_max_workers = 5;
bool     sandbox_enable_logging = true;
float8   sandbox_timeout = 300.0;
char    *sandbox_data_dir = NULL;
int      sandbox_mode = MODE_SAFE;

/* 枚举类型映射表 */
static const struct config_enum_entry mode_options[] = {
    {"safe", MODE_SAFE, NULL},
    {"performance", MODE_PERFORMANCE, NULL},
    {"debug", MODE_DEBUG, NULL},
    {NULL, 0, NULL}
};

/* 检查 int 类型参数合法性，参数赋值之前的动作 */
static bool
check_sandbox_int_params(int *newval, void **extra, GucSource source)
{
    return true;
}

/* 参数赋值后的动作 */
static void
assign_sandbox_int_params(int newval, void *extra)
{
    return;
}

/* 参数初始化函数 */
void
sandbox_guc_init(void)
{

    /* 定义整数参数 */
    DefineCustomIntVariable(
        "sandbox.max_workers",
        "Maximum number of sandbox worker processes.",
        NULL,
        &sandbox_max_workers,
        5,                  /* 默认值 */
        1,                  /* 最小值 */
        100,                /* 最大值 */
        PGC_SIGHUP,         /* 动态重载（需重启进程） */
        0,                  /* 无额外标志 */
        check_sandbox_int_params, /* 参数检查钩子 */
        assign_sandbox_int_params, /* 参数赋值后回调 */
        NULL                /* 无额外数据 */
    );

    /* 定义布尔参数 */
    DefineCustomBoolVariable(
        "sandbox.enable_logging",
        "Enable detailed logging for sandbox operations.",
        NULL,
        &sandbox_enable_logging,
        true,               /* 默认值 */
        PGC_SUSET,          /* 超级用户可修改 */
        0,                  /* 无额外标志 */
        NULL,               /* 无检查钩子 */
        NULL,               /* 无回调 */
        NULL                /* 无额外数据 */
    );

    /* 定义浮点参数 */
    DefineCustomRealVariable(
        "sandbox.timeout",
        "Timeout for sandbox operations (in seconds).",
        NULL,
        &sandbox_timeout,
        300.0,              /* 默认值 */
        0.1,                /* 最小值 */
        3600.0,             /* 最大值 */
        PGC_POSTMASTER,     /* 需重启Postmaster */
        0,                  /* 无额外标志 */
        NULL,               /* 无检查钩子 */
        NULL,               /* 无回调 */
        NULL                /* 无额外数据 */
    );

    /* 定义字符串参数 */
    DefineCustomStringVariable(
        "sandbox.data_dir",
        "Directory for sandbox plugin data files.",
        NULL,
        &sandbox_data_dir,
        "/var/lib/postgresql/sandbox", /* 默认值 */
        PGC_SIGHUP,         /* 动态重载 */
        0,                  /* 无额外标志 */
        NULL,               /* 无检查钩子 */
        NULL,               /* 无回调 */
        NULL                /* 无额外数据 */
    );

    /* 定义枚举参数 */
    DefineCustomEnumVariable(
        "sandbox.mode",
        "Operating mode for the sandbox plugin.",
        NULL,
        &sandbox_mode,
        MODE_SAFE,          /* 默认值 */
        mode_options,       /* 枚举选项表 */
        PGC_SUSET,          /* 超级用户可修改 */
        0,                  /* 无额外标志 */
        NULL,               /* 无检查钩子 */
        NULL,               /* 无回调 */
        NULL                /* 无额外数据 */
    );

}