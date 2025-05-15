#ifndef SANDBOX_GUC_H
#define SANDBOX_GUC_H

#include "postgres.h"

/* 枚举类型定义 */
typedef enum {
    MODE_SAFE,        /* 安全模式 */
    MODE_PERFORMANCE, /* 性能模式 */
    MODE_DEBUG        /* 调试模式 */
} SandboxMode;

/* GUC 参数变量声明（外部链接） */
extern int      sandbox_max_workers;
extern bool     sandbox_enable_logging;
extern float8   sandbox_timeout;
extern char    *sandbox_data_dir;
extern int      sandbox_mode;

/* 函数声明 */
extern void     sandbox_guc_init(void);

#endif