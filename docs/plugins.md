# PostgreSQL 扩展（插件）总览

本仓库基于 PostgreSQL 15.15（分支 `dev_15.15`）。除 PostgreSQL 自带、随源码发布的
原生 `contrib` 模块外，本仓库还在 `contrib/` 目录下额外集成了若干第三方扩展与自研
扩展。

本文档为插件总览与导航，具体的能力、版本与开源协议请参见各分册文档。

## 文档导航

| 文档 | 内容 |
| --- | --- |
| [原生扩展（contrib）](plugins-native.md) | PostgreSQL 自带、随源码发布的原生 `contrib` 模块 |
| [第三方扩展](plugins-third-party.md) | 本仓库额外集成的第三方扩展及自研扩展 |

## 原生扩展

PostgreSQL 原生 `contrib` 模块位于 `contrib/`，涵盖索引与数据类型、管理与监控、
外部数据访问、备份与归档、安全与加密、过程语言桥接、逻辑解码与复制、采样方法等
多个方向。完整列表见 [plugins-native.md](plugins-native.md)。

## 第三方扩展（本仓库新增集成）

这些扩展大多以 git submodule 形式集成，少部分为本仓库自研、直接纳入源码树。
完整列表见 [plugins-third-party.md](plugins-third-party.md)：

| 插件 | 版本 | 开源协议 | 简介 |
| --- | --- | --- | --- |
| timescaledb | 2.28.3 | Apache-2.0 / Timescale License | 时序与事件数据的高性能实时分析 |
| timescaledb-toolkit | 1.22.0 | Timescale License | TimescaleDB 高级分析函数 |
| age | 1.6.0 | Apache-2.0 | 图数据库扩展（openCypher） |
| orioledb | beta15 | PostgreSQL / Apache-2.0 | 云原生事务存储引擎 |
| pgaudit | 1.7.1 | PostgreSQL License | 会话级/对象级审计日志 |
| ptrack | 2.4.3 | PostgreSQL License | 块级增量备份引擎 |
| pg_probackup | 2.5.16 | PostgreSQL License | 备份与恢复管理工具 |
| pg_dirtyread | 2.7 | PostgreSQL License | 读取已死但未 vacuum 的行 |
| temporal_tables | 1.2.2 | BSD 2-Clause | 系统周期时序表 |
| ora2pg | 25.0 | GPL-3.0 | Oracle → PostgreSQL 迁移工具 |
| walminer | 3.0 | MIT | WAL 日志解析/挖掘工具 |
| omnitype | 1.0 | 内部自研（无 OSS 许可） | 自定义数据类型与操作符类 |
| sandbox | 1.0 | 内部自研（无 OSS 许可） | C 扩展示例（UDF/SPI/后台进程/GUC） |

## 通用使用方式

- 初始化第三方子模块：`git submodule update --init --recursive`
- 编译并安装：`make -C contrib && make -C contrib install`（也可进入单个扩展目录单独编译）
- 在数据库中注册扩展：`CREATE EXTENSION <name>;`

> 各扩展的版本、开源协议与能力详情，请以对应文档及扩展自带的 `.control` 文件为准。
