# 第三方扩展（插件）

## 概述

本文档列出本仓库（基于 PostgreSQL 15.15，分支 `dev_15.15`）在 `contrib/` 目录下额外
集成的第三方扩展，以及仓库自研的扩展。

- 大部分第三方扩展以 **git submodule** 的形式集成，声明在仓库根目录的 `.gitmodules`
  中，克隆后需要使用 `git submodule update --init --recursive` 初始化。
- `omnitype` 与 `sandbox` 为**本仓库自研**，源代码直接纳入源码树（非 submodule），
  没有开源许可证文件。
- 版本号一般以扩展的 `.control` / `version.config` 文件以及集成时的提交说明为准。

## 汇总表

| 插件 | 版本 | 开源协议 | 上游仓库 | 主要能力 |
| --- | --- | --- | --- | --- |
| timescaledb | 2.28.3 | Apache-2.0 / Apache 兼容许可 / Timescale License | https://github.com/timescale/timescaledb | 时序与事件数据的高性能实时分析 |
| timescaledb-toolkit | 1.22.0 | Timescale License (TSL) | https://github.com/timescale/timescaledb-toolkit | TimescaleDB 高级分析函数 |
| age | 1.6.0 | Apache-2.0 | https://github.com/apache/age | 图数据库（openCypher） |
| orioledb | beta15 | PostgreSQL License + Apache-2.0 | https://github.com/orioledb/orioledb | 云原生事务存储引擎 |
| pgaudit | 1.7.1 | PostgreSQL License | https://github.com/pgaudit/pgaudit | 会话级/对象级审计日志 |
| ptrack | 2.4.3 | PostgreSQL License | https://github.com/postgrespro/ptrack | 块级增量备份引擎 |
| pg_probackup | 2.5.16 | PostgreSQL License | https://github.com/postgrespro/pg_probackup | 备份与恢复管理工具 |
| pg_dirtyread | 2.7 | PostgreSQL License | https://github.com/df7cb/pg_dirtyread | 读取已死但未 vacuum 的行 |
| temporal_tables | 1.2.2 | BSD 2-Clause | https://github.com/arkhipov/temporal_tables | 系统周期时序表 |
| ora2pg | 25.0 | GPL-3.0 | https://github.com/darold/ora2pg | Oracle → PostgreSQL 迁移工具 |
| walminer | 3.0 | MIT | https://gitee.com/movead/XLogMiner | WAL 日志解析/挖掘工具 |
| omnitype | 1.0 | 内部自研（无 OSS 许可） | 无 | 自定义数据类型与操作符类 |
| sandbox | 1.0 | 内部自研（无 OSS 许可） | 无 | C 扩展示例（UDF/SPI/后台进程/GUC） |

---

## timescaledb

**简介**：面向时序与事件数据的高性能实时分析扩展。

**主要能力**：

- hypertable（自动按时间分区的超表）
- 列式压缩，显著降低时序数据存储成本
- continuous aggregates（连续聚合/物化视图自动刷新）
- 面向实时分析的高效查询

**开源协议**：源码分别按 Apache License 2.0、Apache 兼容许可，以及 Timescale License
(TSL) 授权，每个源文件头部标注其适用的许可。

**源码/上游**：https://github.com/timescale/timescaledb ；git submodule，位于
`contrib/timescaledb`。

---

## timescaledb-toolkit

**简介**：为 TimescaleDB 提供高级分析函数的扩展。

**主要能力**：

- 近似百分位（approximate percentiles）
- 计数器汇总（counter aggregation）
- 时间加权统计（time-weighted averages 等）
- 心跳/状态聚合等分析函数
- 实验特性位于 `toolkit_experimental` schema

**开源协议**：Timescale License (TSL)。

**源码/上游**：https://github.com/timescale/timescaledb-toolkit ；git submodule，位于
`contrib/timescaledb-toolkit`。

---

## age

**简介**：Apache AGE（A Graph Extension），在 PostgreSQL 之上提供图数据库能力。

**主要能力**：

- 支持 openCypher 图查询语言
- `agtype` 数据类型
- 图数据的存储、查询与分析

**开源协议**：Apache License 2.0。

**源码/上游**：https://github.com/apache/age ；git submodule，位于 `contrib/age`
（集成分支 `release/PG15/1.6.0`）。

---

## orioledb

**简介**：面向云的下一代事务存储引擎，作为表访问方法（table access method）实现。

**主要能力**：

- 基于 undo log 的 MVCC，降低写放大
- 索引组织表（index-organized tables）
- 写时复制（copy-on-write）检查点
- 支持 S3 等对象存储

**开源协议**：双许可 —— PostgreSQL License + Apache License 2.0。

**源码/上游**：https://github.com/orioledb/orioledb ；git submodule，位于
`contrib/orioledb`（集成版本 beta15，`orioledb.control` 默认版本为 1.7）。

---

## pgaudit

**简介**：提供详细会话级与对象级审计日志的扩展。

**主要能力**：

- 会话级审计（session audit logging）
- 对象级审计（object audit logging）
- 通过标准 PostgreSQL 日志设施输出审计日志
- 满足政府、金融、ISO 等合规审计需求

**开源协议**：PostgreSQL License。

**源码/上游**：https://github.com/pgaudit/pgaudit ；git submodule，位于
`contrib/pgaudit`。

---

## ptrack

**简介**：块级增量备份引擎。

**主要能力**：

- 跟踪被修改的数据块（block-level change tracking）
- 配合 `pg_probackup` 实现增量备份
- 设计上允许误报（块被标记但实际未变），但不允许漏报
- 依赖少量 PostgreSQL 内核补丁（位于扩展目录的 `patches/`）

**开源协议**：PostgreSQL License。

**源码/上游**：https://github.com/postgrespro/ptrack ；git submodule，位于
`contrib/ptrack`（取自 master 分支，版本约 2.4.3，`ptrack.control` 默认版本 2.4）。

---

## pg_probackup

**简介**：PostgreSQL 集群的备份与恢复管理工具。

**主要能力**：

- 增量备份（页级）与增量恢复
- merge（合并增量，实现 "incrementally updated backups"）
- 校验（validate）与验证（checkdb）
- 保留策略（retention，按时间或数量，支持 TTL）
- 并行备份/恢复/合并/删除/校验
- 压缩与去重
- 远程备份与恢复
- 从备库（standby）执行备份
- 备份 PGDATA 之外的外部目录
- 备份目录与归档目录（支持文本或 JSON 输出）

**开源协议**：PostgreSQL License（含 Postgres Professional、NTT、加州大学等版权）。

**源码/上游**：https://github.com/postgrespro/pg_probackup ；git submodule，位于
`contrib/pg_probackup`。

---

## pg_dirtyread

**简介**：读取关系表中已死但尚未被 vacuum 清理的行。

**主要能力**：

- 读取 dead but unvacuumed rows
- 用于数据恢复与取证（forensics）

**开源协议**：PostgreSQL License（含 OmniTI 与加州大学版权）。

**源码/上游**：https://github.com/df7cb/pg_dirtyread ；git submodule，位于
`contrib/pg_dirtyread`。

---

## temporal_tables

**简介**：实现 SQL:2011 系统周期时序表（system-period temporal tables）。

**主要能力**：

- 在 UPDATE/DELETE 时自动将旧版本行归档到历史表（history table）
- 提供 `versioning()` 等触发器等
- 支持系统周期（transaction-time）语义

**开源协议**：BSD 2-Clause License（Copyright Vladislav Arkhipov）。

**源码/上游**：https://github.com/arkhipov/temporal_tables ；git submodule，位于
`contrib/temporal_tables`。

---

## ora2pg

**简介**：Oracle → PostgreSQL 数据库模式与数据迁移工具（Perl）。

**主要能力**：

- 连接并扫描 Oracle 数据库
- 生成可直接导入 PostgreSQL 的 SQL 脚本
- 支持导出表及其约束、视图、物化视图、表空间、序列、索引、触发器、函数、过程、
  包、分区、类型、数据（INSERT/COPY）、FDW 等

**开源协议**：GNU General Public License v3 (GPL-3.0)。

**源码/上游**：https://github.com/darold/ora2pg ；git submodule，位于
`contrib/ora2pg`。

---

## walminer

**简介**：PostgreSQL WAL 日志解析/挖掘工具。

**主要能力**：

- 从 WAL 中解析出 SQL（DML 及少量 DDL）
- 可生成对应的 undo SQL
- 支持数据页挽回
- 不要求 logical 日志级别，解析方式较为灵活

**开源协议**：MIT License。

**源码/上游**：https://gitee.com/movead/XLogMiner ；git submodule，位于
`contrib/walminer`。

---

## omnitype

**简介**：本仓库自研的自定义数据类型扩展。

**主要能力**：

- 新增数据类型：`mytext`、`tinyint`、`tinyint_v2`、复合（composite）类型等
- 提供 btree/hash/BRIN/GiST/GIN 操作符类与操作符族
- 为上述类型提供索引支持

**开源协议**：内部自研扩展，无开源许可证文件。

**源码/上游**：直接纳入源码树（非 submodule），位于 `contrib/omnitype`。

---

## sandbox

**简介**：本仓库自研的示例/演练扩展。

**主要能力**：

- 演示常见 C 扩展开发能力
- C UDF（用户自定义函数）
- SPI（服务器编程接口）用法
- 后台工作进程（background worker）
- GUC 参数定义

**开源协议**：内部自研扩展，无开源许可证文件。

**源码/上游**：直接纳入源码树（非 submodule），位于 `contrib/sandbox`。

---

## 编译与使用

1. 初始化第三方子模块：

   ```sh
   git submodule update --init --recursive
   ```

2. 编译并安装（也可进入单个扩展目录单独执行）：

   ```sh
   make -C contrib
   make -C contrib install
   ```

3. 在目标数据库中注册扩展：

   ```sql
   CREATE EXTENSION <name>;
   ```

> 说明：各扩展的实际版本、许可与能力，请以扩展自带的 `.control` / `version.config`
> 文件及上游仓库为准。
