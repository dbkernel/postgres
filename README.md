# SealDB on PostgreSQL 15.15

> 基于 PostgreSQL 15.15 的数据库内核开发分支：内置 SealDB V1 特性、自研扩展、常用第三方
> 扩展集成，以及内核源码分析文档。

本仓库是 [`dbkernel/postgres`](https://github.com/dbkernel/postgres) 的 PostgreSQL 内核
开发分支，当前主干为 **`dev_15.15`**，基线版本为 PostgreSQL **15.15**（对应上游 `15.15`
分支，commit `32f38816779`）。本分支在保持与上游兼容的前提下，围绕「内核特性验证 +
扩展生态集成 + 源码分析」进行建设。

## 特性概览

| 方向 | 内容 |
| --- | --- |
| 内核特性 | SealDB V1 编译开关 `--enable-sealdb-v1`；系统表（catalog）示例 `seal_example` |
| 自研扩展 | `omnitype`（自定义数据类型）、`sandbox`（C 扩展示例） |
| 第三方扩展 | 以 git submodule 集成 11 个扩展（时序/图/存储引擎/审计/备份/迁移/WAL 解析等） |
| 原生扩展 | PostgreSQL 自带 59 个 contrib 模块 |
| 学习资料 | `learn-15.15/`、`articles/` 内核源码分析文章与系列目录 |
| 变更提案 | `openspec/`（如 `pg-hash-index`） |

## SealDB V1 内核特性

编译开关：

```sh
./configure --enable-sealdb-v1
```

启用后定义 `SEALDB_V1` 宏（见 `configure.ac`，默认关闭，需显式传入 `--enable-sealdb-v1`）。该特性以「系统表示例」的形式，
完整演示了向 PostgreSQL 添加一张系统表并接入 SysCache 的全流程，可作为内核开发的参考实现：

- **系统表定义**：`src/include/catalog/seal_example.h` / `seal_example.dat`
  （relation OID `8200`，字段 `oid` / `number` / `tname`）。
- **唯一索引与 SysCache**：`seal_example_oid_index`（OID `8201`，主键）、
  `seal_example_number_index`（OID `8202`，唯一），并在 `syscache` 中注册。
- **增删改查接口**（`src/backend/catalog/seal_example.c`）：
  - `InsertSealExampleTuple()`：通过 `CatalogTupleInsert()` 插入元组；
  - `UpdateSealExampleTupleByTableScan()` / `DeleteSealExampleTupleByTableScan()`：
    基于表扫描的更新/删除；
  - `UpdateSealExampleTupleByCacheLookup()` / `DeleteSealExampleTupleByCacheLookup()`：
    基于 SysCache 的更新/删除；
  - `SearchSealExampleTuple()`：SysCache 查找。
- **测试函数**：`seal_example_test()`（`pg_proc` OID `8130`），以集合返回函数（SRF）
  的形式依次触发上述增删改查用例。

> 说明：该示例用于教学/验证目的。`SEALDB_V1` 宏控制系统表 `seal_example`、其 SysCache 与相关增删改查代码是否编译与注册；测试函数 `seal_example_test()` 始终编译（`pg_proc.dat`/`fmgrtab.c` 需要该符号）。

## 自研扩展（contrib/）

| 扩展 | 版本 | 说明 |
| --- | --- | --- |
| [omnitype](contrib/omnitype) | 1.0 | 自定义数据类型扩展：新增 `mytext`、`tinyint`、`tinyint_v2`、复合类型等，并提供 btree/hash/BRIN/GiST/GIN 操作符类、操作符族与索引支持 |
| [sandbox](contrib/sandbox) | 1.0 | C 扩展示例：演示 C UDF、SPI、后台工作进程（background worker）、GUC 等能力 |

## 集成的第三方扩展（contrib/）

以 git submodule 形式集成，克隆后需先初始化子模块：

```sh
git submodule update --init --recursive
```

| 扩展 | 版本 | 开源协议 | 主要能力 |
| --- | --- | --- | --- |
| timescaledb | 2.28.3 | Apache-2.0 / Timescale License | 时序与事件数据的高性能实时分析 |
| timescaledb-toolkit | 1.22.0 | Timescale License | TimescaleDB 高级分析函数 |
| age | 1.6.0 | Apache-2.0 | 图数据库（openCypher） |
| orioledb | beta15 | PostgreSQL + Apache-2.0 | 云原生事务存储引擎 |
| pgaudit | 1.7.1 | PostgreSQL License | 会话级/对象级审计日志 |
| ptrack | 2.4.3 | PostgreSQL License | 块级增量备份引擎 |
| pg_probackup | 2.5.16 | PostgreSQL License | 备份与恢复管理工具 |
| pg_dirtyread | 2.7 | PostgreSQL License | 读取已死但未 vacuum 的行 |
| temporal_tables | 1.2.2 | BSD 2-Clause | 系统周期时序表 |
| ora2pg | 25.0 | GPL-3.0 | Oracle → PostgreSQL 迁移工具 |
| walminer | 3.0 | MIT | WAL 日志解析/挖掘工具 |

完整的插件说明（版本、能力、开源协议、上游仓库）见
[`docs/plugins-third-party.md`](docs/plugins-third-party.md)。

## 原生扩展

PostgreSQL 自带的 59 个 contrib 模块（索引与数据类型、管理与监控、外部数据访问、
备份与归档、安全与加密、过程语言桥接、逻辑解码与复制、采样方法等）说明见
[`docs/plugins-native.md`](docs/plugins-native.md)；插件总览与导航见
[`docs/plugins.md`](docs/plugins.md)。

## 内核源码分析资料

- **`learn-15.15/`**：PostgreSQL 内核源码深度解析文章，例如
  - `PG系统表与系统视图源码深度解析.md`
  - `PG解析器源码深度解析.md`
  - `PostgreSQL_Hash索引架构源码解析.md` / `...核心算法源码解析.md` /
    `...WAL与崩溃恢复解析.md`
  - `ptrack-pg_probackup-architecture.md`
  - `timescaledb-compression.md`
  - `walminer源码深度解析.md` / `wal2json源码深度解析.md`
  - `云原生存算分离数据库对比分析报告.md`
- **`articles/`**：数据库内核源码分析系列目录（PostgreSQL / MySQL / 对照），
  见 [`articles/README.md`](articles/README.md)。
- **`openspec/`**：内核变更提案与设计规格（如 `pg-hash-index`）。

## 目录结构

```
contrib/       扩展模块（原生 + 自研 + 第三方 submodule）
src/           数据库内核源码
docs/          插件与扩展文档
learn-15.15/   内核源码分析文章
articles/      内核分析系列目录
openspec/      变更提案与规格
build.sh       参考构建脚本
```

## 快速开始

### 1. 环境准备

标准 PostgreSQL 构建依赖（GCC/Clang、make、flex、bison、readline、zlib、OpenSSL、
libxml2 等），以及 Git（用于初始化 submodule）。

### 2. 初始化子模块

```sh
git submodule update --init --recursive
```

### 3. 编译与安装

参考 `build.sh`：

```sh
CFLAGS=-O0 ./configure \
    --prefix=$HOME/pgsql \
    --enable-debug --enable-cassert \
    --with-libxml --with-openssl \
    --enable-sealdb-v1 --enable-depend

make -j4
make install
```

也可直接执行：

```sh
./build.sh
```

### 4. 初始化与启动

```sh
export PATH=$HOME/pgsql/bin:$PATH
initdb -D $HOME/pgdata
pg_ctl -D $HOME/pgdata -l $HOME/pgdata/logfile start
psql
```

### 5. 使用扩展

```sql
CREATE EXTENSION omnitype;   -- 自研扩展
CREATE EXTENSION sandbox;    -- 自研扩展
-- 第三方扩展同理，例如：
-- CREATE EXTENSION pgaudit;
-- CREATE EXTENSION timescaledb;
```

## 分支说明

| 分支 | 说明 |
| --- | --- |
| `dev_15.15` | 当前开发主干，基于 PostgreSQL 15.15 |
| `15.15` / `REL_15_5` / `REL_15_7` | PostgreSQL 15.x 相关分支 |
| `18.4` | 基于 PostgreSQL 18.4 |
| `dev_15.5` | 历史开发分支（15.5 基线） |
| `dev_tsdb_2.16.1` / `dev_tsdb_refactor` | TimescaleDB 集成相关分支 |
| `master` | 上游主线同步 |

## 许可协议

- 数据库内核（`src/`、原生 `contrib/`）遵循 **PostgreSQL License**，详见 `COPYRIGHT`。
- 各第三方扩展遵循其各自许可证（如 Apache-2.0、Timescale License、GPL-3.0、MIT、
  BSD 等），详见 [`docs/plugins-third-party.md`](docs/plugins-third-party.md) 及扩展目录下的
  `LICENSE` 文件。

## 上游与致谢

- 上游：PostgreSQL（<https://github.com/postgres/postgres>）
- 第三方扩展的作者及其上游仓库，见 [`docs/plugins-third-party.md`](docs/plugins-third-party.md)。
