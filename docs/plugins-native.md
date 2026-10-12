# 原生扩展（PostgreSQL contrib）

## 概述

本文档列出随 PostgreSQL 15.15 源码一同发布的**原生 contrib 模块**，位于 `contrib/`
目录。这些模块不属于核心服务器，但由 PostgreSQL 官方维护。

- 从源码构建时，这些模块**默认不会**被自动编译，除非构建 `world` 目标。
- 也可以在 `contrib/` 目录下执行 `make all` 与 `make install` 一次性构建安装全部模块，
  或进入某个模块的子目录单独编译安装。
- 部分模块依赖编译选项（SSL、libxml、SELinux、Perl、Python、uuid 等），只有在
  `configure` 时启用了对应选项，该模块才会被构建。
- 使用某个模块前，需要先在数据库中执行 `CREATE EXTENSION <module>;` 注册其 SQL 对象。

> 本仓库额外集成的第三方扩展（timescaledb、age、orioledb、pgaudit、ptrack、
> pg_probackup、walminer、ora2pg、pg_dirtyread、temporal_tables、
> timescaledb-toolkit、omnitype、sandbox）请参见
> [第三方扩展](plugins-third-party.md)。

## 管理与监控（Administration & Monitoring）

| 模块 | 类型 | 主要能力 |
| --- | --- | --- |
| adminpack | 扩展 | 提供 pgAdmin 等工具使用的维护函数（如日志文件读写） |
| auto_explain | 扩展 | 自动记录超过阈值的慢查询的执行计划 |
| pg_stat_statements | 扩展 | 统计 SQL 语句的执行次数、耗时、IO 等信息 |
| pg_buffercache | 扩展 | 检查共享缓冲区缓存内容与使用情况 |
| pg_prewarm | 扩展 | 将关系数据预热（预读）到操作系统缓存或共享缓冲区 |
| pg_freespacemap | 扩展 | 检查空闲空间映射（FSM） |
| pageinspect | 扩展 | 从底层检查数据库页面内容 |
| pg_visibility | 扩展 | 检查可见性映射（VM） |
| pg_walinspect | 扩展 | 从底层检查 WAL 日志内容 |
| pgrowlocks | 扩展 | 显示表的行级锁信息 |
| pgstattuple | 扩展 | 统计表/索引的物理元组与空间占用信息 |
| old_snapshot | 扩展 | 查看旧的快照信息（pg_stat_old_snapshot） |
| amcheck | 扩展 | 验证 B-tree 等索引及堆表的结构完整性，检测损坏 |

## 索引与数据类型（Indexes & Data Types）

| 模块 | 类型 | 主要能力 |
| --- | --- | --- |
| bloom | 索引 | 提供 Bloom 索引访问方法 |
| btree_gin | 索引 | 为 GIN 索引提供与 B-tree 兼容的操作符类 |
| btree_gist | 索引 | 为 GiST 索引提供与 B-tree 兼容的操作符类 |
| citext | 数据类型 | 大小写不敏感的文本类型 |
| cube | 数据类型 | 多维立方体（cube）数据类型 |
| hstore | 数据类型 | 键值对（hstore）数据类型 |
| intarray | 数据类型/函数 | 整数数组操作函数与索引支持 |
| intagg | 函数 | 整数聚合与枚举函数（已废弃） |
| isn | 数据类型 | ISBN/ISSN/EAN/UPC 等标准编号数据类型 |
| ltree | 数据类型 | 树状结构标签（label）数据类型 |
| seg | 数据类型 | 线段/区间数据类型 |
| pg_trgm | 数据类型/索引 | 三元组（trigram）模糊匹配与索引支持 |
| unaccent | 全文词典 | 去除重音符号的全文搜索词典 |
| dict_int | 全文词典 | 整数全文搜索词典模板 |
| dict_xsyn | 全文词典 | 扩展同义词全文搜索词典模板 |
| earthdistance | 函数 | 计算地球表面大圆距离 |
| fuzzystrmatch | 函数 | 模糊字符串匹配（Soundex、Levenshtein、Metaphone 等） |
| tablefunc | 表函数 | 提供交叉表（crosstab）等表函数 |

## 外部数据访问（Foreign Data & Connectivity）

| 模块 | 类型 | 主要能力 |
| --- | --- | --- |
| dblink | 扩展 | 从一个数据库会话中连接其他 PostgreSQL 数据库 |
| file_fdw | FDW | 通过外部表访问服务器上的文件（如 CSV） |
| postgres_fdw | FDW | 访问远程 PostgreSQL 服务器的外部数据包装器 |

## 备份与归档（Backup & Archive）

| 模块 | 类型 | 主要能力 |
| --- | --- | --- |
| basic_archive | 扩展 | 实现 WAL 归档的示例模块 |
| basebackup_to_shell | 扩展 | 通过外部 shell 命令执行 pg_basebackup 的示例模块 |
| pg_surgery | 扩展 | 修复损坏数据（如将死元组标记为存活）的低层工具函数 |

## 安全与加密（Security & Encryption）

| 模块 | 类型 | 主要能力 | 依赖 |
| --- | --- | --- | --- |
| auth_delay | 扩展 | 认证失败时人为延迟，缓解暴力破解 | — |
| passwordcheck | 扩展 | 检查密码强度，阻止弱口令 | — |
| pgcrypto | 扩展 | 提供摘要、加解密、随机数等加密函数 | --with-openssl |
| sslinfo | 扩展 | 提供 SSL 连接相关信息 | --with-openssl |
| sepgsql | 扩展 | 基于 SELinux 的强制访问控制 | --with-selinux |

## 过程语言桥接（PL Bridges）

| 模块 | 类型 | 主要能力 | 依赖 |
| --- | --- | --- | --- |
| bool_plperl | 类型转换 | bool 类型与 PL/Perl 之间的转换 | --with-perl |
| hstore_plperl | 类型转换 | hstore 与 PL/Perl 之间的转换 | --with-perl |
| jsonb_plperl | 类型转换 | jsonb 与 PL/Perl 之间的转换 | --with-perl |
| hstore_plpython | 类型转换 | hstore 与 PL/Python 之间的转换 | --with-python |
| jsonb_plpython | 类型转换 | jsonb 与 PL/Python 之间的转换 | --with-python |
| ltree_plpython | 类型转换 | ltree 与 PL/Python 之间的转换 | --with-python |

## 逻辑解码与复制（Logical Decoding & Replication）

| 模块 | 类型 | 主要能力 |
| --- | --- | --- |
| test_decoding | 逻辑解码 | 逻辑解码的测试/示例输出插件 |
| tcn | 扩展 | 触发器变更通知（Triggered Change Notification） |

## 采样方法（Tablesample Methods）

| 模块 | 类型 | 主要能力 |
| --- | --- | --- |
| tsm_system_rows | 采样方法 | 按指定行数返回样本表的 TABLESAMPLE 方法 |
| tsm_system_time | 采样方法 | 按指定时间返回样本表的 TABLESAMPLE 方法 |

## 工具与示例（Tools & Samples）

| 模块 | 类型 | 主要能力 | 依赖 |
| --- | --- | --- | --- |
| oid2name | 命令行工具 | 将 OID 和文件节点号解析为数据库对象名 | — |
| vacuumlo | 命令行工具 | 清理数据库中无用的大对象 | — |
| lo | 扩展 | 大对象（Large Object）的维护函数 | — |
| spi | 示例 | 演示 SPI（服务器编程接口）用法的示例模块 | — |
| start-scripts | 启动脚本 | 系统启动脚本（非扩展模块，无 Makefile） | — |
| xml2 | 扩展 | XPath 查询与 XML 处理函数 | --with-libxml |
| uuid-ossp | 扩展 | 生成 UUID | uuid（非 --with-uuid=no） |

## 编译与安装

原生 contrib 模块不会随普通构建自动编译：

1. 构建全部（包括 contrib）：

   ```sh
   make world
   ```

   或者只构建 contrib：

   ```sh
   make -C contrib
   make -C contrib install
   ```

2. 在数据库中注册模块：

   ```sql
   CREATE EXTENSION <module>;
   ```

> 注意：`pgcrypto`、`sslinfo`（依赖 SSL）、`sepgsql`（依赖 SELinux）、`xml2`
> （依赖 libxml）、`uuid-ossp`（依赖 uuid），以及 `*_plperl` / `*_plpython`
> 过程语言桥接模块，只有在 `configure` 时启用了对应的编译选项后才会被构建。
