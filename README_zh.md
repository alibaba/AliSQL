<p align="center">
  <img src="./alisql-logo.png" width="180" alt="AliSQL Logo"/>
</p>

<h1 align="center">AliSQL</h1>

<p align="center">
  <strong>阿里巴巴开源 MySQL 分支，集成 DuckDB 分析引擎与原生向量索引</strong>
</p>

<p align="center">
  <em>基于 MySQL 8.0.44，由阿里云数据库团队维护</em>
</p>

<p align="center">
  <a href="https://github.com/alibaba/AliSQL/stargazers"><img src="https://img.shields.io/github/stars/alibaba/AliSQL?style=for-the-badge&logo=github&color=ffca28" alt="GitHub Stars"></a>
  <a href="https://github.com/alibaba/AliSQL/network/members"><img src="https://img.shields.io/github/forks/alibaba/AliSQL?style=for-the-badge&logo=github&color=8bc34a" alt="GitHub Forks"></a>
  <a href="https://github.com/alibaba/AliSQL/blob/master/LICENSE"><img src="https://img.shields.io/badge/License-GPL%202.0-blue?style=for-the-badge" alt="License"></a>
  <a href="https://github.com/alibaba/AliSQL/releases"><img src="https://img.shields.io/badge/MySQL-8.0.44-orange?style=for-the-badge&logo=mysql&logoColor=white" alt="MySQL Version"></a>
</p>

<p align="center">
  <a href="#核心特性">特性</a> •
  <a href="#快速开始">快速开始</a> •
  <a href="#文档">文档</a> •
  <a href="#路线图">路线图</a> •
  <a href="#参与贡献">贡献</a>
</p>

<p align="center">
  <a href="./README_zh.md">简体中文</a> | <a href="./README.md">English</a>
</p>

## AliSQL 简介

AliSQL 是阿里云数据库团队维护的开源 MySQL 分支。本版本基于 MySQL 8.0.44，增加 DuckDB 分析型存储引擎、HNSW 向量索引、Native Flashback 和 Binlog 优化，并保留 MySQL 客户端协议。

<table>
<tr>
<td width="33%" align="center">

### 分析性能参考

随附的 [TPC-H SF100 测试结果](./wiki/duckdb/duckdb-zh.md#性能基准测试performance-benchmarks) 中，DuckDB 的多项查询在该测试环境下相比 InnoDB 加速超过 **200 倍**

</td>
<td width="33%" align="center">

### 原生向量索引

`VECTOR(N)` 类型和 HNSW 索引支持最高 **16,383 维**向量

</td>
<td width="33%" align="center">

### 兼容 MySQL 客户端

应用通过 MySQL 协议连接，可继续使用现有 MySQL 客户端和驱动

</td>
</tr>
</table>

## 核心特性

| 特性 | 描述 | 状态 |
|------|------|------|
| **DuckDB 存储引擎** | 面向分析表的列式执行与存储，支持自动压缩 | 已发布 |
| **向量索引 (VIDX)** | 原生向量存储与 ANN 搜索，基于 HNSW 算法，支持余弦和欧氏距离 | 已发布 |
| **Native Flashback** | 使用 `AS OF TIMESTAMP` 和保留的 Undo 快照查询 InnoDB 历史数据 | 已发布 |
| **大事务优化** | Binlog Cache Free Flush 减少 InnoDB 大事务提交阶段的 Binlog I/O 放大 | 已发布* |
| **Binlog 持久化优化** | Persist Binlog Into Redo V2 在保留崩溃恢复能力的同时降低同步 Binlog I/O | 已发布 |
| **DDL 优化** | Instant DDL、并行 B+树构建、非阻塞锁机制 | 规划中 |
| **RTO 优化** | 加速崩溃恢复，缩短实例启动时间 | 规划中 |

`*` Free Flush 已支持 InnoDB 大事务。当前版本中，DuckDB 会注册额外的 2PC 参与者，因此包含 DuckDB 的 AliSQL 使用普通 Binlog Group Commit；下一版本将支持 DuckDB 场景的大事务优化。

## 快速开始

### 方式一：从源码构建

```bash
# 克隆仓库
git clone https://github.com/alibaba/AliSQL.git
cd AliSQL

# 构建（release 模式）
sh build.sh -t release -d ~/alisql

# 安装
make install
```

### 方式二：搭建 DuckDB 分析节点

> **详细指南：** [如何搭建 DuckDB 节点](./wiki/duckdb/how-to-setup-duckdb-node-zh.md)

### 初始化并启动服务

```bash
# 初始化数据目录
~/alisql/bin/mysqld --initialize-insecure --datadir=~/alisql/data

# 为下方示例启用 DuckDB 并启动服务
~/alisql/bin/mysqld --datadir=~/alisql/data --duckdb_mode=ON
```

## 使用示例

### 使用 DuckDB 进行数据分析

```sql
-- 创建使用 DuckDB 引擎的分析表
CREATE TABLE sales_analytics (
    sale_date DATE,
    product_id INT,
    revenue DECIMAL(10,2),
    quantity INT
) ENGINE=DuckDB;

-- 使用 DuckDB 执行分析聚合
SELECT
    DATE_FORMAT(sale_date, '%Y-%m') as month,
    SUM(revenue) as total_revenue,
    COUNT(*) as transactions
FROM sales_analytics
GROUP BY month
ORDER BY total_revenue DESC;
```

### 使用向量搜索构建 AI 应用

```sql
-- 向量功能默认关闭，向量索引要求使用 RC 隔离级别
SET GLOBAL vidx_disabled = OFF;
SET SESSION transaction_isolation = 'READ-COMMITTED';

-- 创建包含向量列的表
CREATE TABLE embeddings (
    id INT PRIMARY KEY,
    content TEXT,
    embedding VECTOR(3)
) ENGINE=InnoDB;

INSERT INTO embeddings VALUES
    (1, '第一篇文档', VEC_FROMTEXT('[0.1,0.2,0.3]')),
    (2, '第二篇文档', VEC_FROMTEXT('[0.2,0.1,0.4]'));

-- 创建 HNSW 索引以加速 ANN 搜索
CREATE VECTOR INDEX idx_embedding ON embeddings(embedding) DISTANCE=COSINE;

-- 使用余弦距离查找相似项
SELECT id, content,
       VEC_DISTANCE_COSINE(
           embedding, VEC_FROMTEXT('[0.1,0.2,0.3]')
       ) AS distance
FROM embeddings
ORDER BY distance
LIMIT 10;
```

## 构建选项

| 选项 | 描述 | 默认值 |
|------|------|--------|
| `-t release\|debug` | 构建类型 | `debug` |
| `-d <目录>` | 安装目录 | `/usr/local/alisql` 可写时使用该目录，否则为 `$HOME/alisql` |
| `-g asan\|tsan` | 启用内存/线程检测器 | 禁用 |
| `-c` | 启用代码覆盖率 (gcov) | 禁用 |

**前置依赖：** CMake 3.x+、Python 3、GCC 7+ 或 Clang 5+

## 路线图

```
2025 Q4  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
         [x] DuckDB 存储引擎        [x] 向量索引 (VIDX)        [x] 8.0.44 发布

2026     ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
         [x] Native Flashback       [x] 事务与 Binlog 优化      [ ] DDL / RTO
             - AS OF TIMESTAMP          - Binlog in Redo V2       - Instant DDL
             - Undo 快照                - Free Flush                - 快速崩溃恢复
```

## 文档

| 文档 | 描述 |
|------|------|
| [DuckDB 集成指南](./wiki/duckdb/duckdb-zh.md) | 架构、兼容性、复制与性能参考 |
| [向量索引指南](./wiki/vidx/vidx_readme_zh.md) | 原生向量存储与 ANN 搜索 |
| [Native Flashback 指南](./wiki/native-flashback/native-flashback-zh.md) | InnoDB 历史查询与恢复 |
| [Binlog in Redo 指南](./wiki/binlog-in-redo/binlog-in-redo-zh.md) | Redo 持久化 Binlog 与回退规则 |
| [Binlog Cache Free Flush 指南](./wiki/binlog-cache-free-flush/binlog-cache-free-flush-zh.md) | 大事务提交路径、配置与限制 |
| [发布说明](./wiki/changes-in-alisql-8.0.44.md) | AliSQL 8.0.44 新特性 |
| [搭建 DuckDB 节点](./wiki/duckdb/how-to-setup-duckdb-node-zh.md) | 快速搭建分析节点指南 |

**外部资源：**
- [MySQL 8.0 官方文档](https://dev.mysql.com/doc/refman/8.0/en/)
- [DuckDB 官方文档](https://duckdb.org/docs/stable/)
- [技术详解文章](https://mp.weixin.qq.com/s/_YmlV3vPc9CksumXvXWBEw)

## 阿里云 RDS MySQL 商业版

阿里云 RDS MySQL 提供部分 AliSQL 功能的托管版本，并负责内核发布、产品拓扑、数据同步、备份、监控与技术支持。RDS 支持的版本和参数默认值可能与本仓库不同，使用 RDS 实例时应以产品文档为准。

| 功能 | RDS MySQL 产品文档 |
|------|--------------------|
| DuckDB 分析实例 | [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance) / [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance) |
| 向量存储 | [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/vector-storage-1) / [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/vector-storage-1) |
| Native Flashback | [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/native-flashback) / [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/native-flashback) |
| Binlog Cache Free Flush | [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/binlog-cache-free-flush) / [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/binlog-cache-free-flush) |
| Binlog in Redo | [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/binlog-in-redo) / [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/binlog-in-redo) |

各功能指南会注明本源码与 RDS MySQL 的行为差异。

## 参与贡献

AliSQL 自 2016 年 8 月起即为开源项目，由阿里云数据库团队持续维护。当前 8.0.44 功能版本延续了这一开源版本线。

欢迎提交 Bug、文档修正和代码修改。

1. Fork 本仓库。
2. 创建功能分支 (`git checkout -b feature/my-change`)。
3. 提交修改 (`git commit -m 'Describe the change'`)。
4. 推送分支 (`git push origin feature/my-change`)。
5. 发起 Pull Request。

如有 Bug 反馈或功能建议，请通过 [GitHub Issues](https://github.com/alibaba/AliSQL/issues) 提交。

## 相关工具

### RDSAI CLI

<p>
  <a href="https://github.com/aliyun/rdsai-cli"><img src="https://img.shields.io/badge/GitHub-rdsai--cli-blue?style=flat-square&logo=github" alt="RDSAI CLI"></a>
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.13+-blue.svg?style=flat-square" alt="Python 3.13+"></a>
</p>

[RDSAI CLI](https://github.com/aliyun/rdsai-cli) 是面向 AliSQL 和 MySQL 的命令行客户端，支持通过自然语言生成 SQL、分析执行计划和诊断查询问题。

```bash
# 安装
curl -LsSf https://raw.githubusercontent.com/aliyun/rdsai-cli/main/install.sh | sh

# 连接数据库，使用自然语言查询
rdsai --host localhost -u root -p secret -D mydb
mysql> 分析 users 表的索引使用情况
mysql> 显示过去一小时的慢查询
mysql> 为什么这个查询很慢: SELECT * FROM users WHERE name LIKE '%john%'
```

主要功能：

- 自然语言转 SQL（支持中英文）
- 查询优化与诊断分析
- 按 `Ctrl+E` 即时分析执行计划
- 多模型 LLM 支持（通义千问、OpenAI、DeepSeek、Anthropic 等）
- 自动化性能基准测试与分析报告

[项目与安装说明](https://github.com/aliyun/rdsai-cli)

## 社区与支持

<table>
<tr>
<td align="center" width="50%">

**GitHub Issues**

Bug 反馈与功能建议

[提交 Issue](https://github.com/alibaba/AliSQL/issues)

</td>
<td align="center" width="50%">

**阿里云 RDS**

托管的 DuckDB 分析型实例

[了解更多](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance)

</td>
</tr>
</table>

## 开源协议

AliSQL 采用 **GPL-2.0** 协议开源，与 MySQL 保持一致。

详见 [LICENSE](LICENSE) 文件。

## Star 趋势

<p align="center">
  <a href="https://star-history.com/#alibaba/AliSQL&Date">
    <img src="https://api.star-history.com/svg?repos=alibaba/AliSQL&type=Date" alt="Star History Chart" width="600">
  </a>
</p>

<p align="center">
  由 <a href="https://www.alibabacloud.com/product/apsaradb-for-rds-mysql">阿里云数据库团队</a> 维护
</p>

<p align="center">
  <a href="https://github.com/alibaba/AliSQL">GitHub</a> •
  <a href="https://github.com/mysql/mysql-server">MySQL</a> •
  <a href="https://github.com/duckdb/duckdb">DuckDB</a>
</p>
