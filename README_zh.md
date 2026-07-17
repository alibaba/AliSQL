<p align="center">
  <img src="./alisql-logo.png" width="180" alt="AliSQL Logo"/>
</p>

<h1 align="center">AliSQL</h1>

<p align="center">
  <strong>阿里巴巴企业级 MySQL 分支 - 集成 DuckDB OLAP 引擎与原生向量搜索</strong>
</p>

<p align="center">
  <em>经阿里巴巴生产环境大规模验证，支撑数百万数据库实例稳定运行</em>
</p>

<p align="center">
  <a href="https://github.com/alibaba/AliSQL/stargazers"><img src="https://img.shields.io/github/stars/alibaba/AliSQL?style=for-the-badge&logo=github&color=ffca28" alt="GitHub Stars"></a>
  <a href="https://github.com/alibaba/AliSQL/network/members"><img src="https://img.shields.io/github/forks/alibaba/AliSQL?style=for-the-badge&logo=github&color=8bc34a" alt="GitHub Forks"></a>
  <a href="https://github.com/alibaba/AliSQL/blob/master/LICENSE"><img src="https://img.shields.io/badge/License-GPL%202.0-blue?style=for-the-badge" alt="License"></a>
  <a href="https://github.com/alibaba/AliSQL/releases"><img src="https://img.shields.io/badge/MySQL-8.0.44%20LTS-orange?style=for-the-badge&logo=mysql&logoColor=white" alt="MySQL Version"></a>
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

## 为什么选择 AliSQL？

AliSQL 为 MySQL 带来企业级能力，将 InnoDB 可靠的 OLTP 性能与 DuckDB 极速的分析能力和原生向量搜索相结合 - 全部通过熟悉的 MySQL 接口使用。

<table>
<tr>
<td width="33%" align="center">

### 参考测试中多项查询加速超过 200 倍

随附的 [TPC-H SF100 参考结果](./wiki/duckdb/duckdb-zh.md#性能基准测试performance-benchmarks) 显示，DuckDB 的多项查询相比 InnoDB 加速超过 **200 倍**

</td>
<td width="33%" align="center">

### 原生向量搜索

内置 HNSW 算法，支持高达 **16,383 维**向量，满足 AI/ML 工作负载

</td>
<td width="33%" align="center">

### 兼容 MySQL 生态

继续使用熟悉的 MySQL 工具、驱动和 SQL，同时使用 AliSQL 扩展能力

</td>
</tr>
</table>

## 核心特性

| 特性 | 描述 | 状态 |
|------|------|------|
| **DuckDB 存储引擎** | 列式 OLAP 引擎，支持自动压缩，专为分析场景设计 | 已发布 |
| **向量索引 (VIDX)** | 原生向量存储与 ANN 搜索，基于 HNSW 算法，支持余弦和欧氏距离 | 已发布 |
| **Native Flashback** | 使用 `AS OF TIMESTAMP` 和保留的 Undo 快照查询 InnoDB 历史数据 | 已发布 |
| **大事务优化** | 已实现 Binlog Cache Free Flush，但标准 DuckDB 构建中优化路径当前不生效 | 标准构建未生效 |
| **Binlog 持久化优化** | Persist Binlog Into Redo V2 在保留崩溃恢复能力的同时降低同步 Binlog I/O | 已发布 |
| **DDL 优化** | Instant DDL、并行 B+树构建、非阻塞锁机制 | 规划中 |
| **RTO 优化** | 加速崩溃恢复，缩短实例启动时间 | 规划中 |

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
             - Undo 快照                - Free Flush*               - 快速崩溃恢复
```

`*` Free Flush 已包含实现，但标准 DuckDB 构建中的优化路径当前不生效。

## 文档

| 文档 | 描述 |
|------|------|
| [DuckDB 集成指南](./wiki/duckdb/duckdb-zh.md) | DuckDB 存储引擎完整使用指南 |
| [向量索引指南](./wiki/vidx/vidx_readme_zh.md) | 原生向量存储与 ANN 搜索 |
| [Native Flashback 指南](./wiki/native-flashback/native-flashback-zh.md) | InnoDB 历史查询与恢复 |
| [Binlog in Redo 指南](./wiki/binlog-in-redo/binlog-in-redo-zh.md) | Redo 持久化 Binlog 与回退规则 |
| [Binlog Cache Free Flush 指南](./wiki/binlog-cache-free-flush/binlog-cache-free-flush-zh.md) | 大事务优化及当前可用状态 |
| [发布说明](./wiki/changes-in-alisql-8.0.44.md) | AliSQL 8.0.44 新特性 |
| [搭建 DuckDB 节点](./wiki/duckdb/how-to-setup-duckdb-node-zh.md) | 快速搭建分析节点指南 |

**外部资源：**
- [MySQL 8.0 官方文档](https://dev.mysql.com/doc/refman/8.0/en/)
- [DuckDB 官方文档](https://duckdb.org/docs/stable/)
- [技术详解文章](https://mp.weixin.qq.com/s/_YmlV3vPc9CksumXvXWBEw)

## 阿里云 RDS MySQL 商业版

面向托管生产环境，阿里云 RDS MySQL 将部分 AliSQL 能力产品化，并提供服务侧的内核发布、产品拓扑、数据同步、备份、监控与技术支持。RDS 的适用条件和参数默认值可能与本源码分支不同，RDS 实例应以官方产品文档为准。

| 能力 | RDS MySQL 产品文档 |
|------|--------------------|
| DuckDB 分析实例 | [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance) / [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance) |
| 向量存储 | [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/vector-storage-1) / [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/vector-storage-1) |
| Native Flashback | [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/native-flashback) / [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/native-flashback) |
| Binlog Cache Free Flush | [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/binlog-cache-free-flush) / [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/binlog-cache-free-flush) |
| Binlog in Redo | [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/binlog-in-redo) / [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/binlog-in-redo) |

各功能的本地指南分别说明本源码分支与对应 RDS 商业能力的边界。

## 参与贡献

AliSQL 自 2016 年 8 月起即为开源项目，由阿里云数据库团队持续维护。当前 8.0.44 功能版本延续了这一开源版本线。

我们欢迎各种形式的贡献！

1. **Fork** 本仓库
2. **创建** 功能分支 (`git checkout -b feature/amazing-feature`)
3. **提交** 你的修改 (`git commit -m 'Add amazing feature'`)
4. **推送** 到分支 (`git push origin feature/amazing-feature`)
5. **发起** Pull Request

如有 Bug 反馈或功能建议，请通过 [GitHub Issues](https://github.com/alibaba/AliSQL/issues) 提交。

## 相关工具

### RDSAI CLI — AI 驱动的数据库助手

<p>
  <a href="https://github.com/aliyun/rdsai-cli"><img src="https://img.shields.io/badge/GitHub-rdsai--cli-blue?style=flat-square&logo=github" alt="RDSAI CLI"></a>
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.13+-blue.svg?style=flat-square" alt="Python 3.13+"></a>
</p>

[RDSAI CLI](https://github.com/aliyun/rdsai-cli) 是新一代 AI 驱动的数据库命令行工具，让你可以用**自然语言**与 AliSQL 和 MySQL 数据库交互。AI 代理会帮你完成 SQL 生成、执行计划分析、诊断优化等工作。

```bash
# 安装
curl -LsSf https://raw.githubusercontent.com/aliyun/rdsai-cli/main/install.sh | sh

# 连接数据库，使用自然语言查询
rdsai --host localhost -u root -p secret -D mydb
mysql> 分析 users 表的索引使用情况
mysql> 显示过去一小时的慢查询
mysql> 为什么这个查询很慢: SELECT * FROM users WHERE name LIKE '%john%'
```

**核心功能：**
- 自然语言转 SQL（支持中英文）
- AI 驱动的查询优化与诊断分析
- 按 `Ctrl+E` 即时分析执行计划
- 多模型 LLM 支持（通义千问、OpenAI、DeepSeek、Anthropic 等）
- 自动化性能基准测试与分析报告

👉 **[立即体验 RDSAI CLI](https://github.com/aliyun/rdsai-cli)**

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
  由 <a href="https://www.alibabacloud.com/product/apsaradb-for-rds-mysql">阿里云数据库团队</a> 精心打造
</p>

<p align="center">
  <a href="https://github.com/alibaba/AliSQL">GitHub</a> •
  <a href="https://github.com/mysql/mysql-server">MySQL</a> •
  <a href="https://github.com/duckdb/duckdb">DuckDB</a>
</p>
