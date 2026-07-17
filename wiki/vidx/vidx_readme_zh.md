# AliSQL 向量索引 (VIDX)

[ [AliSQL](../../README_zh.md) | [Vector Index](./vidx_readme.md) | [向量索引](./vidx_readme_zh.md) ]

## 概述

AliSQL 增加了 `VECTOR(N)` 列类型，可存储最高 16,383 维的浮点向量。它提供欧氏距离和余弦距离函数，并使用 HNSW（Hierarchical Navigable Small World）索引执行近似最近邻搜索。

向量距离排序可以在标准 SQL 中与标量条件组合使用，适用于语义检索、推荐和多模态搜索等场景。

### 核心特性

<div style="text-align: center;">
<img src="./pic/vidx_core_features_zh.png" alt="vidx 核心特性" style="width:75%; height:auto;">
</div>

- **向量维度**：最高 16,383 维浮点数据
- **ANN 索引**：HNSW 图存储在 InnoDB 辅助表中
- **距离类型**：`EUCLIDEAN` 和 `COSINE`
- **CPU 执行路径**：在支持的处理器上使用 SIMD 指令
- **搜索剪枝**：搜索路径使用布隆过滤器辅助批处理
- **参数调优**：可配置图连接数、搜索宽度和缓存大小
- **SQL 集成**：向量距离表达式可与标量条件组合

## 使用方法

向量功能默认关闭。创建或查询向量索引前，需要全局启用向量功能，并在当前会话中使用 `READ COMMITTED`：

```sql
SET GLOBAL vidx_disabled = OFF;
SET SESSION transaction_isolation = 'READ-COMMITTED';
```

### 向量字段定义

`VECTOR(N)` 由 [Field_vector](../../include/vidx/vidx_field.h#L30) 实现，该类型继承自 [Field_varstring](../../sql/field.h#L3522)。向量值以二进制浮点数组存储。

```sql
CREATE TABLE table_name (
    id INT PRIMARY KEY,
    vector_col VECTOR(3)
) ENGINE=InnoDB;

INSERT INTO table_name VALUES
    (1, VEC_FROMTEXT('[1,2,3]')),
    (2, VEC_FROMTEXT('[2,3,4]')),
    (3, NULL);
```

### 创建向量索引

向量索引可以通过以下语法创建：

```sql
CREATE VECTOR INDEX vidx_name ON table_name (vector_col);  -- 使用默认参数
```

或者在表定义中直接指定：

```sql
CREATE TABLE table_name (
    id INT PRIMARY KEY,
    vector_col VECTOR(3),
    VECTOR INDEX vidx_name (vector_col) M=6 DISTANCE=COSINE  -- 指定参数
) ENGINE=InnoDB;
```

### 函数支持

#### 向量转换函数

| 函数名 | 含义 |
|--------|------|
| VEC_FROMTEXT, TO_VECTOR, STRING_TO_VECTOR | 字符串转向量 |
| VEC_TOTEXT, FROM_VECTOR, VECTOR_TO_STRING | 向量转字符串 |

#### 向量计算函数

| 函数名 | 含义 |
|--------|------|
| VECTOR_DIM | 向量维度 |
| VEC_DISTANCE, VEC_DISTANCE_EUCLIDEAN, VEC_DISTANCE_COSINE | 计算两向量间的距离<br>若参数之一是向量索引中的列，可以不指定距离类型，会自动识别向量索引的距离类型 |

使用示例：

```sql
-- 使用向量距离进行排序
SELECT *
FROM table_name
ORDER BY VEC_DISTANCE(vector_col, VEC_FROMTEXT('[1,2,3]'))
LIMIT 10;

-- 在结果中显示距离值
SELECT id,
       VEC_DISTANCE_COSINE(vector_col, VEC_FROMTEXT('[1,2,3]')) AS distance
FROM table_name
ORDER BY distance
LIMIT 10;
```

### 参数介绍

#### 系统变量

| 变量名 | 描述 | 类型 | 默认值 | 范围 |
|--------|------|------|--------|------|
| vidx_disabled | 禁用向量列和向量索引的创建 | global | ON | ON, OFF |
| vidx_default_distance | 默认向量距离类型 | global, session | EUCLIDEAN | EUCLIDEAN,COSINE |
| vidx_hnsw_default_m | HNSW 算法默认 m | global, session | 6 | [3, 200] |
| vidx_hnsw_ef_search | HNSW 算法默认 ef_search | global, session | 20 | [1, 10000] |
| vidx_hnsw_cache_size | HNSW 节点缓存内存上限（字节） | global | 16 MiB (16777216) | [1048576,18446744073709551615] |

#### 索引参数

- `M`: 控制图中每个节点的连接数，默认值为 6，有效范围是 3 到 200
- `DISTANCE`: 构建索引的距离类型，默认值为 EUCLIDEAN

### 注意事项

1. 向量索引操作要求使用 `READ COMMITTED` 事务隔离级别。
2. 仅支持在 InnoDB 引擎表上创建向量索引。
3. 创建、修改、删除向量索引不能使用 `ALGORITHM=INPLACE`。
4. 向量索引不能设置为 `INVISIBLE`。
5. 向量列可以为 NULL。向量值为 `NULL` 的行不会写入向量索引；使用标量距离函数计算时结果为 `NULL`，按距离升序排列时位于末尾。
6. 查询向量的维度必须与索引列一致。
7. 向量索引的创建和维护会消耗额外的存储空间和计算资源。
8. HNSW 的层级分配和邻居选择包含随机与启发式步骤；即使副本包含相同数据，也不保证向量图拓扑逐字节一致。

### 错误处理

- `ER_NOT_SUPPORTED_YET`: 不支持的事务隔离级别
- `ER_WRONG_ARGUMENTS`: 函数参数错误
- `ER_VECTOR_INDEX_USAGE`: 向量索引使用错误
- `ER_VECTOR_INDEX_FAILED`: 向量索引操作失败

## 技术细节

### 向量搜索总体架构

AliSQL 当前使用 HNSW 实现近似最近邻索引。下图列出了向量查询涉及的 SQL 层、插件、缓存和存储组件。

<div style="text-align: center;">
<img src="./pic/vidx_architecture.png" alt="vidx 总体架构" style="width:75%; height:auto;">
</div>

- 优化器可根据代价选择向量索引，也可以通过 `FORCE INDEX` 等 Index Hint 指定索引。
- HNSW 图持久化在 InnoDB 辅助表中，每行对应一个图节点。
- 向量索引插件将图节点加载到内存缓存，并在这些节点上执行 HNSW 插入和搜索。


### HNSW 算法

HNSW 是基于多层邻近图的近似最近邻算法：

- **分层结构**：第 0 层包含所有节点，向上的每层只保留部分节点，用于粗粒度导航。
- **邻居连接**：每个节点保存数量受限的近邻，近邻由向量距离和 HNSW 启发式规则选择。

<div style="text-align: center;">
<img src="./pic/hnsw.png" alt="hnsw" style="width:50%; height:auto;">
</div>

### 数据结构

AliSQL 使用两类生命周期不同的节点缓存：

- **MHNSW Share** 挂载在辅助表的 `TABLE_SHARE` 上，由只读事务共享，避免每次查询都从辅助表重复加载相同节点。
- **MHNSW Trx** 通过 `thd_set_ha_data` 挂载在会话上。读写事务在独立缓存中保存访问和修改的节点，提交时再更新共享缓存。

### 向量计算优化

- **预计算**：节点加载路径会缓存图遍历中重复使用的距离数据。
- **SIMD**：在支持的 CPU 上使用 SIMD 指令（包括 AVX-512）批量计算向量距离；布隆过滤器用于在计算前组织候选检查。

## 阿里云 RDS MySQL

RDS MySQL 提供托管的向量存储，并由服务侧集成数据同步、备份和恢复。RDS 支持版本、控制台操作和参数默认值可能与本源码不同。

- 向量存储官方文档：[中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/vector-storage-1) / [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/vector-storage-1)
