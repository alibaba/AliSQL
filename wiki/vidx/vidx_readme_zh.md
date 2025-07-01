# AliSQL Vector Index (vidx) 功能介绍

[ [AliSQL](../../README.md) | [Vector Index](./vidx_readme.md) | [向量索引](./vidx_readme_zh.md) ]

## 概述

AliSQL 原生支持最高 16,383 维向量数据的存储及计算，集成余弦相似度（COSINE）、欧式距离（EUCLIDEAN）等主流向量运算函数，并基于深度优化的 HNSW（Hierarchical Navigable Small World）算法构建高效近邻搜索能力，支持对全维度向量列建立索引。

AliSQL 向量能力可为大规模语义检索、智能推荐、多模态分析等场景提供开箱即用的向量化解决方案，用户可通过标准SQL接口无缝实现高精度向量匹配与复杂业务逻辑的融合计算。

### 核心特性

<div style="text-align: center;">
<img src="./pic/vidx_core_features_zh.png" alt="vidx 核心特性" style="width:75%; height:auto;">
</div>

- **高维向量支持**：支持最高 16,383 维度的浮点型向量数据存储
- **高效向量检索**：采用 HNSW (Hierarchical Navigable Small World) 图算法实现高性能的相似性搜索
- **多距离度量支持**：支持 EUCLIDEAN 和 COSINE 等距离计算方式
- **SIMD 硬件加速**：基于 SIMD 指令集优化，提高向量运算效率
- **搜索剪枝优化**：通过布隆过滤器等技术优化搜索过程
- **可配置参数**：支持调整索引参数以平衡检索精度和性能
- **混合查询支持**：支持向量数据与标量数据的联合查询

## 使用方法

### 向量字段定义

向量字段使用特殊的 [Field_vector](../../include/vidx/vidx_func.h#L43-L65) 类型定义，继承自 [Field_varstring](../../include/field.h#L793-L798)，使用二进制字符集存储浮点数数组。

```sql
CREATE TABLE table_name (
    id INT PRIMARY KEY,
    vector_col VECTOR(128)  -- 128维向量
);
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
    vector_col VECTOR(128),
    VECTOR INDEX vidx_name (vector_col) M=6 DISTANCE=COSINE  -- 指定参数
);
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
SELECT * FROM table_name ORDER BY VEC_DISTANCE(vector_col, VEC_FROMTEXT("[1,2,3,4,5]")) LIMIT 10;

-- 在结果中显示距离值
SELECT id, VEC_DISTANCE(vector_col, VEC_FROMTEXT("[1,2,3,4,5]")) AS distance 
FROM table_name ORDER BY distance LIMIT 10;
```

### 参数介绍

#### 系统变量

| 变量名 | 描述 | 类型 | 默认值 | 范围 |
|--------|------|------|--------|------|
| vidx_disabled | 禁用向量列和向量索引的创建 | global | ON | ON, OFF |
| vidx_default_distance | 默认向量距离类型 | session | EUCLIDEAN | EUCLIDEAN,COSINE |
| vidx_hnsw_default_m | HNSW 算法默认 m | session | 6 | [3, 200] |
| vidx_hnsw_ef_search | HNSW 算法默认 ef_search | session | 20 | [1, 10000] |
| vidx_hnsw_cache_size | HNSW 算法默认使用内存限制 | global | 1024 * 1024 | [1048576,18446744073709551615] |

#### 索引参数

- `M`: 控制图中每个节点的连接数，默认值为 6，有效范围是 3 到 200
- `DISTANCE`: 构建索引的距离类型，默认值为 EUCLIDEAN

### 注意事项

1. 当前版本只支持 RC (READ COMMITTED) 事务隔离级别
2. 仅支持在 InnoDB 引擎表上创建向量索引。
3. 创建、修改、删除向量索引无法使用 inplace 语法。
4. 向量索引不能被设置为 INVISIBLE。
5. 向量字段不能为 NULL。
6. 向量索引的创建和维护会消耗额外的存储空间和计算资源

### 错误处理

- `ER_NOT_SUPPORTED_YET`: 不支持的事务隔离级别
- `ER_WRONG_ARGUMENTS`: 函数参数错误
- `ER_VECTOR_INDEX_USAGE`: 向量索引使用错误
- `ER_VECTOR_INDEX_FAILED`: 向量索引操作失败

## 技术细节

### 向量搜索总体架构

HNSW 作为最为流行的 ANN 算法之一，在机构测评和工程实现上取得了广泛的认可和验证。目前 AliSQL 优先支持了基于 HNSW 算法的向量索引，向量搜索总体架构如下图所示。

<div style="text-align: center;">
<img src="./pic/vidx_architecture.png" alt="vidx 总体架构" style="width:75%; height:auto;">
</div>

- ANN 查询经过代价估计后选择合适的索引进行搜索，也可以用 FORCE INDEX 等 hint 指定向量索引进行搜索。
- 逻辑上有一张完整的 HNSW 图，该图的信息被组织成一张辅助表持久化存储在磁盘中，表中每行数据代表 HNSW 图中的一个节点，在这张图上流转 HNSW 算法即可实现向量的插入和检索。
- 不同于普通索引直接访问存储引擎，引入了向量索引插件，在内存中维护一个 HNSW 图的 Nodes Cache，提高查询效率。


### HNSW 算法

HNSW（Hierarchical Navigable Small World）是一种基于多层图结构的高效近似最近邻（ANN）搜索算法。其设计可以概括为：
- 【分层跳表】第 0 层的图有全部点的信息，从低往高，高一层的图是低一层图的缩略图，只有低一层图的部分点，顶层用于快速跳转，底层用于精确搜索。
- 【连接近邻】每一层都是根据向量距离连接的邻近图，每个点都记录它最近的几个点作为邻居。

<div style="text-align: center;">
<img src="./pic/hnsw.png" alt="hnsw" style="width:50%; height:auto;">
</div>

### 数据结构

AliSQL 引入了向量数据的公共缓存（MHNSW Share）和事务缓存（MHNSW Trx），用于加速向量查询性能并保证向量更新的事务安全，实现资源隔离与性能优化的平衡。公共缓存和事务缓存供不同的操作访问，有不同的设计目标：
- 【公共缓存】MHNSW Share 供只读事务访问，挂载于辅助表的 TABLE_SHARE 上。其核心目标是通过共享缓存减少重复加载向量节点的开销，提升查询效率。
- 【事务缓存】MHNSW Trx 继承自 MHNSW Share，供读写事务使用，挂载于会话的 thd_set_ha_data。每个读写事务创建独立的 MHNSW Trx 实例，缓存其访问的节点包括其修改的节点，避免对公共缓存造成污染，仅在提交时去更新公共缓存。

### 向量计算优化

- 【预计算策略】在节点缓存加载阶段，系统会预先计算向量距离并缓存结果，从而避免对高频访问节点的重复计算。
- 【SIMD 指令集加速】在计算优化层面，利用现代 CPU 的 SIMD 指令集（如 AVX512）对向量距离计算进行加速。通过布隆过滤器，系统能够批量处理多个向量，将原本需要多次执行的标量运算转换为并行化的向量操作。这一优化显著减少了 CPU 指令周期的消耗。

## RDS MySQL 开箱即用的向量能力

欢迎访问[阿里云 RDS MySQL 向量能力](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/vector-storage-1)，开源生态，开箱即用。
