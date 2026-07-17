# AliSQL Vector Index (VIDX)

[ [AliSQL](../../README.md) | [Vector Index](./vidx_readme.md) | [向量索引](./vidx_readme_zh.md) ]

## Overview

AliSQL adds a `VECTOR(N)` column type for floating-point vectors with up to 16,383 dimensions. It provides Euclidean and cosine distance functions and an HNSW (Hierarchical Navigable Small World) index for approximate nearest-neighbor search.

Vector distance expressions and ordering can be combined with scalar predicates in standard SQL. Typical uses include semantic retrieval, recommendation, and multimodal search.

### Core Features

<div style="text-align: center;">
<img src="./pic/vidx_core_features.png" alt="vidx Core Features" style="width:75%; height:auto;">
</div>

- **Vector dimensions**: up to 16,383 floating-point values
- **ANN index**: HNSW graph stored in an InnoDB auxiliary table
- **Distance metrics**: `EUCLIDEAN` and `COSINE`
- **CPU paths**: SIMD implementations for supported processors
- **Search pruning**: Bloom-filter-assisted batching in the search path
- **Tuning**: configurable graph degree, search width, and cache size
- **SQL integration**: vector distance expressions can be used with scalar predicates

## Usage

Vector features are disabled by default. Enable them globally and use `READ COMMITTED` before creating or querying a vector index:

```sql
SET GLOBAL vidx_disabled = OFF;
SET SESSION transaction_isolation = 'READ-COMMITTED';
```

### Vector Field Definition

`VECTOR(N)` is implemented by [Field_vector](../../include/vidx/vidx_field.h#L30), which derives from [Field_varstring](../../sql/field.h#L3522). Vector values are stored as binary floating-point arrays.

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

### Creating Vector Index

Vector indexes can be created using the following syntax:

```sql
CREATE VECTOR INDEX vidx_name ON table_name (vector_col);  -- Using default parameters
```

Or specify directly in table definition:

```sql
CREATE TABLE table_name (
    id INT PRIMARY KEY,
    vector_col VECTOR(3),
    VECTOR INDEX vidx_name (vector_col) M=6 DISTANCE=COSINE  -- Specifying parameters
) ENGINE=InnoDB;
```

### Function Support

#### Vector Conversion Functions

| Function Name | Meaning |
|---------------|---------|
| VEC_FROMTEXT, TO_VECTOR, STRING_TO_VECTOR | String to vector |
| VEC_TOTEXT, FROM_VECTOR, VECTOR_TO_STRING | Vector to string |

#### Vector Calculation Functions

| Function Name | Meaning |
|---------------|---------|
| VECTOR_DIM | Vector dimension |
| VEC_DISTANCE, VEC_DISTANCE_EUCLIDEAN, VEC_DISTANCE_COSINE | Calculate distance between two vectors<br>If one of the arguments is a column in the vector index, distance type does not need to be specified, the vector index distance type will be automatically recognized |

Usage examples:

```sql
-- Sort using vector distance
SELECT *
FROM table_name
ORDER BY VEC_DISTANCE(vector_col, VEC_FROMTEXT('[1,2,3]'))
LIMIT 10;

-- Display distance value in results
SELECT id,
       VEC_DISTANCE_COSINE(vector_col, VEC_FROMTEXT('[1,2,3]')) AS distance
FROM table_name
ORDER BY distance
LIMIT 10;
```

### Parameter Introduction

#### System Variables

| Variable Name | Description | Type | Default Value | Range |
|---------------|-------------|------|---------------|-------|
| vidx_disabled | Disable creation of vector columns and vector indexes | global | ON | ON, OFF |
| vidx_default_distance | Default vector distance type | global, session | EUCLIDEAN | EUCLIDEAN, COSINE |
| vidx_hnsw_default_m | HNSW algorithm default m | global, session | 6 | [3, 200] |
| vidx_hnsw_ef_search | HNSW algorithm default ef_search | global, session | 20 | [1, 10000] |
| vidx_hnsw_cache_size | HNSW node-cache memory limit in bytes | global | 16 MiB (16777216) | [1048576,18446744073709551615] |

#### Index Parameters

- `M`: Controls the number of connections for each node in the graph, default value is 6, valid range is 3 to 200
- `DISTANCE`: Distance type for building index, default value is EUCLIDEAN

### Notes

1. Vector-index operations require the `READ COMMITTED` transaction isolation level.
2. Vector indexes are supported only on InnoDB tables.
3. Creating, modifying, and deleting vector indexes cannot use `ALGORITHM=INPLACE`.
4. Vector indexes cannot be set to `INVISIBLE`.
5. Vector columns may be nullable. Rows whose vector value is `NULL` are omitted from the vector index; scalar distance evaluation returns `NULL` and places those rows last in ascending distance order.
6. Query vectors must have the same dimension as the indexed column.
7. Creating and maintaining vector indexes consumes additional storage and compute resources.
8. HNSW layer assignment and neighbor selection use randomized and heuristic steps. Replicas built from the same rows are not guaranteed to have byte-identical graph topology.

### Error Handling

- `ER_NOT_SUPPORTED_YET`: Unsupported transaction isolation level
- `ER_WRONG_ARGUMENTS`: Function argument error
- `ER_VECTOR_INDEX_USAGE`: Vector index usage error
- `ER_VECTOR_INDEX_FAILED`: Vector index operation failure

## Technical Details

### Overall Architecture of Vector Search

AliSQL currently implements approximate nearest-neighbor indexes with HNSW. The diagram below shows the SQL, plugin, cache, and storage components used by a vector query.

<div style="text-align: center;">
<img src="./pic/vidx_architecture.png" alt="vidx Overall Architecture" style="width:75%; height:auto;">
</div>

- The optimizer can select a vector index by cost, or the query can select one with an index hint such as `FORCE INDEX`.
- The HNSW graph is persisted in an InnoDB auxiliary table, with one row for each graph node.
- The vector-index plugin loads graph nodes into an in-memory cache and runs HNSW insertion and search over those nodes.

### HNSW Algorithm

HNSW is an approximate nearest-neighbor algorithm built on a multilayer proximity graph:

- **Layers**: layer 0 contains every node; each higher layer contains a subset used for coarse navigation.
- **Neighbors**: nodes store a bounded set of nearby nodes, selected by vector distance and HNSW heuristics.

<div style="text-align: center;">
<img src="./pic/hnsw.png" alt="hnsw" style="width:50%; height:auto;">
</div>

### Data Structure

AliSQL uses two node caches with different lifetimes:

- **MHNSW Share** is attached to the auxiliary table's `TABLE_SHARE` and is shared by read-only transactions. It avoids loading the same graph nodes from the table for every query.
- **MHNSW Trx** is attached to the session through `thd_set_ha_data`. A read-write transaction keeps accessed and modified nodes in its own cache, then updates the shared cache at commit.

### Vector Computation Optimization

- **Precomputation**: the node-loading path caches distance data used repeatedly during graph traversal.
- **SIMD**: supported CPU paths use SIMD instructions, including AVX-512, for batched distance calculations. Bloom filters group candidate checks before these calculations.

## Alibaba Cloud RDS MySQL

RDS MySQL provides managed vector storage with service-side enablement, data synchronization, backup, and recovery. Supported versions, console operations, and parameter defaults are specific to RDS and may differ from this source tree.

- Official vector storage documentation: [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/vector-storage-1) / [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/vector-storage-1)
