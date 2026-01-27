# AliSQL

AliSQL is Alibaba's MySQL branch, forked from official MySQL and used extensively in Alibaba Group's production environment. It includes various performance optimizations, stability improvements, and features tailored for large-scale applications.

- [AliSQL](#alisql)
  - [🚀 Quick Start (DuckDB)](#-quick-start-duckdb)
  - [Version Information](#version-information)
  - [Features](#features)
  - [Roadmap](#roadmap)
  - [Getting Started](#getting-started)
  - [Support](#support)
  - [Contributing](#contributing)
  - [License](#license)
  - [See Also](#see-also)

## 🚀 Quick Start (DuckDB)

> **Quickly build your DuckDB node:** **[How to set up a DuckDB node](./wiki/duckdb/how-to-setup-duckdb-node-en.md)**

## Version Information

- **AliSQL Version**: 8.0.44 (LTS)
- **Based on**: MySQL 8.0.44

## Features

- **[DuckDB Storage Engine](./wiki/duckdb/duckdb-en.md)**:AliSQL integrates DuckDB as a native storage engine, allowing users to operate DuckDB with the same experience as MySQL. By leveraging AliSQL for rapid deployment of DuckDB service nodes, users can easily achieve lightweight analytical capabilities.

- **[Vector Storage](./wiki/vidx/vidx_readme.md)**:AliSQL natively supports enterprise-grade vector processing for up to 16,383 dimensions. By integrating a highly optimized HNSW algorithm for high-performance Approximate Nearest Neighbor (ANN) search, AliSQL empowers users to build AI-driven applications—such as semantic search and recommendation systems—seamlessly using standard SQL interfaces.

## Roadmap

- **[DDL Optimization](https://www.alibabacloud.com/help/en/rds/apsaradb-rds-for-mysql/alisql-ddl-best-practices?spm=a2c63.p38356.help-menu-26090.d_2_8_0.1f7a28a5F1ZVeK)** *(planned)*:AliSQL delivers a faster, safer, and lighter DDL experience through innovations such as enhanced Instant DDL, parallel B+tree construction, a non-blocking lock mechanism, and real-time DDL apply—significantly improving schema change efficiency and virtually eliminating replication lag.

- **[RTO Optimization](https://www.alibabacloud.com/help/en/rds/apsaradb-rds-for-mysql/best-practices-for-rto-optimization-in-alisql?spm=a3c0i.36496430.J_TlTAa0s_LXHOq4tuiO-gv.1.43c56e9bd5YdDQ&scm=20140722.S_help@@%E6%96%87%E6%A1%A3@@2880006._.ID_help@@%E6%96%87%E6%A1%A3@@2880006-RL_RDSMySQLRTO-LOC_2024SPAllResult-OR_ser-PAR1_0bc3b4af17685488697221621e29f2-V_4-PAR3_r-RE_new5-P0_0-P1_0)** *(planned)*:AliSQL deeply optimizes the end-to-end crash recovery path to accelerate instance startup, shorten RTO, and restore service quickly.

- **[Replication Optimization](https://www.alibabacloud.com/help/en/rds/apsaradb-rds-for-mysql/replication-optimization/?spm=a2c63.p38356.help-menu-26090.d_2_6.48a125033Ze9gw)** *(planned)*: AliSQL significantly boosts replication throughput and minimizes lag by implementing Binlog Parallel Flush, Binlog in Redo, and specialized optimizations for large transactions and DDL operations.

## Getting Started
**Prerequisites**:
- [CMake](https://cmake.org) 3.x or higher
- Python3
- C++17 compliant compiler (GCC 7+ or Clang 5+)

**Build Instructions**:

```bash
# Clone the repository
git clone https://github.com/alibaba/AliSQL.git
cd AliSQL

# Build the project (release build)
sh build.sh -t release -d /path/to/install/dir

# For development/debugging (debug build)
sh build.sh -t debug -d /path/to/install/dir

# Install the built MySQL server
make install
```

**Build Options**:
- `-t release|debug`: Build type (default: debug)
- `-d <dest_dir>`: Installation directory (default: /usr/local/alisql or $HOME/alisql)
- `-s <server_suffix>`: Server suffix (default: alisql-dev)
- `-g asan|tsan`: Enable sanitizer
- `-c`: Enable GCC coverage (gcov)
- `-h, --help`: Show help

## Support
- **GitHub Issues**: [https://github.com/alibaba/AliSQL/issues](https://github.com/alibaba/AliSQL/issues)
- **Alibaba Cloud RDS**: [DuckDB-based Analytical Instance](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/duckdb-based-analytical-instance/)

> For DuckDB-specific support, see the [DuckDB Support Options](https://duckdblabs.com/support/).

## Contributing

AliSQL 8.0 became an open-source project in December 2025 and is actively maintained by engineers at Alibaba Group.

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make your changes with appropriate tests
4. Submit a pull request

For bug reports and feature requests, please use the [GitHub Issues](https://github.com/alibaba/AliSQL/issues) page.

## License

This project is licensed under the GPL-2.0 license. See the [LICENSE](LICENSE) file for details.

AliSQL is based on MySQL, which is licensed under GPL-2.0. The DuckDB integration follows the same licensing terms.

## See Also
- [AliSQL Release Notes](./wiki/changes-in-alisql-8.0.44.md)
- [DuckDB Storage Engine in AliSQL](./wiki/duckdb/duckdb-en.md)
- [Vector Index in AliSQL](./wiki/vidx/vidx_readme.md)
- [MySQL 8.0 Documentation](https://dev.mysql.com/doc/refman/8.0/en/)
- [MySQL 8.0 Github Repository](https://github.com/mysql/mysql-server)
- [DuckDB Official Documentation](https://duckdb.org/docs/stable/)
- [DuckDB GitHub Repository](https://github.com/duckdb/duckdb)
- [Detailed Article (Chinese)](https://mp.weixin.qq.com/s/_YmlV3vPc9CksumXvXWBEw)
