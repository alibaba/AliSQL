# DuckDB in AliSQL
[DuckDB](https://github.com/duckdb/duckdb) 是一个开源的在线分析处理（OLAP）和数据分析工作负载而设计。因其轻量、高性能、零配置和易集成的特性，正在迅速成为数据科学、BI 工具和嵌入式分析场景中的热门选择。DuckDB主要有以下几个特点：
- 卓越的查询性能：单机DuckDB的性能不但远高于InnoDB，甚至比ClickHouse和SelectDB的性能更好。
- 优秀的压缩比：DuckDB采用列式存储，根据类型自动选择合适的压缩算法，具有非常高的压缩率。
- 嵌入式设计：DuckDB是一个嵌入式的数据库系统，天然的适合被集成到MySQL中。
- 插件化设计：DuckDB采用了插件式的设计，非常方便进行第三方的开发和功能扩展。
- 友好的License：DuckDB的License允许任何形式的使用DuckDB的源代码，包括商业行为。

基于以上的几个原因，我们认为DuckDB非常适合成为MySQL的AP存储引擎。因此我们将DuckDB集成到了AliSQL中，将强大的分析能力带给 MySQL 用户，**用户可以像使用 MySQL 一样来操作 DuckDB**。目前，您可以通过 AliSQL 快速部署一个 DuckDB  服务节点，从而实现轻量级的分析能力。

## DuckDB 引擎的实现
### DuckDB 查询链路
InnoDB仅用来保存元数据和系统信息，如账号、配置等。所有的用户数据都存在DuckDB引擎中，InnoDB仅用来保存元数据和系统信息，如账号、配置等。

用户通过MySQL客户端连接到实例。查询到达后，MySQL首先进行解析和必要的处理。然后将SQL发送到DuckDB引擎执行。DuckDB执行完成后，将结果返回到Server层，server层将结果集转换成MySQL的结果集返回给客户。

查询链路最重要的工作就是兼容性的工作。DuckDB和MySQL的数据类型基本上是兼容的，但在语法和函数的支持上都和MySQL有比较大的差异，为此我们扩展了DuckDB的语法解析器，使其兼容MySQL特有的语法；重写了大量的DuckDB函数并新增了大量的MySQL函数，让常见的MySQL函数都可以准确运行。自动化兼容性测试平台大约17万SQL测试，显示兼容率达到99%。详细的兼容性情况见[《DuckDB 分析实例兼容性说明》](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/compatibility-of-duckdb-based-analytical-instances?spm=a2c4g.11186623.help-menu-26090.d_3_4_2.37e117e8adKlF2&scm=20140722.H_2964962._.OR_help-T_cn~zh-V_1)

### Binlog 幂等回放
DuckDB 节点可以作为主节点的备节点，主节点通过 Binlog 将数据同步至 DuckDB 节点。由于DuckDB不支持两阶段提交，因此无法利用两阶段提交来保证 Binlog GTID 和数据之间的一致性，也无法保证DDL操作中InnoDB的元数据和DuckDB的一致性。因此我们对事务提交的过程和 Binlog 的回放过程进行了改造，从而保证实例异常宕机重启后的数据一致性。

### DML 优化
由于DuckDB本身的实现上，有利于大事务的执行。频繁小事务的执行效率非常低迟。针对 DuckDB 引擎，我们专门设计了攒批(Batch)写入的方式，来提供更高的写入性能。

在 DuckDB 节点作为备库的场景下我们对 Binlog 回放做了优化，采用攒批的方式进行事务重放。优化后可以达到 30w rows/s的回放能力。在Sysbench压力测试中，能够做到没有复制延迟，比InnoDB的回放性能还高。

### DDL优化
DuckDB 作为独立的存储引擎接入到 ALiSQL 内核中，ALiSQL 实现了绝大部分 DuckDB 引擎的 DDL 操作。

对于 DuckDB 原生支持的 DDL 类型，ALiSQL 通过重新定义引擎层的 prepare_inplace_alter_table、inplace_alter_table 和 commit_inplace_alter_table 等接口 ，以 inplace/instant 的方式来实现；对于 DuckDB 原生无法支持的 DDL 类型，AliSQL 通过重新定义引擎层的 rnd_init、 rnd_next、rnd_end 等接口，以重建表的方式来实现。

将 InnoDB 表转为 DuckDB 引擎的操作是以重建表的方式进行的。在 InnoDB 引擎转为 DuckDB 的过程中，AlISQL 优化了重建表的过程，采用多线程并行读 InnoDB、多线程并行攒批写 DuckDB 的方式实现了高效的数据转换。


## DuckDB 引擎的性能
在 TPC-H sf100场景下，DuckDB 引擎的性能远超 InnoDB 引擎，相比 ClickHouse 有明显优势，
| Query ID | DuckDB | InnoDB | InnoDB |
| --- | --- | --- | --- |
|q1|0.92|1134.25|3.47|
|q2|0.15|1800|1.52|
|q3|0.53|802.94|3.65|
|q4|0.46|1000.45|2.77|
|q5|0.5|1800|5.38|
|q6|0.22|566.73|0.73|
|q7|0.59|1800|6.06|
|q8|0.68|1800|6.99|
|q9|1.44|1800|13.29|
|q10|0.91|894.35|3.22|
|q11|0.11|79.63|1.1|
|q12|0.44|734.35|1.69|
|q13|1.59|454.15|5.85|
|q14|0.38|574.07|0.83|
|q15|0.31|568.43|1.53|
|q16|0.32|63.56|0.52|
|q17|0.89|1800|7.96|
|q18|1.59|1800|3.11|
|q19|0.8|1800|2.96|
|q20|0.51|1800|3.38|
|q21|1.64|1800|内存不足|
|q22|0.33|361.4|4|
|总计|15.31|25234.31|80.01
