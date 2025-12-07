# 从 Hive 读取数据到 MySQL 写入的完整流程详解

## 一、案例场景

**场景描述：**
- **数据源**：Hive 表（存储在 HDFS 上）
- **目标**：MySQL 数据库表
- **操作**：读取 Hive 表，进行数据转换和计算，写入 MySQL

**示例代码：**
```scala
val spark = SparkSession.builder()
  .appName("HiveToMySQL")
  .enableHiveSupport()
  .getOrCreate()

// 1. 从 Hive 读取数据
val hiveDF = spark.sql("SELECT * FROM hive_db.user_table WHERE dt='2024-01-01'")

// 2. 数据转换和计算
val resultDF = hiveDF
  .filter($"age" > 18)
  .groupBy($"city")
  .agg(sum($"amount").as("total_amount"))

// 3. 写入 MySQL
resultDF.write
  .mode("overwrite")
  .option("batchsize", "1000")
  .jdbc("jdbc:mysql://localhost:3306/testdb", "result_table", 
    new Properties().apply {
      put("user", "root")
      put("password", "password")
    })
```

---

## 二、完整流程图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Spark Application (Driver)                      │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  1. 创建 SparkSession (enableHiveSupport)                               │
│     - 初始化 SparkContext                                               │
│     - 连接 Hive Metastore                                               │
│     - 创建 DAGScheduler, TaskScheduler                                  │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  2. 执行 SQL: SELECT * FROM hive_db.user_table WHERE dt='2024-01-01'   │
│     - Spark SQL 解析 SQL 语句                                           │
│     - 生成逻辑执行计划                                                   │
│     - 优化执行计划                                                       │
│     - 生成物理执行计划                                                   │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  3. HiveTableScanExec (读取 Hive 表)                                    │
│     ┌─────────────────────────────────────────────────────────────┐   │
│     │ 3.1 从 Hive Metastore 获取表元数据                            │   │
│     │     - 表路径: hdfs://namenode:9000/warehouse/user_table/     │   │
│     │     - 文件格式: Parquet/TextFile/ORC                         │   │
│     │     - SerDe: 序列化/反序列化类                                │   │
│     └─────────────────────────────────────────────────────────────┘   │
│                              │                                          │
│                              ▼                                          │
│     ┌─────────────────────────────────────────────────────────────┐   │
│     │ 3.2 HadoopTableReader.makeRDDForTable()                      │   │
│     │     - 创建 HadoopRDD 或 NewHadoopRDD                          │   │
│     │     - 调用 Hadoop InputFormat.getSplits()                   │   │
│     └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  4. 分区划分 (Partitioning)                                             │
│     ┌─────────────────────────────────────────────────────────────┐   │
│     │ 4.1 Hadoop InputFormat.getSplits()                          │   │
│     │     - 扫描 HDFS 文件                                        │   │
│     │     - 按文件块大小切分 (默认 128MB)                          │   │
│     │     - 每个 Split 对应一个 RDD Partition                      │   │
│     │                                                             │   │
│     │     示例：                                                   │   │
│     │     - 文件1: 500MB → 4 个 Split (128MB each)                │   │
│     │     - 文件2: 200MB → 2 个 Split (128MB + 72MB)              │   │
│     │     - 文件3: 50MB  → 1 个 Split (50MB)                      │   │
│     │     - 总计: 7 个 Partitions                                  │   │
│     └─────────────────────────────────────────────────────────────┘   │
│                              │                                          │
│                              ▼                                          │
│     ┌─────────────────────────────────────────────────────────────┐   │
│     │ 4.2 创建 HadoopRDD[Writable]                                 │   │
│     │     - 每个 Partition 包含:                                  │   │
│     │       * InputSplit (文件路径、偏移量、长度)                   │   │
│     │       * Preferred Locations (数据本地性)                     │   │
│     └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  5. 并行读取 (Parallel Reading)                                        │
│     ┌─────────────────────────────────────────────────────────────┐   │
│     │ 5.1 DAGScheduler 提交 Job                                   │   │
│     │     - 创建 ResultStage                                      │   │
│     │     - 创建 ShuffleMapStage (如果有 Shuffle)                  │   │
│     │     - 提交 Stage 到 TaskScheduler                            │   │
│     └─────────────────────────────────────────────────────────────┘   │
│                              │                                          │
│                              ▼                                          │
│     ┌─────────────────────────────────────────────────────────────┐   │
│     │ 5.2 TaskScheduler 分配 Task                                  │   │
│     │     - 为每个 Partition 创建一个 Task                        │   │
│     │     - 考虑数据本地性 (PROCESS_LOCAL > NODE_LOCAL > ...)      │   │
│     │     - 将 Task 分配到 Executor                                │   │
│     └─────────────────────────────────────────────────────────────┘   │
│                              │                                          │
│                              ▼                                          │
│     ┌─────────────────────────────────────────────────────────────┐   │
│     │ 5.3 Executor 并行执行 Task                                    │   │
│     │                                                             │   │
│     │     Executor 1: Task 0 (Partition 0)                        │   │
│     │     ├─ 读取 hdfs://.../file1 (0-128MB)                     │   │
│     │     └─ 反序列化为 InternalRow                              │   │
│     │                                                             │   │
│     │     Executor 2: Task 1 (Partition 1)                        │   │
│     │     ├─ 读取 hdfs://.../file1 (128-256MB)                    │   │
│     │     └─ 反序列化为 InternalRow                              │   │
│     │                                                             │   │
│     │     Executor 3: Task 2 (Partition 2)                        │   │
│     │     ├─ 读取 hdfs://.../file1 (256-384MB)                    │   │
│     │     └─ 反序列化为 InternalRow                              │   │
│     │                                                             │   │
│     │     ... (并行执行所有 Task)                                 │   │
│     └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  6. 数据转换 (Transformations)                                          │
│     ┌─────────────────────────────────────────────────────────────┐   │
│     │ 6.1 Filter: filter($"age" > 18)                             │   │
│     │     - 窄依赖 (Narrow Dependency)                             │   │
│     │     - 每个 Partition 独立处理                                 │   │
│     │     - 不改变分区数                                            │   │
│     └─────────────────────────────────────────────────────────────┘   │
│                              │                                          │
│                              ▼                                          │
│     ┌─────────────────────────────────────────────────────────────┐   │
│     │ 6.2 GroupBy + Agg: groupBy($"city").agg(sum($"amount"))      │   │
│     │     - 宽依赖 (Shuffle Dependency)                            │   │
│     │     - 触发 Shuffle                                            │   │
│     │     - 重新分区 (默认 spark.default.parallelism)                │   │
│     └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  7. Shuffle 过程                                                        │
│     ┌─────────────────────────────────────────────────────────────┐   │
│     │ 7.1 Map Side (ShuffleMapStage)                               │   │
│     │     - 每个 Task 按 key (city) 分区                            │   │
│     │     - 写入本地磁盘 (Shuffle 文件)                             │   │
│     │     - 记录 MapOutputTracker                                  │   │
│     └─────────────────────────────────────────────────────────────┘   │
│                              │                                          │
│                              ▼                                          │
│     ┌─────────────────────────────────────────────────────────────┐   │
│     │ 7.2 Reduce Side (ResultStage)                               │   │
│     │     - 每个 Task 从多个 Map Task 拉取数据                      │   │
│     │     - 按 key 聚合 (sum)                                      │   │
│     │     - 生成最终结果                                            │   │
│     └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  8. 写入 MySQL (JDBC Write)                                              │
│     ┌─────────────────────────────────────────────────────────────┐   │
│     │ 8.1 DataFrameWriter.jdbc()                                   │   │
│     │     - 检查表是否存在                                         │   │
│     │     - 创建表 (如果不存在)                                     │   │
│     │     - 调用 saveTable()                                       │   │
│     └─────────────────────────────────────────────────────────────┘   │
│                              │                                          │
│                              ▼                                          │
│     ┌─────────────────────────────────────────────────────────────┐   │
│     │ 8.2 分区写入策略                                              │   │
│     │     - 每个 RDD Partition → 一个 JDBC 写入 Task                │   │
│     │     - 并行写入多个分区                                        │   │
│     │     - 每个分区使用独立的事务                                  │   │
│     └─────────────────────────────────────────────────────────────┘   │
│                              │                                          │
│                              ▼                                          │
│     ┌─────────────────────────────────────────────────────────────┐   │
│     │ 8.3 Executor 并行写入                                        │   │
│     │                                                             │   │
│     │     Executor 1: Task 0 (Partition 0)                        │   │
│     │     ├─ 建立 JDBC 连接                                        │   │
│     │     ├─ 批量插入 (batchSize=1000)                             │   │
│     │     └─ 提交事务                                              │   │
│     │                                                             │   │
│     │     Executor 2: Task 1 (Partition 1)                        │   │
│     │     ├─ 建立 JDBC 连接                                        │   │
│     │     ├─ 批量插入 (batchSize=1000)                             │   │
│     │     └─ 提交事务                                              │   │
│     │                                                             │   │
│     │     ... (并行写入所有分区)                                   │   │
│     └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 三、详细流程解析

### 3.1 从 Hive 读取数据

#### 3.1.1 Hive 表扫描

```111:144:sql/hive/src/main/scala/org/apache/spark/sql/hive/TableReader.scala
  def makeRDDForTable(
      hiveTable: HiveTable,
      deserializerClass: Class[_ <: Deserializer],
      filterOpt: Option[PathFilter]): RDD[InternalRow] = {

    assert(!hiveTable.isPartitioned,
      "makeRDDForTable() cannot be called on a partitioned table, since input formats may " +
      "differ across partitions. Use makeRDDForPartitionedTable() instead.")

    // Create local references to member variables, so that the entire `this` object won't be
    // serialized in the closure below.
    val localTableDesc = tableDesc
    val broadcastedHadoopConf = _broadcastedHadoopConf

    val tablePath = hiveTable.getPath
    val inputPathStr = applyFilterIfNeeded(tablePath, filterOpt)

    // logDebug("Table input: %s".format(tablePath))
    val hadoopRDD = createHadoopRDD(localTableDesc, inputPathStr)

    val attrsWithIndex = attributes.zipWithIndex
    val mutableRow = new SpecificInternalRow(attributes.map(_.dataType))

    val deserializedHadoopRDD = hadoopRDD.mapPartitions { iter =>
      val hconf = broadcastedHadoopConf.value.value
      val deserializer = deserializerClass.getConstructor().newInstance()
      DeserializerLock.synchronized {
        deserializer.initialize(hconf, localTableDesc.getProperties)
      }
      HadoopTableReader.fillObject(iter, deserializer, attrsWithIndex, mutableRow, deserializer)
    }

    deserializedHadoopRDD
  }
```

**关键步骤：**
1. **获取表路径**：从 Hive Metastore 获取表的 HDFS 路径
2. **创建 HadoopRDD**：调用 `createHadoopRDD()` 创建底层 RDD
3. **反序列化**：使用 Hive SerDe 将 Writable 转换为 InternalRow

#### 3.1.2 分区划分

**Hadoop InputFormat 分区规则：**

```226:257:core/src/main/scala/org/apache/spark/rdd/HadoopRDD.scala
  override def getPartitions: Array[Partition] = {
    val jobConf = getJobConf()
    // add the credentials here as this can be called before SparkContext initialized
    SparkHadoopUtil.get.addCredentials(jobConf)
    try {
      val allInputSplits = getInputFormat(jobConf).getSplits(jobConf, minPartitions)
      val inputSplits = if (ignoreEmptySplits) {
        allInputSplits.filter(_.getLength > 0)
      } else {
        allInputSplits
      }
      if (inputSplits.length == 1 && inputSplits(0).isInstanceOf[FileSplit]) {
        val fileSplit = inputSplits(0).asInstanceOf[FileSplit]
        val path = fileSplit.getPath
        if (fileSplit.getLength > conf.get(IO_WARNING_LARGEFILETHRESHOLD)) {
          val codecFactory = new CompressionCodecFactory(jobConf)
          if (Utils.isFileSplittable(path, codecFactory)) {
            logWarning(log"Loading one large file ${MDC(PATH, path.toString)} " +
              log"with only one partition, " +
              log"we can increase partition numbers for improving performance.")
          } else {
            logWarning(log"Loading one large unsplittable file ${MDC(PATH, path.toString)} " +
              log"with only one " +
              log"partition, because the file is compressed by unsplittable compression codec.")
          }
        }
      }
      val array = new Array[Partition](inputSplits.size)
      for (i <- 0 until inputSplits.size) {
        array(i) = new HadoopPartition(id, i, inputSplits(i))
      }
      array
    } catch {
```

**分区数决定因素：**
1. **文件大小**：按 HDFS 块大小（默认 128MB）切分
2. **文件数量**：每个文件至少一个分区
3. **`minPartitions`**：最小分区数（默认 `defaultMinPartitions = 2`）
4. **压缩格式**：不可切分的压缩格式（如 gzip）只能一个分区

**示例：**
```
Hive 表数据：
- file1.parquet: 500MB (4 个 HDFS 块)
- file2.parquet: 200MB (2 个 HDFS 块)
- file3.parquet: 50MB  (1 个 HDFS 块)

分区结果：
- Partition 0: file1 (0-128MB)
- Partition 1: file1 (128-256MB)
- Partition 2: file1 (256-384MB)
- Partition 3: file1 (384-500MB)
- Partition 4: file2 (0-128MB)
- Partition 5: file2 (128-200MB)
- Partition 6: file3 (0-50MB)

总计：7 个分区
```

### 3.2 并行读取机制

#### 3.2.1 数据本地性

**Task 调度优先级：**
1. **PROCESS_LOCAL**：数据在同一个 Executor 的缓存中
2. **NODE_LOCAL**：数据在同一节点的磁盘上
3. **NO_PREF**：无偏好
4. **RACK_LOCAL**：数据在同一机架
5. **ANY**：任意位置

**HadoopRDD 的首选位置：**

```133:136:core/src/main/scala/org/apache/spark/rdd/RDD.scala
  /**
   * Optionally overridden by subclasses to specify placement preferences.
   */
  protected def getPreferredLocations(split: Partition): Seq[String] = Nil
```

HadoopRDD 会从 InputSplit 获取数据块位置，优先调度到数据所在节点。

#### 3.2.2 并行执行

**执行流程：**
```
Driver:
  ├─ DAGScheduler 创建 Stage
  ├─ TaskScheduler 创建 TaskSet
  └─ 分配 Task 到 Executor

Executor 1 (Node1):
  ├─ Task 0: 读取 Partition 0 (本地数据)
  └─ Task 3: 读取 Partition 3 (本地数据)

Executor 2 (Node2):
  ├─ Task 1: 读取 Partition 1 (本地数据)
  └─ Task 4: 读取 Partition 4 (本地数据)

Executor 3 (Node3):
  ├─ Task 2: 读取 Partition 2 (本地数据)
  ├─ Task 5: 读取 Partition 5 (本地数据)
  └─ Task 6: 读取 Partition 6 (本地数据)
```

**并行度 = 分区数 = Task 数**

### 3.3 数据转换和 Shuffle

#### 3.3.1 Filter（窄依赖）

```scala
.filter($"age" > 18)
```

- **依赖类型**：窄依赖（OneToOneDependency）
- **分区数**：不变（7 个分区）
- **执行方式**：每个分区独立处理，无需 Shuffle

#### 3.3.2 GroupBy + Agg（宽依赖）

```scala
.groupBy($"city").agg(sum($"amount"))
```

- **依赖类型**：宽依赖（ShuffleDependency）
- **分区数**：重新分区（默认 `spark.default.parallelism`）
- **执行方式**：
  1. **Map Side**：按 key (city) 分区，写入本地磁盘
  2. **Reduce Side**：从多个 Map Task 拉取数据，聚合

**Shuffle 分区数：**
- 如果设置了 `spark.default.parallelism = 200`，则 Shuffle 后为 200 个分区
- 如果未设置，则使用父 RDD 的最大分区数（7 个）

### 3.4 写入 MySQL

#### 3.4.1 JDBC 写入流程

```998:1020:sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala
  def saveTable(
      df: DataFrame,
      tableSchema: Option[StructType],
      isCaseSensitive: Boolean,
      options: JdbcOptionsInWrite): Unit = {
    val url = options.url
    val table = options.table
    val dialect = JdbcDialects.get(url)
    val rddSchema = df.schema
    val batchSize = options.batchSize
    val isolationLevel = options.isolationLevel

    val insertStmt = getInsertStatement(table, rddSchema, tableSchema, isCaseSensitive, dialect)
    val repartitionedDF = options.numPartitions match {
      case Some(n) if n <= 0 => throw QueryExecutionErrors.invalidJdbcNumPartitionsError(
        n, JDBCOptions.JDBC_NUM_PARTITIONS)
      case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
      case _ => df
    }
    repartitionedDF.foreachPartition { iterator => savePartition(
      table, iterator, rddSchema, insertStmt, batchSize, dialect, isolationLevel, options)
    }
  }
```

**关键步骤：**
1. **分区调整**：可以通过 `numPartitions` 参数调整写入分区数
2. **并行写入**：每个分区独立写入，使用 `foreachPartition`
3. **批量插入**：使用 `batchSize` 控制批量大小

#### 3.4.2 分区写入实现

```784:873:sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala
  def savePartition(
      table: String,
      iterator: Iterator[Row],
      rddSchema: StructType,
      insertStmt: String,
      batchSize: Int,
      dialect: JdbcDialect,
      isolationLevel: Int,
      options: JDBCOptions): Unit = {

    if (iterator.isEmpty) {
      return
    }

    val outMetrics = TaskContext.get().taskMetrics().outputMetrics

    val conn = dialect.createConnectionFactory(options)(-1)
    var committed = false

    var finalIsolationLevel = Connection.TRANSACTION_NONE
    if (isolationLevel != Connection.TRANSACTION_NONE) {
      try {
        val metadata = conn.getMetaData
        if (metadata.supportsTransactions()) {
          // Update to at least use the default isolation, if any transaction level
          // has been chosen and transactions are supported
          val defaultIsolation = metadata.getDefaultTransactionIsolation
          finalIsolationLevel = defaultIsolation
          if (metadata.supportsTransactionIsolationLevel(isolationLevel))  {
            // Finally update to actually requested level if possible
            finalIsolationLevel = isolationLevel
          } else {
            logWarning(log"Requested isolation level ${MDC(ISOLATION_LEVEL, isolationLevel)} " +
              log"is not supported; falling back to default isolation level " +
              log"${MDC(DEFAULT_ISOLATION_LEVEL, defaultIsolation)}")
          }
        } else {
          logWarning(log"Requested isolation level ${MDC(ISOLATION_LEVEL, isolationLevel)}, " +
            log"but transactions are unsupported")
        }
      } catch {
        case NonFatal(e) => logWarning("Exception while detecting transaction support", e)
      }
    }
    val supportsTransactions = finalIsolationLevel != Connection.TRANSACTION_NONE
    var totalRowCount = 0L
    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
        conn.setTransactionIsolation(finalIsolationLevel)
      }
      val stmt = conn.prepareStatement(insertStmt)
      val setters = rddSchema.fields.map(f => makeSetter(conn, dialect, f.dataType))
      val nullTypes = rddSchema.fields.map(f => getJdbcType(f.dataType, dialect).jdbcNullType)
      val numFields = rddSchema.fields.length

      try {
        var rowCount = 0

        stmt.setQueryTimeout(options.queryTimeout)

        while (iterator.hasNext) {
          val row = iterator.next()
          var i = 0
          while (i < numFields) {
            if (row.isNullAt(i)) {
              stmt.setNull(i + 1, nullTypes(i))
            } else {
              setters(i).apply(stmt, row, i)
            }
            i = i + 1
          }
          stmt.addBatch()
          rowCount += 1
          totalRowCount += 1
          if (rowCount % batchSize == 0) {
            stmt.executeBatch()
            rowCount = 0
          }
        }
        if (rowCount > 0) {
          stmt.executeBatch()
        }
      } finally {
        stmt.close()
      }
      if (supportsTransactions) {
        conn.commit()
      }
      committed = true
    } catch {
```

**写入特点：**
1. **每个分区独立事务**：每个分区使用独立的事务，失败只影响该分区
2. **批量插入**：使用 `PreparedStatement.addBatch()` 批量插入
3. **并行写入**：多个分区同时写入，提高吞吐量

**注意事项：**
- **不要创建太多分区**：过多的并行连接可能导致数据库压力过大
- **合理设置 batchSize**：平衡内存和性能
- **监控数据库连接数**：确保数据库连接池足够

---

## 四、分区和并行度详解

### 4.1 读取阶段的分区

**Hive 表读取分区数：**
```
分区数 = max(
    Hadoop InputFormat 计算的分区数,
    minPartitions
)

Hadoop InputFormat 计算规则：
- 每个文件至少 1 个分区
- 按 HDFS 块大小切分（默认 128MB）
- 不可切分的压缩格式（gzip）只能 1 个分区
```

**示例：**
```
场景：读取 10GB 的 Hive 表（Parquet 格式，未压缩）

计算：
- 文件大小：10GB = 10240MB
- HDFS 块大小：128MB
- 分区数：10240 / 128 = 80 个分区

如果 minPartitions = 100：
- 实际分区数：max(80, 100) = 100 个分区
```

### 4.2 Shuffle 阶段的分区

**GroupBy 后的分区数：**
```
分区数 = spark.default.parallelism（如果设置）
      = 父 RDD 的最大分区数（如果未设置）
```

**示例：**
```
场景：GroupBy 操作

输入：7 个分区（从 Hive 读取）
配置：spark.default.parallelism = 200

输出：200 个分区（Shuffle 后）
```

### 4.3 写入阶段的分区

**MySQL 写入分区数：**
```
分区数 = 输入 RDD 的分区数（默认）
      = numPartitions（如果指定）
```

**示例：**
```
场景：写入 MySQL

输入：200 个分区（Shuffle 后）
配置：numPartitions = 10（写入时指定）

输出：10 个分区（coalesce 后）
写入：10 个并行连接写入 MySQL
```

### 4.4 并行度优化建议

**读取阶段：**
- 如果文件很大，增加 `minPartitions` 提高并行度
- 如果文件很小，减少分区数避免调度开销

**Shuffle 阶段：**
- 设置 `spark.default.parallelism = 2-4 × CPU 核心数`
- 对于大表，可以设置更大的值

**写入阶段：**
- **不要设置太多分区**：建议 10-50 个分区
- 考虑数据库连接池大小
- 使用 `numPartitions` 参数控制写入并行度

---

## 五、性能优化建议

### 5.1 读取优化

1. **分区裁剪**：使用分区过滤减少数据扫描
   ```scala
   spark.sql("SELECT * FROM hive_db.user_table WHERE dt='2024-01-01'")
   ```

2. **列裁剪**：只选择需要的列
   ```scala
   spark.sql("SELECT city, amount FROM hive_db.user_table")
   ```

3. **文件格式**：使用列式存储格式（Parquet、ORC）

### 5.2 计算优化

1. **缓存中间结果**：如果多次使用，使用 `cache()`
   ```scala
   val filteredDF = hiveDF.filter($"age" > 18).cache()
   ```

2. **合理设置 Shuffle 分区数**：
   ```scala
   spark.conf.set("spark.default.parallelism", "200")
   ```

### 5.3 写入优化

1. **控制写入并行度**：
   ```scala
   resultDF.write
     .option("numPartitions", "10")  // 控制写入分区数
     .jdbc(...)
   ```

2. **批量大小**：
   ```scala
   resultDF.write
     .option("batchsize", "5000")  // 增加批量大小
     .jdbc(...)
   ```

3. **事务隔离级别**：
   ```scala
   resultDF.write
     .option("isolationLevel", "READ_COMMITTED")
     .jdbc(...)
   ```

---

## 六、完整示例代码

```scala
import java.util.Properties
import org.apache.spark.sql.SparkSession

object HiveToMySQLExample {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("HiveToMySQL")
      .config("spark.default.parallelism", "200")  // 设置默认并行度
      .config("spark.sql.shuffle.partitions", "200")  // Shuffle 分区数
      .enableHiveSupport()
      .getOrCreate()

    // 2. 从 Hive 读取数据
    val hiveDF = spark.sql("""
      SELECT 
        city,
        age,
        amount
      FROM hive_db.user_table
      WHERE dt = '2024-01-01'
        AND age > 18
    """)

    // 3. 数据转换和计算
    val resultDF = hiveDF
      .groupBy($"city")
      .agg(
        sum($"amount").as("total_amount"),
        count("*").as("user_count")
      )
      .orderBy($"total_amount".desc)

    // 4. 写入 MySQL
    val jdbcUrl = "jdbc:mysql://localhost:3306/testdb"
    val jdbcProps = new Properties()
    jdbcProps.put("user", "root")
    jdbcProps.put("password", "password")
    jdbcProps.put("driver", "com.mysql.cj.jdbc.Driver")

    resultDF.write
      .mode("overwrite")
      .option("numPartitions", "10")  // 控制写入并行度
      .option("batchsize", "5000")   // 批量大小
      .option("isolationLevel", "READ_COMMITTED")
      .jdbc(jdbcUrl, "result_table", jdbcProps)

    spark.stop()
  }
}
```

---

## 七、总结

### 7.1 数据流转路径

```
Hive 表 (HDFS)
  ↓ [HadoopRDD, 7 个分区]
并行读取 (7 个 Task)
  ↓ [Filter, 窄依赖]
过滤后数据 (7 个分区)
  ↓ [GroupBy, 宽依赖, Shuffle]
聚合后数据 (200 个分区)
  ↓ [JDBC Write, 10 个分区]
MySQL 表
```

### 7.2 关键点

1. **分区数决定并行度**：每个分区对应一个 Task
2. **数据本地性优化**：优先调度到数据所在节点
3. **Shuffle 重新分区**：宽依赖会触发重新分区
4. **写入并行度控制**：避免过多连接导致数据库压力

### 7.3 性能调优

- **读取**：合理设置 `minPartitions`，使用分区裁剪
- **计算**：设置 `spark.default.parallelism`，缓存中间结果
- **写入**：控制 `numPartitions`，合理设置 `batchSize`

