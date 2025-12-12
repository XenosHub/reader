# Spark SQL 运行时配置分析

## 一、功能分类

### 1. 自适应查询执行 (Adaptive Query Execution, AQE)

#### 核心配置
- `spark.sql.adaptive.enabled` (默认: true) - 启用自适应查询执行
- `spark.sql.adaptive.coalescePartitions.enabled` (默认: true) - 合并小分区
- `spark.sql.adaptive.skewJoin.enabled` (默认: true) - 处理数据倾斜
- `spark.sql.adaptive.advisoryPartitionSizeInBytes` - 建议的分区大小

#### 使用场景
- **大数据量处理**: 当数据量很大且分区大小不均匀时
- **数据倾斜问题**: 处理 JOIN 操作中的数据倾斜
- **动态优化**: 需要根据运行时统计信息动态调整执行计划
- **性能调优**: 自动优化 shuffle 分区数量，减少小任务

#### 关键参数调优
```scala
// 处理严重数据倾斜
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 256MB
spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5.0

// 控制分区合并
spark.sql.adaptive.advisoryPartitionSizeInBytes = 64MB
spark.sql.adaptive.coalescePartitions.minPartitionSize = 1MB
```

---

### 2. 数据源优化配置

#### 2.1 Parquet 格式
- `spark.sql.parquet.enableVectorizedReader` (默认: true) - 向量化读取
- `spark.sql.parquet.filterPushdown` (默认: true) - 谓词下推
- `spark.sql.parquet.mergeSchema` (默认: false) - 合并 Schema
- `spark.sql.parquet.aggregatePushdown` (默认: false) - 聚合下推

#### 2.2 ORC 格式
- `spark.sql.orc.enableVectorizedReader` (默认: true)
- `spark.sql.orc.filterPushdown` (默认: true)
- `spark.sql.orc.aggregatePushdown` (默认: false)

#### 2.3 CSV/JSON 格式
- `spark.sql.csv.filterPushdown.enabled` (默认: true)
- `spark.sql.json.filterPushdown.enabled` (默认: true)

#### 使用场景
- **Parquet/ORC 数据仓库**: 大规模数据分析，需要高性能读取
- **Schema 演进**: 处理不同版本的数据文件
- **查询优化**: 通过谓词下推减少 I/O
- **聚合优化**: 在存储层进行预聚合（需要启用 aggregatePushdown）

---

### 3. JOIN 优化配置

#### 核心配置
- `spark.sql.autoBroadcastJoinThreshold` (默认: 10MB) - 广播 JOIN 阈值
- `spark.sql.adaptive.autoBroadcastJoinThreshold` - AQE 框架下的广播阈值
- `spark.sql.shuffledHashJoinFactor` (默认: 3) - Shuffle Hash Join 因子
- `spark.sql.broadcastTimeout` (默认: 300s) - 广播超时

#### 使用场景
- **小表 JOIN 大表**: 使用广播 JOIN 避免 shuffle
- **大表 JOIN 大表**: 调整 shuffledHashJoinFactor 选择最优策略
- **网络优化**: 减少数据传输，提高 JOIN 性能

---

### 4. 分区和分桶配置

#### 分区相关
- `spark.sql.shuffle.partitions` (默认: 200) - Shuffle 分区数
- `spark.sql.files.maxPartitionBytes` (默认: 128MB) - 文件分区最大字节数
- `spark.sql.files.minPartitionNum` - 最小分区数

#### 分桶相关
- `spark.sql.sources.bucketing.enabled` (默认: true) - 启用分桶
- `spark.sql.bucketing.coalesceBucketsInJoin.enabled` (默认: false) - JOIN 时分桶合并
- `spark.sql.sources.bucketing.maxBuckets` (默认: 100000) - 最大分桶数

#### 使用场景
- **数据倾斜**: 通过合理设置分区数避免数据倾斜
- **JOIN 优化**: 使用分桶表避免 shuffle
- **文件大小控制**: 控制输出文件大小，避免小文件问题

---

### 5. 流处理配置 (Structured Streaming)

#### 核心配置
- `spark.sql.streaming.checkpointLocation` - 检查点位置
- `spark.sql.streaming.stopActiveRunOnRestart` (默认: true) - 重启时停止旧运行
- `spark.sql.streaming.multipleWatermarkPolicy` (默认: min) - 多水印策略
- `spark.sql.streaming.stateStore.stateSchemaCheck` (默认: true) - 状态 Schema 检查

#### 使用场景
- **实时数据处理**: Kafka、Kinesis 等流数据源
- **状态管理**: 需要维护状态的流处理（如窗口聚合）
- **容错恢复**: 通过检查点实现故障恢复
- **多流处理**: 处理多个流的水印策略

---

### 6. Python/Pandas UDF 优化

#### Arrow 优化
- `spark.sql.execution.arrow.pyspark.enabled` - 启用 Arrow 优化
- `spark.sql.execution.arrow.maxRecordsPerBatch` (默认: 10000) - 每批最大记录数
- `spark.sql.execution.pythonUDF.arrow.enabled` (默认: false) - Python UDF Arrow 优化

#### 使用场景
- **PySpark 性能优化**: DataFrame 与 Pandas 之间的高效转换
- **批量处理**: 通过 Arrow 减少序列化开销
- **数据科学工作流**: 与 Pandas、NumPy 集成

---

### 7. ANSI SQL 兼容性

#### 核心配置
- `spark.sql.ansi.enabled` (默认: true) - 启用 ANSI 模式
- `spark.sql.storeAssignmentPolicy` (默认: ANSI) - 类型转换策略
- `spark.sql.ansi.enforceReservedKeywords` (默认: false) - 强制保留关键字

#### 使用场景
- **SQL 标准兼容**: 需要与标准 SQL 兼容的应用
- **严格类型检查**: 防止隐式类型转换导致的数据错误
- **迁移场景**: 从其他数据库系统迁移到 Spark

---

### 8. 性能调优配置

#### 内存和缓存
- `spark.sql.inMemoryColumnarStorage.batchSize` (默认: 10000) - 列式缓存批次大小
- `spark.sql.inMemoryColumnarStorage.compressed` (默认: true) - 列式缓存压缩
- `spark.sql.defaultCacheStorageLevel` (默认: MEMORY_AND_DISK) - 默认缓存级别

#### 执行优化
- `spark.sql.execution.topKSortFallbackThreshold` (默认: 2147483632) - Top-K 排序阈值
- `spark.sql.leafNodeDefaultParallelism` - 叶子节点默认并行度

#### 使用场景
- **内存优化**: 控制缓存策略，平衡内存使用和性能
- **排序优化**: 小结果集使用内存排序，大结果集使用磁盘排序
- **并行度调优**: 根据集群资源调整并行度

---

### 9. 元数据和统计信息

#### 核心配置
- `spark.sql.cbo.enabled` (默认: false) - 基于成本的优化器
- `spark.sql.statistics.histogram.enabled` (默认: false) - 直方图统计
- `spark.sql.statistics.size.autoUpdate.enabled` (默认: false) - 自动更新表大小

#### 使用场景
- **查询优化**: CBO 需要统计信息来生成最优执行计划
- **JOIN 顺序优化**: 基于统计信息重新排序 JOIN
- **资源估算**: 准确估算查询资源需求

---

### 10. 时区和时间戳

#### 核心配置
- `spark.sql.session.timeZone` - 会话时区
- `spark.sql.timestampType` (默认: TIMESTAMP_LTZ) - 时间戳类型
- `spark.sql.datetime.java8API.enabled` (默认: false) - 使用 Java 8 时间 API

#### 使用场景
- **国际化应用**: 处理不同时区的数据
- **时间戳精度**: 选择合适的时间戳类型（LTZ vs NTZ）
- **兼容性**: 与 Java 8+ 时间 API 集成

---

## 二、使用场景总结

### 场景 1: 大数据分析 (OLAP)
**推荐配置组合:**
```scala
spark.sql.adaptive.enabled = true
spark.sql.adaptive.coalescePartitions.enabled = true
spark.sql.adaptive.skewJoin.enabled = true
spark.sql.parquet.enableVectorizedReader = true
spark.sql.parquet.filterPushdown = true
spark.sql.shuffle.partitions = 200  // 根据数据量调整
```

### 场景 2: 实时流处理
**推荐配置组合:**
```scala
spark.sql.streaming.checkpointLocation = "/path/to/checkpoint"
spark.sql.streaming.stateStore.stateSchemaCheck = true
spark.sql.streaming.stopActiveRunOnRestart = true
```

### 场景 3: PySpark 数据科学
**推荐配置组合:**
```scala
spark.sql.execution.arrow.pyspark.enabled = true
spark.sql.execution.arrow.maxRecordsPerBatch = 10000
spark.sql.execution.pandas.inferPandasDictAsMap = false
```

### 场景 4: 数据倾斜处理
**推荐配置组合:**
```scala
spark.sql.adaptive.enabled = true
spark.sql.adaptive.skewJoin.enabled = true
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 256MB
spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5.0
```

### 场景 5: 小文件优化
**推荐配置组合:**
```scala
spark.sql.files.maxPartitionBytes = 128MB
spark.sql.files.minPartitionNum = 1
spark.sql.adaptive.coalescePartitions.enabled = true
```

---

## 三、配置调优建议

### 1. 性能优化优先级

**高优先级 (必须配置):**
- AQE 相关配置（现代 Spark 版本默认启用）
- 数据源向量化读取
- 谓词下推

**中优先级 (根据场景):**
- 广播 JOIN 阈值
- Shuffle 分区数
- 缓存策略

**低优先级 (特殊需求):**
- CBO 和统计信息
- ANSI 兼容性
- 流处理特定配置

### 2. 常见问题及解决方案

| 问题 | 相关配置 | 建议值 |
|------|---------|--------|
| 数据倾斜 | `spark.sql.adaptive.skewJoin.*` | 根据数据分布调整 |
| 小文件过多 | `spark.sql.files.maxPartitionBytes` | 128MB - 256MB |
| JOIN 性能差 | `spark.sql.autoBroadcastJoinThreshold` | 根据小表大小调整 |
| 内存不足 | `spark.sql.inMemoryColumnarStorage.batchSize` | 减小批次大小 |
| 查询慢 | `spark.sql.shuffle.partitions` | 增加分区数 |

### 3. 版本兼容性注意事项

- **Spark 3.0+**: AQE 默认启用，建议保持默认配置
- **Spark 3.4+**: 新增 TIMESTAMP_NTZ 支持
- **Spark 4.0+**: 新增多项流处理和优化配置

---

## 四、最佳实践总结

### 1. 默认配置原则
- **保持默认**: 大多数默认配置已经过优化，无需修改
- **按需调整**: 根据实际工作负载和数据特征调整
- **监控验证**: 修改配置后监控性能指标

### 2. 配置管理策略
- **环境隔离**: 开发、测试、生产环境使用不同配置
- **文档记录**: 记录配置变更原因和效果
- **渐进调优**: 一次只修改少量配置，观察效果

### 3. 性能调优流程
1. **基准测试**: 记录当前性能指标
2. **问题诊断**: 识别性能瓶颈
3. **配置调整**: 针对性修改配置
4. **验证测试**: 对比性能提升
5. **生产部署**: 确认稳定后部署

---

## 五、配置分类速查表

| 类别 | 配置数量 | 关键配置 |
|------|---------|---------|
| AQE 自适应执行 | ~15 | `adaptive.enabled`, `skewJoin.enabled` |
| 数据源优化 | ~30 | Parquet/ORC/CSV/JSON 相关 |
| JOIN 优化 | ~5 | `autoBroadcastJoinThreshold` |
| 流处理 | ~20 | `streaming.checkpointLocation` |
| Python/Pandas | ~10 | `arrow.pyspark.enabled` |
| ANSI 兼容 | ~5 | `ansi.enabled`, `storeAssignmentPolicy` |
| 性能调优 | ~15 | 缓存、并行度、排序相关 |
| 元数据统计 | ~5 | `cbo.enabled`, `statistics.*` |

---

## 六、总结

Spark SQL 的运行时配置涵盖了从查询优化到数据源处理、从批处理到流处理的各个方面。核心配置策略：

1. **自适应执行优先**: 充分利用 AQE 的自动优化能力
2. **数据源优化**: 启用向量化读取和谓词下推
3. **按需调优**: 根据实际场景调整特定配置
4. **监控验证**: 持续监控配置效果，迭代优化

合理配置这些参数可以显著提升 Spark SQL 的查询性能和资源利用率。

