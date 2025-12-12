# Spark SQL 静态配置分析

## 一、功能分类

### 1. Hive 元存储集成配置

#### 核心配置
- `spark.sql.hive.metastore.version` (默认: 2.3.10) - Hive 元存储版本
- `spark.sql.hive.metastore.jars` (默认: builtin) - Hive 元存储 JAR 位置
- `spark.sql.hive.metastore.jars.path` - Hive 元存储 JAR 路径
- `spark.sql.hive.metastore.sharedPrefixes` - 共享类前缀（JDBC 驱动等）
- `spark.sql.hive.metastore.barrierPrefixes` - 隔离类前缀（Hive UDF 等）
- `spark.sql.hive.version` (默认: 2.3.10) - 内置 Hive 版本（只读）

#### 功能说明
这些配置用于管理 Spark SQL 与 Hive 元存储的集成，确保类加载器正确隔离和共享。

**共享前缀 (sharedPrefixes):**
- 用于共享 JDBC 驱动、日志组件等
- 避免类加载冲突
- 默认包含: `com.mysql.jdbc`, `org.postgresql`, `com.microsoft.sqlserver`, `oracle.jdbc`

**隔离前缀 (barrierPrefixes):**
- 为每个 Hive 版本单独加载
- 防止版本冲突
- 通常用于 Hive UDF、自定义函数等

---

### 2. 扩展和插件配置

#### 核心配置
- `spark.sql.extensions` - Spark Session 扩展类列表
- `spark.sql.extensions.test.loadFromCp` (默认: true) - 测试标志，从类路径加载扩展

#### 功能说明
允许通过实现 `Function1[SparkSessionExtensions, Unit]` 接口来扩展 Spark Session 功能。

**扩展类型:**
- **解析器扩展**: 自定义 SQL 语法解析
- **规则扩展**: 自定义优化规则
- **策略扩展**: 自定义物理计划策略
- **函数扩展**: 注册自定义函数

---

### 3. 缓存和序列化配置

#### 核心配置
- `spark.sql.cache.serializer` (默认: `org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer`) - 缓存序列化器
- `spark.sql.metadataCacheTTLSeconds` (默认: -1000ms) - 元数据缓存 TTL

#### 功能说明
控制数据缓存和元数据缓存的序列化方式和生命周期。

**缓存序列化器:**
- 将 SQL 数据转换为可高效缓存的格式
- 默认使用列式序列化器
- API 可能变更，需谨慎使用

**元数据缓存 TTL:**
- 控制分区文件元数据和会话目录缓存的过期时间
- 需要启用 Hive 元存储和分区管理
- 正数表示缓存时间，负数表示不启用

---

### 4. 监听器和监控配置

#### 核心配置
- `spark.sql.queryExecutionListeners` - 查询执行监听器列表
- `spark.sql.streaming.streamingQueryListeners` - 流查询监听器列表

#### 功能说明
允许注册自定义监听器来监控和记录查询执行过程。

**查询执行监听器:**
- 监听批处理查询的执行
- 可用于性能监控、审计、日志记录
- 实现 `QueryExecutionListener` 接口

**流查询监听器:**
- 监听流处理查询的生命周期
- 可用于监控流处理状态、异常处理
- 实现 `StreamingQueryListener` 接口

---

### 5. UI 和可视化配置

#### 核心配置
- `spark.sql.streaming.ui.enabled` (默认: true) - 启用流处理 Web UI
- `spark.sql.streaming.ui.retainedProgressUpdates` (默认: 100) - 保留的进度更新数
- `spark.sql.streaming.ui.retainedQueries` (默认: 100) - 保留的查询数
- `spark.sql.ui.retainedExecutions` (默认: 1000) - 保留的执行数
- `spark.sql.event.truncate.length` (默认: 2147483647) - SQL 事件截断长度

#### 功能说明
控制 Spark UI 中显示的信息量和保留时间。

**流处理 UI:**
- 显示流查询的实时进度
- 保留历史查询和进度更新
- 帮助调试和监控流处理应用

**事件截断:**
- 防止过长的 SQL 语句占用过多内存
- 设置为 0 时记录调用位置而非 SQL
- 用于事件日志和审计

---

### 6. 目录和仓库配置

#### 核心配置
- `spark.sql.catalog.spark_catalog.defaultDatabase` (默认: default) - 默认数据库
- `spark.sql.warehouse.dir` (默认: `$PWD/spark-warehouse`) - 数据仓库目录

#### 功能说明
定义 Spark SQL 的默认数据库和表存储位置。

**默认数据库:**
- 新会话的默认数据库
- 影响未指定数据库的查询

**数据仓库目录:**
- 托管数据库和表的默认存储位置
- 可以是本地路径或 HDFS 路径
- 影响 `CREATE DATABASE` 和 `CREATE TABLE` 的默认位置

---

### 7. Thrift Server 配置

#### 核心配置
- `spark.sql.hive.thriftServer.singleSession` (默认: false) - Thrift Server 单会话模式

#### 功能说明
控制 Hive Thrift Server 的会话模式。

**单会话模式:**
- 所有 JDBC/ODBC 连接共享同一个会话
- 共享临时视图、函数注册、SQL 配置、当前数据库
- 适用于需要共享状态的场景

**多会话模式 (默认):**
- 每个连接有独立的会话
- 更好的隔离性
- 适用于多租户场景

---

### 8. 数据源配置

#### 核心配置
- `spark.sql.sources.disabledJdbcConnProviderList` - 禁用的 JDBC 连接提供者列表

#### 功能说明
禁用特定的 JDBC 连接提供者，用于安全或兼容性控制。

---

## 二、应用场景

### 场景 1: Hive 元存储集成

#### 使用场景
- **数据仓库迁移**: 从 Hive 迁移到 Spark SQL
- **混合环境**: 同时使用 Hive 和 Spark SQL
- **元数据共享**: 共享 Hive 元存储中的表定义

#### 配置示例
```scala
// 使用自定义 Hive 版本
spark.sql.hive.metastore.version = "3.1.3"
spark.sql.hive.metastore.jars = "maven"

// 使用本地 JAR 路径
spark.sql.hive.metastore.jars = "path"
spark.sql.hive.metastore.jars.path = "file:///path/to/hive-jars/*.jar"

// 配置共享和隔离前缀
spark.sql.hive.metastore.sharedPrefixes = "com.mysql.jdbc,org.postgresql"
spark.sql.hive.metastore.barrierPrefixes = "com.custom.hive.udf"
```

#### 注意事项
- 确保 Hive 版本兼容性
- 正确配置类加载器隔离
- 避免版本冲突导致的运行时错误

---

### 场景 2: 自定义扩展开发

#### 使用场景
- **自定义 SQL 语法**: 扩展 Spark SQL 支持新的 SQL 语法
- **自定义优化规则**: 实现领域特定的优化规则
- **自定义函数**: 注册自定义 UDF/UDAF
- **安全增强**: 添加访问控制、审计等功能

#### 配置示例
```scala
// 注册自定义扩展
spark.sql.extensions = "com.company.CustomParserExtension,com.company.CustomOptimizerExtension"

// 扩展类实现示例
class CustomParserExtension extends (SparkSessionExtensions => Unit) {
  def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (session, parser) =>
      new CustomParser(parser)
    }
    extensions.injectOptimizerRule { session =>
      CustomOptimizerRule()
    }
  }
}
```

#### 开发流程
1. 实现 `Function1[SparkSessionExtensions, Unit]` 接口
2. 在 `apply` 方法中注册扩展
3. 通过 `spark.sql.extensions` 配置加载
4. 确保类在类路径中可用

---

### 场景 3: 性能监控和审计

#### 使用场景
- **查询性能分析**: 记录查询执行时间和资源使用
- **安全审计**: 记录所有 SQL 查询和用户操作
- **成本分析**: 跟踪查询成本，优化资源使用
- **异常检测**: 监控异常查询模式

#### 配置示例
```scala
// 注册查询执行监听器
spark.sql.queryExecutionListeners = "com.company.QueryAuditListener,com.company.PerformanceMonitorListener"

// 注册流查询监听器
spark.sql.streaming.streamingQueryListeners = "com.company.StreamingQueryListener"

// 配置 SQL 事件截断（防止内存溢出）
spark.sql.event.truncate.length = 10000  // 截断超过 10KB 的 SQL
```

#### 监听器实现示例
```scala
class QueryAuditListener extends QueryExecutionListener {
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    // 记录成功查询
    auditLog.info(s"Query succeeded: $funcName, duration: ${durationNs}ns")
  }
  
  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    // 记录失败查询
    auditLog.error(s"Query failed: $funcName", exception)
  }
}
```

---

### 场景 4: 多租户和资源隔离

#### 使用场景
- **Thrift Server 多租户**: 为不同用户提供隔离的会话
- **资源管理**: 通过会话隔离控制资源使用
- **安全隔离**: 防止用户之间相互影响

#### 配置示例
```scala
// 多会话模式（默认，推荐用于多租户）
spark.sql.hive.thriftServer.singleSession = false

// 单会话模式（适用于共享状态场景）
spark.sql.hive.thriftServer.singleSession = true
```

#### 使用建议
- **多租户环境**: 使用多会话模式，确保隔离
- **共享状态场景**: 使用单会话模式，共享临时视图和函数
- **性能考虑**: 单会话模式减少资源开销，但降低隔离性

---

### 场景 5: 缓存优化

#### 使用场景
- **频繁查询优化**: 缓存热点数据
- **元数据缓存**: 加速分区发现和元数据查询
- **自定义序列化**: 优化缓存格式

#### 配置示例
```scala
// 自定义缓存序列化器（高级用法）
spark.sql.cache.serializer = "com.company.CustomCachedBatchSerializer"

// 配置元数据缓存 TTL（秒）
spark.sql.metadataCacheTTLSeconds = 3600  // 1 小时

// 需要同时启用以下配置
spark.sql.catalogImplementation = "hive"
spark.sql.hive.filesourcePartitionFileCacheSize = 262144000
spark.sql.hive.manageFilesourcePartitions = true
```

#### 调优建议
- **元数据缓存 TTL**: 根据元数据变更频率调整
- **缓存序列化器**: 默认序列化器已优化，除非有特殊需求否则不建议修改
- **监控缓存效果**: 通过 UI 和指标监控缓存命中率

---

### 场景 6: 数据仓库管理

#### 使用场景
- **统一数据存储**: 集中管理 Spark 表
- **多环境部署**: 开发、测试、生产使用不同仓库目录
- **HDFS 集成**: 将数据仓库存储在 HDFS

#### 配置示例
```scala
// 本地开发环境
spark.sql.warehouse.dir = "file:///tmp/spark-warehouse"

// 生产环境（HDFS）
spark.sql.warehouse.dir = "hdfs://namenode:9000/user/spark/warehouse"

// 设置默认数据库
spark.sql.catalog.spark_catalog.defaultDatabase = "production"
```

#### 最佳实践
- **环境隔离**: 不同环境使用不同的仓库目录
- **权限控制**: 确保仓库目录有适当的权限
- **备份策略**: 定期备份重要数据仓库

---

### 场景 7: UI 和调试

#### 使用场景
- **流处理监控**: 实时监控流查询状态
- **性能分析**: 通过 UI 分析查询执行计划
- **调试支持**: 查看历史查询和执行信息

#### 配置示例
```scala
// 启用流处理 UI
spark.sql.streaming.ui.enabled = true

// 调整 UI 保留的数据量
spark.sql.streaming.ui.retainedQueries = 200
spark.sql.streaming.ui.retainedProgressUpdates = 200
spark.sql.ui.retainedExecutions = 2000

// 配置 SQL 事件截断（防止 UI 内存问题）
spark.sql.event.truncate.length = 50000
```

#### 使用建议
- **内存管理**: 根据可用内存调整保留数量
- **监控需求**: 根据监控需求调整保留时间
- **性能影响**: 保留过多数据可能影响 UI 性能

---

## 三、总结

### 1. 配置特点

#### 静态配置特性
- **跨会话**: 所有会话共享相同的配置值
- **不可变**: 创建 SparkSession 后无法修改
- **只读查询**: 可以通过 `SET` 命令查询，但不能设置
- **启动时设置**: 必须在创建 SparkSession 之前配置

#### 与运行时配置的区别

| 特性 | 静态配置 | 运行时配置 |
|------|---------|-----------|
| 可修改性 | 不可变 | 可在运行时修改 |
| 作用范围 | 跨会话 | 单会话 |
| 设置方式 | SparkConf/配置文件 | SET 命令/Session.conf |
| 典型用途 | 扩展、监听器、Hive集成 | 性能调优、查询优化 |

---

### 2. 核心配置分类总结

| 分类 | 配置数量 | 重要性 | 使用频率 |
|------|---------|--------|---------|
| Hive 元存储集成 | 6 | 高 | 中等（Hive 环境） |
| 扩展和插件 | 2 | 中 | 低（自定义开发） |
| 缓存和序列化 | 2 | 中 | 低（高级优化） |
| 监听器和监控 | 2 | 中 | 高（生产环境） |
| UI 和可视化 | 5 | 低 | 高（调试监控） |
| 目录和仓库 | 2 | 高 | 高（所有环境） |
| Thrift Server | 1 | 低 | 低（Thrift Server） |
| 数据源 | 1 | 低 | 低（特殊需求） |

---

### 3. 最佳实践建议

#### 必须配置（根据场景）
1. **数据仓库目录**: 生产环境必须明确设置
   ```scala
   spark.sql.warehouse.dir = "hdfs://namenode/path/to/warehouse"
   ```

2. **Hive 集成**: 使用 Hive 元存储时必须配置
   ```scala
   spark.sql.hive.metastore.version = "3.1.3"
   spark.sql.hive.metastore.jars = "maven"
   ```

3. **监控监听器**: 生产环境建议配置
   ```scala
   spark.sql.queryExecutionListeners = "com.company.AuditListener"
   ```

#### 可选配置（按需）
1. **自定义扩展**: 仅在需要扩展功能时配置
2. **缓存优化**: 仅在性能瓶颈时考虑
3. **UI 调优**: 根据监控需求调整

#### 注意事项
1. **版本兼容性**: Hive 版本必须兼容
2. **类路径**: 扩展和监听器类必须在类路径中
3. **性能影响**: 监听器可能影响查询性能
4. **内存管理**: UI 保留数据过多可能导致内存问题

---

### 4. 配置优先级和依赖关系

#### 配置依赖
```
Hive 元存储配置
├── spark.sql.hive.metastore.version (必需)
├── spark.sql.hive.metastore.jars (必需)
├── spark.sql.hive.metastore.jars.path (当 jars=path 时)
├── spark.sql.hive.metastore.sharedPrefixes (可选)
└── spark.sql.hive.metastore.barrierPrefixes (可选)

元数据缓存配置
├── spark.sql.metadataCacheTTLSeconds (可选)
├── spark.sql.catalogImplementation = "hive" (必需)
├── spark.sql.hive.filesourcePartitionFileCacheSize > 0 (必需)
└── spark.sql.hive.manageFilesourcePartitions = true (必需)
```

#### 配置顺序
1. **基础配置**: 数据仓库目录、默认数据库
2. **集成配置**: Hive 元存储（如需要）
3. **扩展配置**: 自定义扩展、监听器
4. **优化配置**: 缓存、UI 调优

---

### 5. 常见问题和解决方案

| 问题 | 可能原因 | 解决方案 |
|------|---------|---------|
| 类加载冲突 | Hive 版本不兼容 | 检查 `barrierPrefixes` 和 `sharedPrefixes` |
| 扩展未加载 | 类不在类路径 | 确保扩展类在类路径中 |
| 监听器不工作 | 监听器类错误 | 检查类实现和构造函数 |
| 元数据缓存无效 | 依赖配置未启用 | 检查所有必需配置 |
| UI 内存溢出 | 保留数据过多 | 减少 `retained*` 配置值 |

---

### 6. 配置检查清单

#### 开发环境
- [ ] 设置数据仓库目录（本地路径）
- [ ] 配置默认数据库（如需要）
- [ ] 启用流处理 UI（调试用）

#### 测试环境
- [ ] 设置数据仓库目录（测试路径）
- [ ] 配置查询监听器（测试监控）
- [ ] 配置 Hive 集成（如需要）

#### 生产环境
- [ ] 设置数据仓库目录（生产路径，HDFS）
- [ ] 配置 Hive 元存储（如使用）
- [ ] 配置审计监听器（安全要求）
- [ ] 配置性能监控监听器
- [ ] 调整 UI 保留配置（内存优化）
- [ ] 配置 SQL 事件截断（防止内存问题）

---

## 四、配置速查表

### 快速参考

```scala
// Hive 集成
spark.sql.hive.metastore.version = "3.1.3"
spark.sql.hive.metastore.jars = "maven"

// 扩展
spark.sql.extensions = "com.company.CustomExtension"

// 监听器
spark.sql.queryExecutionListeners = "com.company.Listener"
spark.sql.streaming.streamingQueryListeners = "com.company.StreamListener"

// 数据仓库
spark.sql.warehouse.dir = "hdfs://namenode/warehouse"
spark.sql.catalog.spark_catalog.defaultDatabase = "default"

// UI 配置
spark.sql.streaming.ui.enabled = true
spark.sql.ui.retainedExecutions = 1000
spark.sql.event.truncate.length = 10000

// 缓存
spark.sql.metadataCacheTTLSeconds = 3600
```

---

## 五、总结

Spark SQL 静态配置主要关注以下几个方面：

1. **系统集成**: Hive 元存储集成是核心功能，需要仔细配置版本和类加载
2. **可扩展性**: 通过扩展机制支持自定义功能开发
3. **可观测性**: 监听器和 UI 配置提供监控和调试能力
4. **资源管理**: 数据仓库和缓存配置影响存储和性能

**关键要点:**
- 静态配置在 SparkSession 创建时确定，无法运行时修改
- 大多数配置有合理的默认值，仅在特殊需求时修改
- Hive 集成配置需要特别注意版本兼容性
- 监听器配置对生产环境很重要，但要注意性能影响
- UI 配置需要平衡监控需求和内存使用

合理配置这些静态参数可以为 Spark SQL 应用提供稳定的基础环境。

