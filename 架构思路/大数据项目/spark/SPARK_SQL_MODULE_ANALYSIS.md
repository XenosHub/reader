# Spark SQL 模块结构与执行流程分析

## 一、SQL 模块子模块概览

根据代码结构，Spark SQL 主要包含以下子模块：

### 1. **sql/api** - 公共 API 模块
**作用**: 提供 Spark SQL 的公共 API 接口

**核心组件**:
- `SparkSession` - Spark SQL 的入口点
- `Dataset` / `DataFrame` - 核心数据结构
- `Row` - 行数据表示
- `DataType` - 数据类型定义
- `Encoder` - 编码器接口

**特点**:
- 可以被 Catalyst 和 Spark Connect 客户端共享
- 定义了用户可见的公共接口
- 不包含具体实现逻辑

---

### 2. **sql/catalyst** - Catalyst 优化器框架
**作用**: 实现无关的关系操作符和表达式树操作框架

**核心组件**:

#### 2.1 解析器 (Parser)
- `ParserInterface` - SQL 解析接口
- `AstBuilder` - AST 构建器
- 将 SQL 文本解析为 `UnresolvedLogicalPlan`

#### 2.2 分析器 (Analyzer)
- `Analyzer` - 逻辑计划分析器
- `Resolver` - 解析器集合
  - `ResolveRelations` - 解析表和视图
  - `ResolveReferences` - 解析列引用
  - `ResolveFunctions` - 解析函数
  - `ResolveSubquery` - 解析子查询
- 将 `UnresolvedLogicalPlan` 转换为 `ResolvedLogicalPlan`

#### 2.3 优化器 (Optimizer)
- `Optimizer` - 逻辑计划优化器基类
- `SparkOptimizer` - Spark 特定优化器
- 优化规则批次 (Batches):
  - `FinishAnalysis` - 完成分析
  - `EliminateSubqueries` - 消除子查询
  - `ReplaceOperators` - 替换操作符
  - `PushDownPredicate` - 谓词下推
  - `ColumnPruning` - 列裁剪
  - `CollapseProject` - 合并投影
  - `ConstantFolding` - 常量折叠
  - 等等...

#### 2.4 表达式系统
- `Expression` - 表达式基类
- `UnaryExpression` / `BinaryExpression` - 一元/二元表达式
- `AggregateExpression` - 聚合表达式
- `SubqueryExpression` - 子查询表达式

#### 2.5 逻辑计划
- `LogicalPlan` - 逻辑计划基类
- `UnaryNode` / `BinaryNode` / `LeafNode` - 节点类型
- `Project`, `Filter`, `Join`, `Aggregate` 等操作符

#### 2.6 规则执行器
- `RuleExecutor` - 规则执行器基类
- `Rule` - 规则接口
- `Batch` - 规则批次

**特点**:
- 实现无关，不依赖 Spark 运行时
- 基于规则和成本的优化
- 支持自定义规则扩展

---

### 3. **sql/core** - 执行引擎
**作用**: 将 Catalyst 的逻辑计划转换为 Spark RDD 执行

**核心组件**:

#### 3.1 查询执行
- `QueryExecution` - 查询执行的核心类
- `SparkSession` - Spark SQL 会话（经典实现）
- `Dataset` - 数据集实现

#### 3.2 物理计划
- `SparkPlan` - 物理计划基类
- `UnaryExecNode` / `BinaryExecNode` / `LeafExecNode` - 执行节点
- `ProjectExec`, `FilterExec`, `HashJoinExec` 等执行操作符

#### 3.3 计划器
- `SparkPlanner` - Spark 物理计划器
- `Strategy` - 策略接口
- `SparkStrategies` - Spark 策略集合
  - `FileSourceStrategy` - 文件源策略
  - `DataSourceStrategy` - 数据源策略
  - `JoinSelection` - JOIN 选择策略
  - `Aggregation` - 聚合策略
  - 等等...

#### 3.4 数据源
- `DataSource` - 数据源接口
- `FileFormat` - 文件格式接口
- `HadoopFsRelation` - Hadoop 文件系统关系
- 支持 Parquet、ORC、JSON、CSV 等格式

#### 3.5 自适应执行
- `AdaptiveSparkPlanExec` - 自适应执行计划
- `AdaptiveExecutionContext` - 自适应执行上下文
- `InsertAdaptiveSparkPlan` - 插入自适应计划规则

#### 3.6 执行准备
- `EnsureRequirements` - 确保执行要求
- `ReuseExchangeAndSubquery` - 重用 Exchange 和子查询
- `PlanDynamicPruningFilters` - 动态分区裁剪

**特点**:
- 将逻辑计划转换为可执行的物理计划
- 集成 Spark RDD 执行引擎
- 支持代码生成优化

---

### 4. **sql/hive** - Hive 支持
**作用**: 提供 Hive 兼容性和元存储集成

**核心组件**:
- `HiveSessionState` - Hive 会话状态
- `HiveMetastoreCatalog` - Hive 元存储目录
- `HiveSerDe` - Hive 序列化/反序列化
- Hive UDF/UDAF/UDTF 支持

**特点**:
- 兼容 HiveQL 语法
- 支持 Hive 元存储
- 支持 Hive SerDe

---

### 5. **sql/hive-thriftserver** - Thrift Server
**作用**: 提供 JDBC/ODBC 接口和 SQL CLI

**核心组件**:
- `SparkSQLCLIDriver` - SQL CLI 驱动
- `SparkSQLSessionManager` - 会话管理器
- `SparkExecuteStatementOperation` - 语句执行操作

**特点**:
- 兼容 HiveServer2 协议
- 支持 JDBC/ODBC 连接
- 提供 SQL 命令行工具

---

### 6. **sql/connect** - Spark Connect
**作用**: 提供远程 Spark 连接能力（Spark 3.4+）

**核心组件**:
- `client` - 客户端实现
- `server` - 服务器实现
- `common` - 共享协议定义

**特点**:
- 支持远程 Spark 连接
- 基于 gRPC 协议
- 分离客户端和服务器

---

### 7. **sql/pipelines** - 管道支持
**作用**: 提供数据管道功能

**特点**:
- 支持复杂的数据处理管道
- 集成流处理和批处理

---

## 二、SQL 执行完整流程

### 阶段 1: SQL 文本解析 (Parsing)

**入口**: `SparkSession.sql(sqlText: String)`

**流程**:
```
SQL 文本
  ↓
SqlParser.parsePlan(sqlText)
  ↓
AST (抽象语法树)
  ↓
UnresolvedLogicalPlan
```

**核心代码路径**:
```scala
// sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala
def sql(sqlText: String): DataFrame = {
  val plan = sessionState.sqlParser.parsePlan(sqlText)
  Dataset.ofRows(self, plan)
}
```

**关键类**:
- `ParserInterface` - 解析器接口
- `AstBuilder` - AST 构建器
- `LogicalPlan` - 逻辑计划

**示例**:
```sql
SELECT name, age FROM people WHERE age > 18
```
解析后生成:
```scala
Project(
  Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age")),
  Filter(
    GreaterThan(UnresolvedAttribute("age"), Literal(18)),
    UnresolvedRelation("people")
  )
)
```

---

### 阶段 2: 逻辑计划分析 (Analysis)

**入口**: `QueryExecution.analyzed`

**流程**:
```
UnresolvedLogicalPlan
  ↓
Analyzer.execute(plan)
  ↓
应用分析规则 (ResolveRelations, ResolveReferences, ...)
  ↓
ResolvedLogicalPlan
```

**核心代码路径**:
```scala
// sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala
private val lazyAnalyzed = LazyTry {
  sparkSession.sessionState.analyzer.executeAndCheck(sqlScriptExecuted, tracker)
}
```

**关键分析规则**:

1. **ResolveRelations** - 解析表和视图
   - 查找表定义（临时视图、持久化表）
   - 解析视图定义
   - 处理数据源关系

2. **ResolveReferences** - 解析列引用
   - 展开 `*` 通配符
   - 解析列名到属性引用
   - 处理别名和限定名

3. **ResolveFunctions** - 解析函数
   - 查找内置函数
   - 解析用户定义函数 (UDF)
   - 验证函数参数

4. **ResolveSubquery** - 解析子查询
   - 关联子查询解析
   - 非关联子查询解析
   - 子查询去关联化

**示例转换**:
```scala
// 分析前
Project(
  Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age")),
  Filter(
    GreaterThan(UnresolvedAttribute("age"), Literal(18)),
    UnresolvedRelation("people")
  )
)

// 分析后
Project(
  Seq(AttributeReference("name", StringType, ...), 
      AttributeReference("age", LongType, ...)),
  Filter(
    GreaterThan(AttributeReference("age", LongType, ...), Literal(18)),
    LogicalRelation(HadoopFsRelation(...), ...)
  )
)
```

---

### 阶段 3: 逻辑计划优化 (Optimization)

**入口**: `QueryExecution.optimizedPlan`

**流程**:
```
ResolvedLogicalPlan
  ↓
Optimizer.execute(plan)
  ↓
应用优化规则批次 (Batches)
  ↓
OptimizedLogicalPlan
```

**核心代码路径**:
```scala
// sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala
private val lazyOptimizedPlan = LazyTry {
  sparkSession.sessionState.optimizer.executeAndTrack(withCachedData.clone(), tracker)
}
```

**关键优化规则批次**:

#### Batch 1: FinishAnalysis
- `EliminateSubqueryAliases` - 消除子查询别名
- `EliminateView` - 消除视图
- `ComputeCurrentTime` - 计算当前时间
- `EvalInlineTables` - 评估内联表

#### Batch 2: EliminateSubqueries
- `EliminateCorrelatedExistsSubquery` - 消除关联 EXISTS 子查询
- `EliminateCorrelatedScalarSubquery` - 消除关联标量子查询

#### Batch 3: ReplaceOperators
- `ReplaceIntersectWithSemiJoin` - 用半连接替换 INTERSECT
- `ReplaceExceptWithAntiJoin` - 用反连接替换 EXCEPT
- `ReplaceDistinctWithAggregate` - 用聚合替换 DISTINCT

#### Batch 4: PushDownPredicate
- `PushPredicateThroughJoin` - 谓词下推通过 JOIN
- `PushPredicateThroughProject` - 谓词下推通过投影
- `PushPredicateThroughGenerate` - 谓词下推通过生成

#### Batch 5: ColumnPruning
- 移除未使用的列
- 减少数据传输量

#### Batch 6: CollapseProject
- 合并相邻的 Project 节点
- 减少中间结果

#### Batch 7: ConstantFolding
- 常量折叠
- 表达式简化

#### Batch 8: InferFilters
- 从约束推断过滤器
- 自动添加过滤条件

**示例优化**:
```scala
// 优化前
Project(
  Seq(AttributeReference("name", StringType, ...)),
  Filter(
    GreaterThan(AttributeReference("age", LongType, ...), Literal(18)),
    Project(
      Seq(AttributeReference("name", StringType, ...),
          AttributeReference("age", LongType, ...),
          AttributeReference("city", StringType, ...)),
      LogicalRelation(...)
    )
  )
)

// 优化后 (列裁剪 + 谓词下推)
Project(
  Seq(AttributeReference("name", StringType, ...)),
  Filter(
    GreaterThan(AttributeReference("age", LongType, ...), Literal(18)),
    Project(
      Seq(AttributeReference("name", StringType, ...),
          AttributeReference("age", LongType, ...)),
      LogicalRelation(...)
    )
  )
)
```

---

### 阶段 4: 物理计划生成 (Physical Planning)

**入口**: `QueryExecution.sparkPlan`

**流程**:
```
OptimizedLogicalPlan
  ↓
SparkPlanner.plan(plan)
  ↓
应用策略 (Strategies)
  ↓
SparkPlan (物理计划)
```

**核心代码路径**:
```scala
// sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala
private val lazySparkPlan = LazyTry {
  QueryExecution.createSparkPlan(planner, optimizedPlan.clone())
}
```

**关键策略**:

1. **FileSourceStrategy** - 文件源策略
   - 生成 `FileSourceScanExec`
   - 支持分区裁剪
   - 支持谓词下推

2. **DataSourceStrategy** - 数据源策略
   - 生成 `RowDataSourceScanExec`
   - 支持各种数据源

3. **JoinSelection** - JOIN 选择策略
   - `BroadcastHashJoinExec` - 广播哈希连接
   - `ShuffledHashJoinExec` - 混洗哈希连接
   - `SortMergeJoinExec` - 排序合并连接
   - `BroadcastNestedLoopJoinExec` - 广播嵌套循环连接

4. **Aggregation** - 聚合策略
   - `HashAggregateExec` - 哈希聚合
   - `SortAggregateExec` - 排序聚合
   - `ObjectHashAggregateExec` - 对象哈希聚合

5. **Window** - 窗口函数策略
   - `WindowExec` - 窗口执行

6. **SpecialLimits** - 特殊限制策略
   - `CollectLimitExec` - 收集限制
   - `TakeOrderedAndProjectExec` - 取有序并投影

**示例转换**:
```scala
// 逻辑计划
Project(
  Seq(AttributeReference("name", StringType, ...)),
  Filter(
    GreaterThan(AttributeReference("age", LongType, ...), Literal(18)),
    LogicalRelation(...)
  )
)

// 物理计划
ProjectExec(
  Seq(AttributeReference("name", StringType, ...)),
  FilterExec(
    GreaterThan(AttributeReference("age", LongType, ...), Literal(18)),
    FileSourceScanExec(
      relation = ...,
      output = Seq(...),
      requiredSchema = ...,
      partitionFilters = ...,
      dataFilters = ...
    )
  )
)
```

---

### 阶段 5: 执行准备 (Preparation)

**入口**: `QueryExecution.executedPlan`

**流程**:
```
SparkPlan
  ↓
应用准备规则 (Preparations)
  ↓
ExecutedSparkPlan
```

**核心代码路径**:
```scala
// sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala
private val lazyExecutedPlan = LazyTry {
  QueryExecution.prepareForExecution(preparations, sparkPlan.clone())
}
```

**关键准备规则**:

1. **PlanSubqueries** - 规划子查询
   - 为子查询生成独立的执行计划

2. **EnsureRequirements** - 确保执行要求
   - 添加必要的 Shuffle
   - 确保分区要求
   - 添加 Exchange 节点

3. **ReuseExchangeAndSubquery** - 重用 Exchange 和子查询
   - 识别可重用的 Exchange
   - 识别可重用的子查询

4. **CollapseCodegenStages** - 合并代码生成阶段
   - 合并 WholeStageCodegen

5. **InsertAdaptiveSparkPlan** - 插入自适应计划（如果启用 AQE）
   - 包装为 `AdaptiveSparkPlanExec`

**示例转换**:
```scala
// 准备前
ProjectExec(...)
  FilterExec(...)
    FileSourceScanExec(...)

// 准备后 (添加 Exchange 和 WholeStageCodegen)
WholeStageCodegenExec(
  ProjectExec(...)
    FilterExec(...)
      Exchange(...)
        FileSourceScanExec(...)
)
```

---

### 阶段 6: RDD 执行 (Execution)

**入口**: `QueryExecution.toRdd` 或 `Dataset.collect()`

**流程**:
```
ExecutedSparkPlan
  ↓
sparkPlan.execute()
  ↓
RDD[InternalRow]
  ↓
转换为 RDD[Row]
  ↓
执行 Spark Job
```

**核心代码路径**:
```scala
// sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala
val lazyToRdd = LazyTry {
  new SQLExecutionRDD(executedPlan.execute(), sparkSession.sessionState.conf)
}
```

**执行过程**:

1. **代码生成** (如果启用)
   - 生成 Java 字节码
   - 编译为可执行代码
   - 提高执行效率

2. **任务调度**
   - 将物理计划转换为任务
   - 调度到 Executor
   - 执行计算

3. **数据流转**
   - 从数据源读取数据
   - 应用过滤和投影
   - 执行 JOIN 和聚合
   - 返回结果

**示例执行**:
```scala
// 物理计划
WholeStageCodegenExec(
  ProjectExec(...)
    FilterExec(...)
      FileSourceScanExec(...)
)

// 执行
val rdd = executedPlan.execute()
// 生成 RDD[InternalRow]
// 调度任务到 Executor
// 执行计算
// 返回结果
```

---

## 三、核心代码交互流程

### 3.1 完整执行链路图

```
┌─────────────────────────────────────────────────────────────┐
│                    SparkSession.sql()                        │
│              (sql/core/.../SparkSession.scala)                │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              SqlParser.parsePlan(sqlText)                    │
│              (catalyst/.../ParserInterface.scala)             │
│  - 解析 SQL 文本                                              │
│  - 生成 UnresolvedLogicalPlan                                │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              Dataset.ofRows(session, plan)                   │
│              (sql/core/.../Dataset.scala)                    │
│  - 创建 Dataset                                               │
│  - 创建 QueryExecution                                       │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│         QueryExecution.analyzed (lazy evaluation)            │
│         (sql/core/.../QueryExecution.scala)                 │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Analyzer.execute(plan)                                │  │
│  │ (catalyst/.../Analyzer.scala)                         │  │
│  │  - ResolveRelations: 解析表和视图                     │  │
│  │  - ResolveReferences: 解析列引用                      │  │
│  │  - ResolveFunctions: 解析函数                         │  │
│  │  - ResolveSubquery: 解析子查询                        │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│        QueryExecution.optimizedPlan (lazy evaluation)       │
│        (sql/core/.../QueryExecution.scala)                  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Optimizer.execute(plan)                               │  │
│  │ (catalyst/.../Optimizer.scala)                       │  │
│  │  - FinishAnalysis: 完成分析                           │  │
│  │  - EliminateSubqueries: 消除子查询                   │  │
│  │  - PushDownPredicate: 谓词下推                       │  │
│  │  - ColumnPruning: 列裁剪                             │  │
│  │  - CollapseProject: 合并投影                         │  │
│  │  - ConstantFolding: 常量折叠                         │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│          QueryExecution.sparkPlan (lazy evaluation)          │
│          (sql/core/.../QueryExecution.scala)                 │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ SparkPlanner.plan(plan)                               │  │
│  │ (sql/core/.../SparkPlanner.scala)                     │  │
│  │  - FileSourceStrategy: 文件源策略                   │  │
│  │  - DataSourceStrategy: 数据源策略                    │  │
│  │  - JoinSelection: JOIN 选择策略                      │  │
│  │  - Aggregation: 聚合策略                             │  │
│  │  - Window: 窗口函数策略                              │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│        QueryExecution.executedPlan (lazy evaluation)        │
│        (sql/core/.../QueryExecution.scala)                  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ QueryExecution.prepareForExecution()                │  │
│  │  - PlanSubqueries: 规划子查询                        │  │
│  │  - EnsureRequirements: 确保执行要求                 │  │
│  │  - ReuseExchangeAndSubquery: 重用 Exchange          │  │
│  │  - CollapseCodegenStages: 合并代码生成阶段          │  │
│  │  - InsertAdaptiveSparkPlan: 插入自适应计划 (AQE)    │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│            QueryExecution.toRdd (lazy evaluation)            │
│            (sql/core/.../QueryExecution.scala)                │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ executedPlan.execute()                               │  │
│  │ (sql/core/.../SparkPlan.scala)                       │  │
│  │  - 代码生成 (WholeStageCodegen)                      │  │
│  │  - 任务调度                                           │  │
│  │  - 数据执行                                           │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    RDD[InternalRow]                          │
│                    转换为 RDD[Row]                           │
│                    执行 Spark Job                            │
└─────────────────────────────────────────────────────────────┘
```

---

### 3.2 关键类交互关系

#### QueryExecution 类结构
```scala
class QueryExecution(
    val sparkSession: SparkSession,
    val logical: LogicalPlan,
    val tracker: QueryPlanningTracker = new QueryPlanningTracker) {
  
  // 阶段 1: 分析
  lazy val analyzed: LogicalPlan = 
    sparkSession.sessionState.analyzer.executeAndCheck(logical, tracker)
  
  // 阶段 2: 优化
  lazy val optimizedPlan: LogicalPlan = 
    sparkSession.sessionState.optimizer.executeAndTrack(withCachedData, tracker)
  
  // 阶段 3: 物理计划
  lazy val sparkPlan: SparkPlan = 
    QueryExecution.createSparkPlan(planner, optimizedPlan)
  
  // 阶段 4: 执行准备
  lazy val executedPlan: SparkPlan = 
    QueryExecution.prepareForExecution(preparations, sparkPlan)
  
  // 阶段 5: RDD 执行
  lazy val toRdd: RDD[InternalRow] = 
    new SQLExecutionRDD(executedPlan.execute(), conf)
}
```

#### SessionState 组件
```scala
class SessionState(
    sharedState: SharedState,
    conf: SQLConf,
    experimentalMethods: ExperimentalMethods) {
  
  // 解析器
  lazy val sqlParser: ParserInterface = ...
  
  // 分析器
  lazy val analyzer: Analyzer = ...
  
  // 优化器
  lazy val optimizer: Optimizer = ...
  
  // 计划器
  lazy val planner: SparkPlanner = ...
  
  // 其他组件...
}
```

---

### 3.3 规则执行机制

#### RuleExecutor 执行流程
```scala
// sql/catalyst/.../RuleExecutor.scala
def execute(plan: TreeType): TreeType = {
  var curPlan = plan
  batches.foreach { batch =>
    var iteration = 1
    var lastPlan = curPlan
    var continue = true
    
    // 固定点迭代
    while (continue) {
      curPlan = batch.rules.foldLeft(curPlan) { (plan, rule) =>
        rule(plan)  // 应用规则
      }
      
      // 检查是否达到固定点
      if (curPlan.fastEquals(lastPlan) || iteration >= batch.strategy.maxIterations) {
        continue = false
      } else {
        lastPlan = curPlan
        iteration += 1
      }
    }
  }
  curPlan
}
```

#### 规则应用示例
```scala
// 优化规则: 列裁剪
object ColumnPruning extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformWithPruning {
      case p @ Project(_, child) if !child.outputSet.subsetOf(p.references) =>
        // 只保留需要的列
        p.copy(child = prunedChild(child, p.references))
    }
  }
}
```

---

## 四、自适应查询执行 (AQE)

### 4.1 AQE 工作流程

```
┌─────────────────────────────────────────────────────────────┐
│               AdaptiveSparkPlanExec                          │
│  (sql/core/.../adaptive/AdaptiveSparkPlanExec.scala)         │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│           初始执行计划 (Initial SparkPlan)                    │
│           执行部分任务，收集统计信息                          │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│           自适应优化 (Adaptive Optimization)                │
│  - 合并小分区 (Coalesce Partitions)                          │
│  - 处理数据倾斜 (Skew Join)                                  │
│  - 动态分区裁剪 (Dynamic Partition Pruning)                  │
│  - 优化 JOIN 策略                                           │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│           最终执行计划 (Final SparkPlan)                     │
│           继续执行剩余任务                                    │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 AQE 关键组件

1. **AdaptiveSparkPlanExec** - 自适应计划执行器
   - 包装原始 SparkPlan
   - 管理执行阶段

2. **AdaptiveExecutionContext** - 自适应执行上下文
   - 收集运行时统计信息
   - 执行自适应优化

3. **QueryStageExec** - 查询阶段执行器
   - 表示可优化的执行阶段
   - 支持重新优化

---

## 五、代码生成优化

### 5.1 WholeStageCodegen

**目的**: 将多个操作符合并为单个 Java 方法，减少虚函数调用开销

**流程**:
```
ProjectExec
  FilterExec
    FileSourceScanExec
      ↓
WholeStageCodegenExec(
  // 生成的 Java 代码
  public Object generate(Object[] references) {
    // 合并的代码
    while (scan.hasNext()) {
      row = scan.next();
      if (filter.eval(row)) {
        result = project.eval(row);
        output.add(result);
      }
    }
    return output;
  }
)
```

**优势**:
- 减少虚函数调用
- 更好的 JIT 优化
- 减少内存分配

---

## 六、总结

### 6.1 模块职责总结

| 模块 | 主要职责 | 关键组件 |
|------|---------|---------|
| **api** | 公共 API 定义 | SparkSession, Dataset, DataFrame |
| **catalyst** | 查询优化框架 | Parser, Analyzer, Optimizer |
| **core** | 执行引擎 | QueryExecution, SparkPlanner, SparkPlan |
| **hive** | Hive 兼容性 | HiveSessionState, HiveMetastoreCatalog |
| **hive-thriftserver** | JDBC/ODBC 接口 | SparkSQLCLIDriver, ThriftServer |
| **connect** | 远程连接 | Client, Server, Protocol |
| **pipelines** | 数据管道 | Pipeline API |

### 6.2 执行流程总结

1. **解析阶段**: SQL → UnresolvedLogicalPlan
2. **分析阶段**: UnresolvedLogicalPlan → ResolvedLogicalPlan
3. **优化阶段**: ResolvedLogicalPlan → OptimizedLogicalPlan
4. **计划阶段**: OptimizedLogicalPlan → SparkPlan
5. **准备阶段**: SparkPlan → ExecutedSparkPlan
6. **执行阶段**: ExecutedSparkPlan → RDD[Row]

### 6.3 关键设计模式

1. **Lazy Evaluation**: 所有阶段都是懒加载，按需计算
2. **Rule-based Optimization**: 基于规则的优化，易于扩展
3. **Tree Transformation**: 树形结构转换，支持模式匹配
4. **Strategy Pattern**: 策略模式选择执行策略
5. **Code Generation**: 代码生成提高执行效率

### 6.4 扩展点

1. **自定义解析器**: 实现 `ParserInterface`
2. **自定义规则**: 实现 `Rule[LogicalPlan]`
3. **自定义策略**: 实现 `Strategy`
4. **自定义数据源**: 实现 `DataSource` 或 `FileFormat`
5. **自定义函数**: 注册 UDF/UDAF/UDTF

---

## 七、参考代码位置

### 核心类文件路径

- **QueryExecution**: `sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala`
- **SparkSession**: `sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala`
- **Analyzer**: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/Analyzer.scala`
- **Optimizer**: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala`
- **SparkPlanner**: `sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlanner.scala`
- **SparkPlan**: `sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlan.scala`
- **AdaptiveSparkPlanExec**: `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveSparkPlanExec.scala`

---

本文档详细分析了 Spark SQL 模块的结构和执行流程，为深入理解 Spark SQL 的工作原理提供了全面的参考。

