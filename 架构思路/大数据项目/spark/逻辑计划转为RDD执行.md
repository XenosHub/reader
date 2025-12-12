查找逻辑计划与 core 模块的交互，以及逻辑计划如何转换为 RDD 执行。


[4 tools called]


查看关键文件，梳理逻辑计划到 RDD 的转换流程：


[4 tools called]


查看关键执行代码：


[3 tools called]


查看一个具体的物理计划实现示例：


[1 tool called]


## 逻辑计划与 Core 模块交互流程

从逻辑计划到 RDD 的转换流程如下：

### 一、整体架构流程

```
SQL 查询
  ↓
Parser (生成 Unresolved LogicalPlan)
  ↓
Analyzer (生成 Resolved LogicalPlan)  ← Analyzer.scala
  ↓
Optimizer (生成 Optimized LogicalPlan) ← Optimizer
  ↓
SparkPlanner (生成 SparkPlan)         ← SparkPlanner.scala
  ↓
QueryExecution (准备执行)              ← QueryExecution.scala
  ↓
SparkPlan.execute() → RDD[InternalRow] ← SparkPlan.scala
```

### 二、核心组件交互

#### 1. QueryExecution - 执行协调器

`QueryExecution` 是连接逻辑计划和物理执行的核心类：

```scala
class QueryExecution(
    sparkSession: SparkSession,
    logical: LogicalPlan,  // 逻辑计划入口
    ...
) {
  // 1. 分析阶段
  def analyzed: LogicalPlan = {
    sparkSession.sessionState.analyzer.executeAndCheck(logical, tracker)
  }
  
  // 2. 优化阶段
  def optimizedPlan: LogicalPlan = {
    sparkSession.sessionState.optimizer.executeAndTrack(withCachedData, tracker)
  }
  
  // 3. 物理计划生成
  def sparkPlan: SparkPlan = {
    QueryExecution.createSparkPlan(planner, optimizedPlan.clone())
  }
  
  // 4. 准备执行
  def executedPlan: SparkPlan = {
    QueryExecution.prepareForExecution(preparations, sparkPlan.clone())
  }
  
  // 5. 转换为 RDD
  def toRdd: RDD[InternalRow] = {
    new SQLExecutionRDD(executedPlan.execute(), conf)
  }
}
```

#### 2. SparkPlanner - 逻辑计划到物理计划转换

`SparkPlanner` 使用策略模式将逻辑计划转换为物理计划：

```scala
class SparkPlanner extends SparkStrategies {
  override def strategies: Seq[Strategy] = Seq(
    LogicalQueryStageStrategy ::
    DataSourceV2Strategy ::
    FileSourceStrategy ::
    DataSourceStrategy ::
    JoinSelection ::
    Aggregation ::
    BasicOperators ::
    ...
  )
  
  // 调用父类的 plan 方法，遍历策略直到找到匹配的
  override def plan(plan: LogicalPlan): Iterator[SparkPlan] = {
    super.plan(plan).map { p =>
      p.setLogicalLink(logicalPlan)  // 建立逻辑-物理计划链接
      p
    }
  }
}
```

策略示例 - `BasicOperators`：
```scala
object BasicOperators extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case Project(projectList, child) =>
      ProjectExec(projectList, planLater(child)) :: Nil
    case Filter(condition, child) =>
      FilterExec(condition, planLater(child)) :: Nil
    case ...
  }
}
```

#### 3. SparkPlan - 物理计划执行

`SparkPlan` 是所有物理操作符的基类，负责生成 RDD：

```scala
abstract class SparkPlan extends QueryPlan[SparkPlan] {
  // 执行入口
  final def execute(): RDD[InternalRow] = executeQuery {
    executeRDD.get  // 调用 doExecute()
  }
  
  // 子类必须实现
  protected def doExecute(): RDD[InternalRow]
  
  // 执行前的准备
  protected final def executeQuery[T](query: => T): T = {
    RDDOperationScope.withScope(sparkContext, nodeName, false, true) {
      prepare()           // 准备子查询、广播等
      waitForSubqueries() // 等待子查询完成
      query               // 执行 doExecute()
    }
  }
}
```

### 三、具体执行示例

#### 示例1：ProjectExec（投影操作）

```scala
case class ProjectExec(
    projectList: Seq[NamedExpression],
    child: SparkPlan) extends UnaryExecNode {
  
  protected override def doExecute(): RDD[InternalRow] = {
    // 1. 执行子计划，得到 RDD
    val childRDD = child.execute()
    
    // 2. 生成投影表达式
    val projection = UnsafeProjection.create(projectList, child.output)
    
    // 3. 对每个分区应用投影
    childRDD.mapPartitionsInternal { iter =>
      projection.initialize()
      iter.map(projection)
    }
  }
}
```

#### 示例2：FilterExec（过滤操作）

```scala
case class FilterExec(
    condition: Expression,
    child: SparkPlan) extends UnaryExecNode {
  
  protected override def doExecute(): RDD[InternalRow] = {
    // 1. 执行子计划
    val childRDD = child.execute()
    
    // 2. 生成过滤谓词
    val predicate = Predicate.create(condition, child.output)
    
    // 3. 对每个分区应用过滤
    childRDD.mapPartitionsInternal { iter =>
      predicate.initialize()
      iter.filter(predicate.eval)
    }
  }
}
```

#### 示例3：FileSourceScanExec（文件扫描）

```scala
case class FileSourceScanExec(...) extends DataSourceScanExec {
  
  protected override def doExecute(): RDD[InternalRow] = {
    // 1. 创建文件分区
    val filePartitions = getFilePartitions()
    
    // 2. 为每个分区创建 RDD
    sparkContext.parallelize(filePartitions).mapPartitionsWithIndex {
      case (index, partitions) =>
        // 3. 读取文件数据
        partitions.flatMap { partition =>
          readPartition(partition)
        }
    }
  }
}
```

### 四、执行流程详解

#### 阶段1：逻辑计划分析（Analyzer）
```scala
// Analyzer.scala
analyzer.executeAndCheck(logicalPlan, tracker)
  → 解析表、列、函数等
  → 返回 Resolved LogicalPlan
```

#### 阶段2：逻辑计划优化（Optimizer）
```scala
// Optimizer
optimizer.executeAndTrack(analyzedPlan, tracker)
  → 应用优化规则（谓词下推、列裁剪等）
  → 返回 Optimized LogicalPlan
```

#### 阶段3：物理计划生成（SparkPlanner）
```scala
// SparkPlanner.scala
planner.plan(optimizedPlan)
  → 遍历策略列表
  → 匹配逻辑操作符
  → 生成对应的 SparkPlan
```

#### 阶段4：物理计划准备（QueryExecution）
```scala
// QueryExecution.scala
prepareForExecution(preparations, sparkPlan)
  → EnsureRequirements (确保分区要求)
  → CollapseCodegenStages (代码生成阶段合并)
  → ReuseExchangeAndSubquery (重用 Exchange 和子查询)
  → 返回 prepared SparkPlan
```

#### 阶段5：RDD 生成（SparkPlan.execute）
```scala
// SparkPlan.scala
executedPlan.execute()
  → prepare()              // 准备子查询、广播变量
  → waitForSubqueries()    // 等待子查询完成
  → doExecute()            // 子类实现，生成 RDD
  → 返回 RDD[InternalRow]
```

### 五、关键设计模式

1. 策略模式：`SparkPlanner` 使用策略列表匹配逻辑操作符
2. 模板方法：`SparkPlan.execute()` 定义执行框架，`doExecute()` 由子类实现
3. 懒加载：使用 `LazyTry` 延迟执行，只在需要时计算
4. 递归执行：子计划通过 `child.execute()` 递归生成 RDD

### 六、数据流示例

```scala
// SQL: SELECT name FROM users WHERE age > 18

// 逻辑计划
Project(name, Filter(age > 18, TableScan(users)))

// 物理计划
ProjectExec(name, FilterExec(age > 18, FileSourceScanExec(users)))

// RDD 执行
FileSourceScanExec.doExecute()
  → RDD[InternalRow] (从文件读取原始数据)
  ↓
FilterExec.doExecute()
  → childRDD.filter(predicate)  // 过滤 age > 18
  ↓
ProjectExec.doExecute()
  → childRDD.map(projection)    // 投影 name 列
  ↓
最终 RDD[InternalRow]
```

### 总结

- Analyzer 模块：解析逻辑计划
- Core 模块：通过 `QueryExecution` 协调整个流程
- SparkPlanner：将逻辑计划转换为物理计划
- SparkPlan：通过 `doExecute()` 生成 RDD

整个流程是递归的：每个物理操作符执行其子计划，组合成最终的 RDD DAG，然后由 Spark Core 调度执行。