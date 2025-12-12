讲解 `Analyzer.scala` 的核心内容。这是 Spark SQL 查询分析器的核心实现。


[1 tool called]

## Analyzer.scala 代码讲解

### 一、整体架构

`Analyzer` 是 Spark SQL 的查询分析器，负责将未解析的逻辑计划转换为完全解析的逻辑计划。

核心职责：
- 解析未解析的属性引用（`UnresolvedAttribute`）
- 解析未解析的关系（`UnresolvedRelation`）
- 类型检查和转换
- 函数解析
- 子查询处理

### 二、核心组件

#### 1. AnalysisContext（分析上下文）
```scala
case class AnalysisContext(
    catalogAndNamespace: Seq[String] = Nil,
    nestedViewDepth: Int = 0,
    relationCache: mutable.Map[...] = mutable.Map.empty,
    outerPlan: Option[LogicalPlan] = None,
    ...
)
```
- 线程本地状态，保存分析过程中的上下文
- 管理视图解析的嵌套深度
- 缓存已解析的关系
- 支持子查询中的外部引用解析

#### 2. Analyzer 主类
```scala
class Analyzer(
    catalogManager: CatalogManager,
    sharedRelationCache: RelationCache)
  extends RuleExecutor[LogicalPlan]
```
- 继承 `RuleExecutor`，使用规则引擎执行分析
- 使用固定点迭代（FixedPoint）直到计划不再变化

### 三、分析规则批次（Batches）

规则按批次执行，顺序如下：

#### 早期批次（Early Batches）
```scala
Batch("Substitution", fixedPoint,
  OptimizeUpdateFields,
  CTESubstitution,      // CTE 替换
  WindowsSubstitution,   // 窗口定义替换
  EliminateUnions,       // 消除单子 Union
  EliminateLazyExpression
)
```

#### 主要解析批次（Resolution Batch）
```scala
Batch("Resolution", fixedPoint,
  new ResolveCatalogs(catalogManager),    // 解析目录
  ResolveRelations,                       // 解析表和视图
  ResolveReferences,                      // 解析列引用
  ResolveFunctions,                       // 解析函数
  ResolveSubquery,                        // 解析子查询
  ResolveAliases,                         // 解析别名
  ...
)
```

### 四、关键规则详解

#### 1. ResolveRelations（解析关系）
```scala
object ResolveRelations extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    case u: UnresolvedRelation =>
      resolveRelation(u).map(resolveViews(_, u.options)).getOrElse(u)
  }
}
```
- 将 `UnresolvedRelation` 转换为具体的表或视图
- 处理临时视图、持久化视图和 V2 表

#### 2. ResolveReferences（解析引用）
```scala
class ResolveReferences extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    // 1. 展开星号 (*)
    case p: Project if containsStar(p.projectList) =>
      expandStar(p)
    
    // 2. 解析列引用
    case q: LogicalPlan =>
      q.mapExpressions(resolveExpressionByPlanChildren(_, q))
  }
}
```
- 展开 `*` 表达式
- 解析列名到属性引用
- 处理外部引用（子查询）
- 处理元数据列

#### 3. ResolveFunctions（解析函数）
```scala
object ResolveFunctions extends Rule[LogicalPlan] {
  case u @ UnresolvedFunction(nameParts, arguments, ...) =>
    functionResolution.resolveFunction(u)
}
```
- 解析内置函数、临时函数、持久化函数
- 处理表值函数（TVF）

#### 4. ResolveSubquery（解析子查询）
```scala
object ResolveSubquery extends Rule[LogicalPlan] {
  private def resolveSubQuery(e: SubqueryExpression, outer: LogicalPlan) = {
    AnalysisContext.withOuterPlan(outer) {
      executeSameContext(e.plan)  // 递归分析子查询
    }
  }
}
```
- 递归分析子查询
- 使用 `AnalysisContext.outerPlan` 解析外部引用

#### 5. ResolveAggregateFunctions（解析聚合函数）
```scala
object ResolveAggregateFunctions extends Rule[LogicalPlan] {
  case Filter(cond, agg: Aggregate) =>
    // 将 HAVING 中的聚合函数下推到 Aggregate
    resolveOperatorWithAggregate(Seq(cond), agg, ...)
}
```
- 将 HAVING/ORDER BY 中的聚合函数下推到 Aggregate
- 处理分组表达式

### 五、特殊处理规则

#### 1. ExtractGenerator（提取生成器）
- 将 `explode`、`json_tuple` 等生成器函数提取为 `Generate` 操作符

#### 2. ExtractWindowExpressions（提取窗口表达式）
- 将窗口函数提取为独立的 `Window` 操作符
- 处理多个窗口规范

#### 3. ResolvePivot/ResolveUnpivot
- 处理 PIVOT/UNPIVOT 操作

#### 4. ResolveGroupingAnalytics
- 处理 GROUPING SETS、CUBE、ROLLUP
- 生成 `Expand` 操作符

### 六、分析流程

```
用户 SQL
  ↓
Parser (生成 Unresolved LogicalPlan)
  ↓
Analyzer.execute()
  ↓
早期批次 (CTE替换、窗口替换等)
  ↓
解析批次 (关系、引用、函数等)
  ↓
后处理批次 (清理别名等)
  ↓
Resolved LogicalPlan
```

### 七、设计模式

1. 规则引擎模式：使用 `RuleExecutor` 执行规则
2. 固定点迭代：规则重复执行直到计划不再变化
3. 上下文传递：通过 `AnalysisContext` 传递分析状态
4. 递归分析：子查询通过递归调用分析器处理

### 八、关键特性

1. 惰性解析：只在需要时解析表和列
2. 缓存机制：`relationCache` 避免重复解析
3. 错误处理：提供清晰的错误信息
4. 扩展性：支持自定义规则扩展

### 总结

`Analyzer` 是 Spark SQL 查询分析的核心，通过规则引擎将未解析的逻辑计划转换为完全解析的计划。它处理表、列、函数、子查询等的解析，为后续优化和执行做准备。

如需深入某个具体规则或流程，可继续提问。