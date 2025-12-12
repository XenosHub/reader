这是调试器评估时触发的断点被跳过。检查 `QueryExecution.scala` 第 150 行附近的代码：


[1 tool called]


问题分析：断点位于 `QueryExecution.scala:150` 的 `analyzed` 属性上，这是一个懒加载属性（lazy val）。调试器在评估表达式时会触发该属性，可能再次触发断点，导致递归。

解决方案：


[1 tool called]


## 问题分析

断点位于第 150 行的 `analyzed` 属性上：

```scala
def analyzed: LogicalPlan = lazyAnalyzed.get  // ← 断点在这里
```

这是一个懒加载属性的 getter。当调试器评估变量或表达式时会访问该属性，触发 `lazyAnalyzed.get`，进而触发分析过程，可能再次命中断点，形成递归。

## 解决方案

### 方案 1：将断点移到实际执行位置（推荐）

将断点从第 150 行移到实际执行分析的位置：

```scala
// 第 137-139 行：实际执行分析的地方
val plan = executePhase(QueryPlanningTracker.ANALYSIS) {
  sparkSession.sessionState.analyzer.executeAndCheck(sqlScriptExecuted, tracker)
}
```

或者更深入，在 `Analyzer.executeAndCheck` 方法中设置断点。

### 方案 2：使用条件断点

在 IntelliJ IDEA 中：
1. 右键断点 → Edit Breakpoint
2. 勾选 Condition，输入：
   ```scala
   !Thread.currentThread().getStackTrace().exists(_.getClassName.contains("debugger"))
   ```
3. 或使用更简单的条件：
   ```scala
   Thread.currentThread().getName.contains("main")
   ```

### 方案 3：禁用调试器评估（推荐用于懒加载属性）

在 IntelliJ IDEA 中：
1. File → Settings → Build, Execution, Deployment → Debugger
2. 取消勾选 "Enable 'toString' object view"
3. 取消勾选 "Show debugger info tooltips"
4. 或在 Variables 面板中禁用 "Auto-evaluate expressions"

### 方案 4：使用日志替代断点

在关键位置添加日志：

```scala
private val lazyAnalyzed = LazyTry {
  logInfo("=== Starting Analysis ===")
  try {
    val plan = executePhase(QueryPlanningTracker.ANALYSIS) {
      logInfo("=== Calling Analyzer.executeAndCheck ===")
      sparkSession.sessionState.analyzer.executeAndCheck(sqlScriptExecuted, tracker)
    }
    logInfo(s"=== Analysis Complete: ${plan.simpleString} ===")
    tracker.setAnalyzed(plan)
    plan
  } catch {
    case NonFatal(e) =>
      logError("=== Analysis Failed ===", e)
      tracker.setAnalysisFailed(sqlScriptExecuted)
      throw e
  }
}
```

### 方案 5：在 Analyzer 中设置断点

在 `Analyzer.scala` 的关键方法中设置断点：

```scala
// Analyzer.scala 第 314 行
def executeAndCheck(plan: LogicalPlan, tracker: QueryPlanningTracker): LogicalPlan = {
  if (plan.analyzed) {
    plan
  } else {
    // 在这里设置断点
    if (AnalysisContext.get.isDefault) {
      AnalysisContext.reset()
      try {
        AnalysisHelper.markInAnalyzer {
          HybridAnalyzer.fromLegacyAnalyzer(legacyAnalyzer = this).apply(plan, tracker)
        }
      } finally {
        AnalysisContext.reset()
      }
    }
  }
}
```

### 方案 6：使用断点过滤器

在 IntelliJ IDEA 中：
1. 右键断点 → More → Thread filters
2. 选择 "Thread name filter"，输入主线程名称

## 最佳实践建议

对于懒加载属性（lazy val），建议：

1. 不要在 getter 上设置断点
2. 在初始化代码块中设置断点（如 `lazyAnalyzed` 的初始化代码）
3. 在调用链的更深层设置断点
4. 使用条件断点避免递归触发

## 快速修复

立即修复：
1. 删除第 150 行的断点
2. 在第 139 行设置新断点：
   ```scala
   sparkSession.sessionState.analyzer.executeAndCheck(sqlScriptExecuted, tracker)
   ```
3. 或在 `Analyzer.executeAndCheck` 方法入口设置断点

这样可以避免调试器评估导致的递归问题，并更准确地捕获分析执行流程。