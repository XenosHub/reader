# LiveListenerBus 监听事件解读

## 概述

`LiveListenerBus` 是 Spark 的**异步事件总线**，用于将 `SparkListenerEvent` 事件分发给注册的 `SparkListener` 监听器。

### 核心特性

```39:45:core/src/main/scala/org/apache/spark/scheduler/LiveListenerBus.scala
/**
 * Asynchronously passes SparkListenerEvents to registered SparkListeners.
 *
 * Until `start()` is called, all posted events are only buffered. Only after this listener bus
 * has started will events be actually propagated to all attached listeners. This listener bus
 * is stopped when `stop()` is called, and it will drop further events after stopping.
 */
private[spark] class LiveListenerBus(conf: SparkConf) {
```

**关键点：**

- **异步处理**：事件在独立线程中异步分发
- **缓冲机制**：启动前的事件会被缓冲
- **多队列支持**：不同类型的事件可以分发到不同的队列

---

## 事件队列类型

`LiveListenerBus` 支持多个事件队列，用于隔离不同类型的监听器：

```254:261:core/src/main/scala/org/apache/spark/scheduler/LiveListenerBus.scala
  private[scheduler] val SHARED_QUEUE = "shared"

  private[scheduler] val APP_STATUS_QUEUE = "appStatus"

  private[scheduler] val EXECUTOR_MANAGEMENT_QUEUE = "executorManagement"

  private[scheduler] val EVENT_LOG_QUEUE = "eventLog"
```

**队列说明：**

- **SHARED_QUEUE**：共享队列，用于一般监听器
- **APP_STATUS_QUEUE**：应用状态队列，用于状态跟踪
- **EXECUTOR_MANAGEMENT_QUEUE**：Executor 管理队列
- **EVENT_LOG_QUEUE**：事件日志队列，用于事件日志记录

---

## 事件类型分类

`LiveListenerBus` 监听所有 `SparkListenerEvent` 的子类事件。根据功能，可以分为以下几类：

### 1. 应用生命周期事件

#### SparkListenerApplicationStart

```273:280:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
case class SparkListenerApplicationStart(
    appName: String,
    appId: Option[String],
    time: Long,
    sparkUser: String,
    appAttemptId: Option[String],
    driverLogs: Option[Map[String, String]] = None,
    driverAttributes: Option[Map[String, String]] = None) extends SparkListenerEvent
```

**触发时机**：应用启动时
**包含信息**：应用名称、ID、启动时间、用户、Driver 日志等

#### SparkListenerApplicationEnd

```282:285:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
case class SparkListenerApplicationEnd(
    time: Long,
    exitCode: Option[Int] = None) extends SparkListenerEvent
```

**触发时机**：应用结束时
**包含信息**：结束时间、退出码

---

### 2. Job 生命周期事件

#### SparkListenerJobStart

```79:88:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
case class SparkListenerJobStart(
    jobId: Int,
    time: Long,
    stageInfos: Seq[StageInfo],
    properties: Properties = null)
  extends SparkListenerEvent {
  // Note: this is here for backwards-compatibility with older versions of this event which
  // only stored stageIds and not StageInfos:
  val stageIds: Seq[Int] = stageInfos.map(_.stageId)
}
```

**触发时机**：Job 开始时
**包含信息**：Job ID、开始时间、Stage 信息、属性

#### SparkListenerJobEnd

```91:95:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
case class SparkListenerJobEnd(
    jobId: Int,
    time: Long,
    jobResult: JobResult)
  extends SparkListenerEvent
```

**触发时机**：Job 结束时
**包含信息**：Job ID、结束时间、结果（成功/失败）

---

### 3. Stage 生命周期事件

#### SparkListenerStageSubmitted

```33:34:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
case class SparkListenerStageSubmitted(stageInfo: StageInfo, properties: Properties = null)
  extends SparkListenerEvent
```

**触发时机**：Stage 提交时
**包含信息**：Stage 信息、属性

#### SparkListenerStageCompleted

```37:37:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
case class SparkListenerStageCompleted(stageInfo: StageInfo) extends SparkListenerEvent
```

**触发时机**：Stage 完成时
**包含信息**：Stage 信息

---

### 4. Task 生命周期事件

#### SparkListenerTaskStart

```40:41:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
case class SparkListenerTaskStart(stageId: Int, stageAttemptId: Int, taskInfo: TaskInfo)
  extends SparkListenerEvent
```

**触发时机**：Task 开始时
**包含信息**：Stage ID、尝试 ID、Task 信息

#### SparkListenerTaskGettingResult

```44:44:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
case class SparkListenerTaskGettingResult(taskInfo: TaskInfo) extends SparkListenerEvent
```

**触发时机**：Task 开始获取结果时（远程获取）
**包含信息**：Task 信息

#### SparkListenerTaskEnd

```67:76:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
case class SparkListenerTaskEnd(
    stageId: Int,
    stageAttemptId: Int,
    taskType: String,
    reason: TaskEndReason,
    taskInfo: TaskInfo,
    taskExecutorMetrics: ExecutorMetrics,
    // may be null if the task has failed
    @Nullable taskMetrics: TaskMetrics)
  extends SparkListenerEvent
```

**触发时机**：Task 结束时
**包含信息**：Stage ID、Task 类型、结束原因、Task 信息、指标、度量

#### SparkListenerSpeculativeTaskSubmitted

```47:64:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
case class SparkListenerSpeculativeTaskSubmitted(
    stageId: Int,
    stageAttemptId: Int = 0)
  extends SparkListenerEvent {
  // Note: this is here for backwards-compatibility with older versions of this event which
  // didn't stored taskIndex
  private var _taskIndex: Int = -1
  private var _partitionId: Int = -1

  def taskIndex: Int = _taskIndex
  def partitionId: Int = _partitionId

  def this(stageId: Int, stageAttemptId: Int, taskIndex: Int, partitionId: Int) = {
    this(stageId, stageAttemptId)
    _partitionId = partitionId
    _taskIndex = taskIndex
  }
}
```

**触发时机**：提交推测执行 Task 时
**包含信息**：Stage ID、Task 索引、分区 ID

---

### 5. Executor 管理事件

#### SparkListenerExecutorAdded

```119:120:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
case class SparkListenerExecutorAdded(time: Long, executorId: String, executorInfo: ExecutorInfo)
  extends SparkListenerEvent
```

**触发时机**：Executor 添加时
**包含信息**：时间、Executor ID、Executor 信息

#### SparkListenerExecutorRemoved

```123:124:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
case class SparkListenerExecutorRemoved(time: Long, executorId: String, reason: String)
  extends SparkListenerEvent
```

**触发时机**：Executor 移除时
**包含信息**：时间、Executor ID、移除原因

#### SparkListenerExecutorExcluded

```136:140:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
@Since("3.1.0")
case class SparkListenerExecutorExcluded(
    time: Long,
    executorId: String,
    taskFailures: Int)
  extends SparkListenerEvent
```

**触发时机**：Executor 被排除时（由于失败）
**包含信息**：时间、Executor ID、任务失败次数

#### SparkListenerExecutorUnexcluded

```191:192:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
case class SparkListenerExecutorUnexcluded(time: Long, executorId: String)
  extends SparkListenerEvent
```

**触发时机**：Executor 被重新启用时
**包含信息**：时间、Executor ID

#### SparkListenerExecutorExcludedForStage

```155:161:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
@Since("3.1.0")
case class SparkListenerExecutorExcludedForStage(
    time: Long,
    executorId: String,
    taskFailures: Int,
    stageId: Int,
    stageAttemptId: Int)
  extends SparkListenerEvent
```

**触发时机**：Executor 在特定 Stage 被排除时
**包含信息**：时间、Executor ID、失败次数、Stage ID

---

### 6. Node 管理事件

#### SparkListenerNodeExcluded

```204:209:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
@Since("3.1.0")
case class SparkListenerNodeExcluded(
    time: Long,
    hostId: String,
    executorFailures: Int)
  extends SparkListenerEvent
```

**触发时机**：节点被排除时
**包含信息**：时间、主机 ID、Executor 失败次数

#### SparkListenerNodeUnexcluded

```218:218:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
@Since("3.1.0")
case class SparkListenerNodeUnexcluded(time: Long, hostId: String)
  extends SparkListenerEvent
```

**触发时机**：节点被重新启用时
**包含信息**：时间、主机 ID

---

### 7. BlockManager 事件

#### SparkListenerBlockManagerAdded

```103:109:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
case class SparkListenerBlockManagerAdded(
    time: Long,
    blockManagerId: BlockManagerId,
    maxMem: Long,
    maxOnHeapMem: Option[Long] = None,
    maxOffHeapMem: Option[Long] = None) extends SparkListenerEvent {
}
```

**触发时机**：BlockManager 添加时
**包含信息**：时间、BlockManager ID、最大内存

#### SparkListenerBlockManagerRemoved

```112:113:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
case class SparkListenerBlockManagerRemoved(time: Long, blockManagerId: BlockManagerId)
  extends SparkListenerEvent
```

**触发时机**：BlockManager 移除时
**包含信息**：时间、BlockManager ID

#### SparkListenerBlockUpdated

```234:234:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
case class SparkListenerBlockUpdated(blockUpdatedInfo: BlockUpdatedInfo) extends SparkListenerEvent
```

**触发时机**：Block 更新时
**包含信息**：Block 更新信息

---

### 8. RDD 事件

#### SparkListenerUnpersistRDD

```116:116:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
case class SparkListenerUnpersistRDD(rddId: Int) extends SparkListenerEvent
```

**触发时机**：RDD 被取消持久化时
**包含信息**：RDD ID

---

### 9. 指标更新事件

#### SparkListenerExecutorMetricsUpdate

```250:254:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
case class SparkListenerExecutorMetricsUpdate(
    execId: String,
    accumUpdates: Seq[(Long, Int, Int, Seq[AccumulableInfo])],
    executorUpdates: Map[(Int, Int), ExecutorMetrics] = Map.empty)
  extends SparkListenerEvent
```

**触发时机**：Executor 指标更新时（定期心跳）
**包含信息**：Executor ID、累加器更新、Executor 指标

#### SparkListenerStageExecutorMetrics

```265:270:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
case class SparkListenerStageExecutorMetrics(
    execId: String,
    stageId: Int,
    stageAttemptId: Int,
    executorMetrics: ExecutorMetrics)
  extends SparkListenerEvent
```

**触发时机**：Stage 完成时，记录 Executor 的峰值指标
**包含信息**：Executor ID、Stage ID、Executor 指标

---

### 10. 环境更新事件

#### SparkListenerEnvironmentUpdate

```98:100:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
case class SparkListenerEnvironmentUpdate(
    environmentDetails: Map[String, collection.Seq[(String, String)]])
  extends SparkListenerEvent
```

**触发时机**：环境信息更新时
**包含信息**：环境详细信息（JVM、Spark、系统属性等）

---

### 11. 其他事件

#### SparkListenerUnschedulableTaskSetAdded

```223:225:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
@Since("3.1.0")
case class SparkListenerUnschedulableTaskSetAdded(
  stageId: Int,
  stageAttemptId: Int) extends SparkListenerEvent
```

**触发时机**：TaskSet 变为不可调度时（由于动态分配和排除）
**包含信息**：Stage ID

#### SparkListenerUnschedulableTaskSetRemoved

```229:231:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
@Since("3.1.0")
case class SparkListenerUnschedulableTaskSetRemoved(
  stageId: Int,
  stageAttemptId: Int) extends SparkListenerEvent
```

**触发时机**：TaskSet 重新变为可调度时
**包含信息**：Stage ID

#### SparkListenerResourceProfileAdded

```295:296:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
@Since("3.1.0")
case class SparkListenerResourceProfileAdded(resourceProfile: ResourceProfile)
  extends SparkListenerEvent
```

**触发时机**：添加资源配置文件时
**包含信息**：资源配置文件

#### SparkListenerLogStart

```291:291:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
case class SparkListenerLogStart(sparkVersion: String) extends SparkListenerEvent
```

**触发时机**：事件日志开始时
**包含信息**：Spark 版本

#### SparkListenerMiscellaneousProcessAdded

```238:239:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
@DeveloperApi
@Since("3.2.0")
case class SparkListenerMiscellaneousProcessAdded(time: Long, processId: String,
    info: MiscellaneousProcessDetails) extends SparkListenerEvent
```

**触发时机**：添加其他进程时
**包含信息**：时间、进程 ID、进程详情

---

## 事件分发机制

### 事件发布

```126:153:core/src/main/scala/org/apache/spark/scheduler/LiveListenerBus.scala
  /** Post an event to all queues. */
  def post(event: SparkListenerEvent): Unit = {
    if (stopped.get()) {
      return
    }

    metrics.numEventsPosted.inc()

    // If the event buffer is null, it means the bus has been started and we can avoid
    // synchronization and post events directly to the queues. This should be the most
    // common case during the life of the bus.
    if (queuedEvents == null) {
      postToQueues(event)
      return
    }

    // Otherwise, need to synchronize to check whether the bus is started, to make sure the thread
    // calling start() picks up the new event.
    synchronized {
      if (!started.get()) {
        queuedEvents += event
        return
      }
    }

    // If the bus was already started when the check above was made, just post directly to the
    // queues.
    postToQueues(event)
  }

  private def postToQueues(event: SparkListenerEvent): Unit = {
    val it = queues.iterator()
    while (it.hasNext()) {
      it.next().post(event)
    }
  }
```

**流程：**

1. 检查是否已停止，如果是则忽略事件
2. 如果已启动，直接分发到所有队列
3. 如果未启动，缓冲事件（等待启动后分发）

### 事件处理

```28:102:core/src/main/scala/org/apache/spark/scheduler/SparkListenerBus.scala
  protected override def doPostEvent(
      listener: SparkListenerInterface,
      event: SparkListenerEvent): Unit = {
    event match {
      case stageSubmitted: SparkListenerStageSubmitted =>
        listener.onStageSubmitted(stageSubmitted)
      case stageCompleted: SparkListenerStageCompleted =>
        listener.onStageCompleted(stageCompleted)
      case jobStart: SparkListenerJobStart =>
        listener.onJobStart(jobStart)
      case jobEnd: SparkListenerJobEnd =>
        listener.onJobEnd(jobEnd)
      case taskStart: SparkListenerTaskStart =>
        listener.onTaskStart(taskStart)
      case taskGettingResult: SparkListenerTaskGettingResult =>
        listener.onTaskGettingResult(taskGettingResult)
      case taskEnd: SparkListenerTaskEnd =>
        listener.onTaskEnd(taskEnd)
      case environmentUpdate: SparkListenerEnvironmentUpdate =>
        listener.onEnvironmentUpdate(environmentUpdate)
      case blockManagerAdded: SparkListenerBlockManagerAdded =>
        listener.onBlockManagerAdded(blockManagerAdded)
      case blockManagerRemoved: SparkListenerBlockManagerRemoved =>
        listener.onBlockManagerRemoved(blockManagerRemoved)
      case unpersistRDD: SparkListenerUnpersistRDD =>
        listener.onUnpersistRDD(unpersistRDD)
      case applicationStart: SparkListenerApplicationStart =>
        listener.onApplicationStart(applicationStart)
      case applicationEnd: SparkListenerApplicationEnd =>
        listener.onApplicationEnd(applicationEnd)
      case metricsUpdate: SparkListenerExecutorMetricsUpdate =>
        listener.onExecutorMetricsUpdate(metricsUpdate)
      case stageExecutorMetrics: SparkListenerStageExecutorMetrics =>
        listener.onStageExecutorMetrics(stageExecutorMetrics)
      case executorAdded: SparkListenerExecutorAdded =>
        listener.onExecutorAdded(executorAdded)
      case executorRemoved: SparkListenerExecutorRemoved =>
        listener.onExecutorRemoved(executorRemoved)
      case executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage =>
        listener.onExecutorBlacklistedForStage(executorBlacklistedForStage)
      case nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage =>
        listener.onNodeBlacklistedForStage(nodeBlacklistedForStage)
      case executorBlacklisted: SparkListenerExecutorBlacklisted =>
        listener.onExecutorBlacklisted(executorBlacklisted)
      case executorUnblacklisted: SparkListenerExecutorUnblacklisted =>
        listener.onExecutorUnblacklisted(executorUnblacklisted)
      case nodeBlacklisted: SparkListenerNodeBlacklisted =>
        listener.onNodeBlacklisted(nodeBlacklisted)
      case nodeUnblacklisted: SparkListenerNodeUnblacklisted =>
        listener.onNodeUnblacklisted(nodeUnblacklisted)
      case executorExcludedForStage: SparkListenerExecutorExcludedForStage =>
        listener.onExecutorExcludedForStage(executorExcludedForStage)
      case nodeExcludedForStage: SparkListenerNodeExcludedForStage =>
        listener.onNodeExcludedForStage(nodeExcludedForStage)
      case executorExcluded: SparkListenerExecutorExcluded =>
        listener.onExecutorExcluded(executorExcluded)
      case executorUnexcluded: SparkListenerExecutorUnexcluded =>
        listener.onExecutorUnexcluded(executorUnexcluded)
      case nodeExcluded: SparkListenerNodeExcluded =>
        listener.onNodeExcluded(nodeExcluded)
      case nodeUnexcluded: SparkListenerNodeUnexcluded =>
        listener.onNodeUnexcluded(nodeUnexcluded)
      case blockUpdated: SparkListenerBlockUpdated =>
        listener.onBlockUpdated(blockUpdated)
      case speculativeTaskSubmitted: SparkListenerSpeculativeTaskSubmitted =>
        listener.onSpeculativeTaskSubmitted(speculativeTaskSubmitted)
      case unschedulableTaskSetAdded: SparkListenerUnschedulableTaskSetAdded =>
        listener.onUnschedulableTaskSetAdded(unschedulableTaskSetAdded)
      case unschedulableTaskSetRemoved: SparkListenerUnschedulableTaskSetRemoved =>
        listener.onUnschedulableTaskSetRemoved(unschedulableTaskSetRemoved)
      case resourceProfileAdded: SparkListenerResourceProfileAdded =>
        listener.onResourceProfileAdded(resourceProfileAdded)
      case _ => listener.onOtherEvent(event)
    }
  }
```

**说明：**

- 使用模式匹配（match）分发不同类型的事件
- 调用对应的监听器方法
- 未知事件通过 `onOtherEvent()` 处理

---

## 事件监听器接口

### SparkListenerInterface

```304:495:core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala
private[spark] trait SparkListenerInterface {

  /**
   * Called when a stage completes successfully or fails, with information on the completed stage.
   */
  def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit

  /**
   * Called when a stage is submitted
   */
  def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit

  /**
   * Called when a task starts
   */
  def onTaskStart(taskStart: SparkListenerTaskStart): Unit

  /**
   * Called when a task begins remotely fetching its result (will not be called for tasks that do
   * not need to fetch the result remotely).
   */
  def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit

  /**
   * Called when a task ends
   */
  def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit

  /**
   * Called when a job starts
   */
  def onJobStart(jobStart: SparkListenerJobStart): Unit

  /**
   * Called when a job ends
   */
  def onJobEnd(jobEnd: SparkListenerJobEnd): Unit

  /**
   * Called when environment properties have been updated
   */
  def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit

  /**
   * Called when a new block manager has joined
   */
  def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit

  /**
   * Called when an existing block manager has been removed
   */
  def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit

  /**
   * Called when an RDD is manually unpersisted by the application
   */
  def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit

  /**
   * Called when the application starts
   */
  def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit

  /**
   * Called when the application ends
   */
  def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit

  /**
   * Called when the driver receives task metrics from an executor in a heartbeat.
   */
  def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit

  /**
   * Called with the peak memory metrics for a given (executor, stage) combination. Note that this
   * is only present when reading from the event log (as in the history server), and is never
   * called in a live application.
   */
  def onStageExecutorMetrics(executorMetrics: SparkListenerStageExecutorMetrics): Unit

  /**
   * Called when the driver registers a new executor.
   */
  def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit

  /**
   * Called when the driver removes an executor.
   */
  def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit

  /**
   * Called when the driver excludes an executor for a Spark application.
   */
  @deprecated("use onExecutorExcluded instead", "3.1.0")
  def onExecutorBlacklisted(executorBlacklisted: SparkListenerExecutorBlacklisted): Unit

  /**
   * Called when the driver excludes an executor for a Spark application.
   */
  def onExecutorExcluded(executorExcluded: SparkListenerExecutorExcluded): Unit

  /**
   * Called when the driver excludes an executor for a stage.
   */
  @deprecated("use onExecutorExcludedForStage instead", "3.1.0")
  def onExecutorBlacklistedForStage(
      executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage): Unit

  /**
   * Called when the driver excludes an executor for a stage.
   */
  def onExecutorExcludedForStage(
      executorExcludedForStage: SparkListenerExecutorExcludedForStage): Unit

  /**
   * Called when the driver excludes a node for a stage.
   */
  @deprecated("use onNodeExcludedForStage instead", "3.1.0")
  def onNodeBlacklistedForStage(nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage): Unit

  /**
   * Called when the driver excludes a node for a stage.
   */
  def onNodeExcludedForStage(nodeExcludedForStage: SparkListenerNodeExcludedForStage): Unit

  /**
   * Called when the driver re-enables a previously excluded executor.
   */
  @deprecated("use onExecutorUnexcluded instead", "3.1.0")
  def onExecutorUnblacklisted(executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit

  /**
   * Called when the driver re-enables a previously excluded executor.
   */
  def onExecutorUnexcluded(executorUnexcluded: SparkListenerExecutorUnexcluded): Unit

  /**
   * Called when the driver excludes a node for a Spark application.
   */
  @deprecated("use onNodeExcluded instead", "3.1.0")
  def onNodeBlacklisted(nodeBlacklisted: SparkListenerNodeBlacklisted): Unit

  /**
   * Called when the driver excludes a node for a Spark application.
   */
  def onNodeExcluded(nodeExcluded: SparkListenerNodeExcluded): Unit

  /**
   * Called when the driver re-enables a previously excluded node.
   */
  @deprecated("use onNodeUnexcluded instead", "3.1.0")
  def onNodeUnblacklisted(nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit

  /**
   * Called when the driver re-enables a previously excluded node.
   */
  def onNodeUnexcluded(nodeUnexcluded: SparkListenerNodeUnexcluded): Unit

  /**
   * Called when a taskset becomes unschedulable due to exludeOnFailure and dynamic allocation
   * is enabled.
   */
  def onUnschedulableTaskSetAdded(
      unschedulableTaskSetAdded: SparkListenerUnschedulableTaskSetAdded): Unit

  /**
   * Called when an unschedulable taskset becomes schedulable and dynamic allocation
   * is enabled.
   */
  def onUnschedulableTaskSetRemoved(
      unschedulableTaskSetRemoved: SparkListenerUnschedulableTaskSetRemoved): Unit

  /**
   * Called when the driver receives a block update info.
   */
  def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit

  /**
   * Called when a speculative task is submitted
   */
  def onSpeculativeTaskSubmitted(speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit

  /**
   * Called when other events like SQL-specific events are posted.
   */
  def onOtherEvent(event: SparkListenerEvent): Unit

  /**
   * Called when a Resource Profile is added to the manager.
   */
  def onResourceProfileAdded(event: SparkListenerResourceProfileAdded): Unit
}
```

---

## 事件总结表

| 类别               | 事件类型                                       | 触发时机              | 主要信息                    |
| ---------------- | ------------------------------------------ | ----------------- | ----------------------- |
| **应用生命周期**       | `SparkListenerApplicationStart`            | 应用启动              | 应用名称、ID、用户              |
|                  | `SparkListenerApplicationEnd`              | 应用结束              | 结束时间、退出码                |
| **Job 生命周期**     | `SparkListenerJobStart`                    | Job 开始            | Job ID、Stage 信息         |
|                  | `SparkListenerJobEnd`                      | Job 结束            | Job ID、结果               |
| **Stage 生命周期**   | `SparkListenerStageSubmitted`              | Stage 提交          | Stage 信息                |
|                  | `SparkListenerStageCompleted`              | Stage 完成          | Stage 信息                |
| **Task 生命周期**    | `SparkListenerTaskStart`                   | Task 开始           | Task 信息                 |
|                  | `SparkListenerTaskGettingResult`           | Task 获取结果         | Task 信息                 |
|                  | `SparkListenerTaskEnd`                     | Task 结束           | Task 信息、指标              |
|                  | `SparkListenerSpeculativeTaskSubmitted`    | 推测任务提交            | Stage ID、Task 索引        |
| **Executor 管理**  | `SparkListenerExecutorAdded`               | Executor 添加       | Executor ID、信息          |
|                  | `SparkListenerExecutorRemoved`             | Executor 移除       | Executor ID、原因          |
|                  | `SparkListenerExecutorExcluded`            | Executor 排除       | Executor ID、失败次数        |
|                  | `SparkListenerExecutorUnexcluded`          | Executor 重新启用     | Executor ID             |
|                  | `SparkListenerExecutorExcludedForStage`    | Stage 级别排除        | Executor ID、Stage ID    |
| **Node 管理**      | `SparkListenerNodeExcluded`                | 节点排除              | 主机 ID、失败次数              |
|                  | `SparkListenerNodeUnexcluded`              | 节点重新启用            | 主机 ID                   |
|                  | `SparkListenerNodeExcludedForStage`        | Stage 级别节点排除      | 主机 ID、Stage ID          |
| **BlockManager** | `SparkListenerBlockManagerAdded`           | BlockManager 添加   | BlockManager ID、内存      |
|                  | `SparkListenerBlockManagerRemoved`         | BlockManager 移除   | BlockManager ID         |
|                  | `SparkListenerBlockUpdated`                | Block 更新          | Block 更新信息              |
| **RDD**          | `SparkListenerUnpersistRDD`                | RDD 取消持久化         | RDD ID                  |
| **指标更新**         | `SparkListenerExecutorMetricsUpdate`       | Executor 指标更新     | Executor ID、指标          |
|                  | `SparkListenerStageExecutorMetrics`        | Stage Executor 指标 | Executor ID、Stage ID、指标 |
| **环境**           | `SparkListenerEnvironmentUpdate`           | 环境更新              | 环境详细信息                  |
| **其他**           | `SparkListenerUnschedulableTaskSetAdded`   | TaskSet 不可调度      | Stage ID                |
|                  | `SparkListenerUnschedulableTaskSetRemoved` | TaskSet 重新可调度     | Stage ID                |
|                  | `SparkListenerResourceProfileAdded`        | 资源配置添加            | 资源配置                    |
|                  | `SparkListenerLogStart`                    | 事件日志开始            | Spark 版本                |
|                  | `SparkListenerMiscellaneousProcessAdded`   | 其他进程添加            | 进程 ID、详情                |

---

## 使用示例

### 自定义监听器

```scala
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerJobEnd}

class MySparkListener extends SparkListener {
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    println(s"Job ${jobStart.jobId} started at ${jobStart.time}")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    println(s"Job ${jobEnd.jobId} ended at ${jobEnd.time}")
  }
}

// 注册监听器
val spark = SparkSession.builder().getOrCreate()
spark.sparkContext.addSparkListener(new MySparkListener())
```

---

## 总结

`LiveListenerBus` 监听的事件涵盖了 Spark 应用的**完整生命周期**：

1. **应用级别**：启动、结束
2. **作业级别**：Job 开始、结束
3. **阶段级别**：Stage 提交、完成
4. **任务级别**：Task 开始、结束、推测执行
5. **资源管理**：Executor/Node 添加、移除、排除
6. **存储管理**：BlockManager、Block 更新
7. **指标监控**：Executor 指标、Stage 指标
8. **环境信息**：环境更新

这些事件为 Spark UI、事件日志、监控系统等提供了完整的数据源。
