# TaskSet 划分逻辑详解

## 概述

`TaskSet` 是 Spark 调度中的一个核心概念，代表**一个 Stage 的一个 Attempt 中需要执行的所有任务**。TaskSet 的划分逻辑决定了哪些任务会被一起提交到 TaskScheduler。

---

## TaskSet 定义

```25:48:core/src/main/scala/org/apache/spark/scheduler/TaskSet.scala
/**
 * A set of tasks submitted together to the low-level TaskScheduler, usually representing
 * missing partitions of a particular stage.
 */
private[spark] class TaskSet(
    val tasks: Array[Task[_]],
    val stageId: Int,
    val stageAttemptId: Int,
    val priority: Int,
    val properties: Properties,
    val resourceProfileId: Int,
    val shuffleId: Option[Int]) {
  val id: String = s"$stageId.$stageAttemptId"

  override def toString: String = "TaskSet " + id

  // Identifier used in the structured logging framework.
  lazy val logId: MessageWithContext = {
    val hashMap = new java.util.HashMap[String, String]()
    hashMap.put(STAGE_ID.name, stageId.toString)
    hashMap.put(STAGE_ATTEMPT_ID.name, stageAttemptId.toString)
    MessageWithContext(id, hashMap)
  }
}
```

**关键字段：**

- `tasks`：任务数组
- `stageId`：所属 Stage ID
- `stageAttemptId`：Stage Attempt ID
- `shuffleId`：Shuffle ID（仅 ShuffleMapStage 有）

---

## 核心划分原则

### 1. **一个 Stage 的一个 Attempt = 一个 TaskSet**

**核心规则：**

- **一个 Stage 的一个 Attempt 对应一个 TaskSet**
- 不同 Attempt 会创建不同的 TaskSet
- 同一个 Stage 的不同 Attempt 不能同时是 active 的

**代码证据：**

```248:267:core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala
    this.synchronized {
      val manager = createTaskSetManager(taskSet, maxTaskFailures)
      val stage = taskSet.stageId
      val stageTaskSets =
        taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])

      // Mark all the existing TaskSetManagers of this stage as zombie, as we are adding a new one.
      // This is necessary to handle a corner case. Let's say a stage has 10 partitions and has 2
      // TaskSetManagers: TSM1(zombie) and TSM2(active). TSM1 has a running task for partition 10
      // and it completes. TSM2 finishes tasks for partition 1-9, and thinks he is still active
      // because partition 10 is not completed yet. However, DAGScheduler gets task completion
      // events for all the 10 partitions and thinks the stage is finished. If it's a shuffle stage
      // and somehow it has missing map outputs, then DAGScheduler will resubmit it and create a
      // TSM3 for it. As a stage can't have more than one active task set managers, we must mark
      // TSM2 as zombie (it actually is).
      stageTaskSets.foreach { case (_, ts) =>
        ts.isZombie = true
      }
      stageTaskSets(taskSet.stageAttemptId) = manager
```

**说明：**

- 当新的 TaskSet 提交时，旧的 TaskSetManager 会被标记为 zombie
- 一个 Stage 同时只能有一个 active 的 TaskSetManager

---

## TaskSet 创建流程

### 完整流程

```
1. DAGScheduler.submitStage()
   ↓
2. 检查父 Stage 是否完成
   ↓
3. 如果父 Stage 未完成，递归提交父 Stage
   ↓
4. 如果父 Stage 完成，调用 submitMissingTasks()
   ↓
5. submitMissingTasks() 创建 TaskSet
   - 调用 stage.findMissingPartitions() 找出缺失的分区
   - 为每个缺失分区创建 Task
   - 创建 TaskSet 包含所有这些任务
   ↓
6. TaskScheduler.submitTasks(taskSet)
```

---

## 缺失分区确定逻辑

### 1. ShuffleMapStage 的缺失分区

```86:97:core/src/main/scala/org/apache/spark/scheduler/ShuffleMapStage.scala
  /**
   * Returns true if the map stage is ready, i.e. all partitions have shuffle outputs.
   */
  def isAvailable: Boolean = numAvailableOutputs == numPartitions

  /** Returns the sequence of partition ids that are missing (i.e. needs to be computed). */
  override def findMissingPartitions(): Seq[Int] = {
    mapOutputTrackerMaster
      .findMissingPartitions(shuffleDep.shuffleId)
      .getOrElse(0 until numPartitions)
  }
```

**逻辑：**

- 通过 `MapOutputTrackerMaster` 查询哪些分区的 Shuffle 输出缺失
- 如果查询失败，返回所有分区（0 until numPartitions）

**场景：**

- 首次执行：所有分区都需要计算
- 部分失败：只有失败的分区需要重新计算
- 完全失败：所有分区都需要重新计算

---

### 2. ResultStage 的缺失分区

```57:65:core/src/main/scala/org/apache/spark/scheduler/ResultStage.scala
  /**
   * Returns the sequence of partition ids that are missing (i.e. needs to be computed).
   *
   * This can only be called when there is an active job.
   */
  override def findMissingPartitions(): Seq[Int] = {
    val job = activeJob.get
    (0 until job.numPartitions).filter(id => !job.finished(id))
  }
```

**逻辑：**

- 检查 Job 的 `finished` 数组
- 返回所有未完成的分区

**说明：**

- ResultStage 的分区可能少于 RDD 的分区数（如 `first()`、`take()` 等操作）
- 只计算 Job 需要的分区

---

## TaskSet 创建代码详解

### submitMissingTasks 方法

```1555:1786:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
  /** Called when stage's parents are available and we can now do its task. */
  private def submitMissingTasks(stage: Stage, jobId: Int): Unit = {
    logDebug("submitMissingTasks(" + stage + ")")

    // Before find missing partition, do the intermediate state clean work first.
    // The operation here can make sure for the partially completed intermediate stage,
    // `findMissingPartitions()` returns all partitions every time.
    stage match {
      case sms: ShuffleMapStage if !sms.isAvailable =>
        val needFullStageRetry = if (sms.shuffleDep.checksumMismatchFullRetryEnabled) {
          // When the parents of this stage are indeterminate (e.g., some parents are not
          // checkpointed and checksum mismatches are detected), the output data of the parents
          // may have changed due to task retries. For correctness reason, we need to
          // retry all tasks of the current stage. The legacy way of using current stage's
          // deterministic level to trigger full stage retry is not accurate.
          stage.isParentIndeterminate
        } else {
          if (stage.isIndeterminate) {
            // already executed at least once
            if (sms.getNextAttemptId > 0) {
              // While we previously validated possible rollbacks during the handling of a FetchFailure,
              // where we were fetching from an indeterminate source map stages, this later check
              // covers additional cases like recalculating an indeterminate stage after an executor
              // loss. Moreover, because this check occurs later in the process, if a result stage task
              // has successfully completed, we can detect this and abort the job, as rolling back a
              // result stage is not possible.
              val stagesToRollback = collectSucceedingStages(sms)
              abortStageWithInvalidRollBack(stagesToRollback)
              // stages which cannot be rolled back were aborted which leads to removing the
              // the dependant job(s) from the active jobs set
              val numActiveJobsWithStageAfterRollback =
                activeJobs.count(job => stagesToRollback.contains(job.finalStage))
              if (numActiveJobsWithStageAfterRollback == 0) {
                logInfo(log"All jobs depending on the indeterminate stage " +
                  log"(${MDC(STAGE_ID, stage.id)}) were aborted so this stage is not needed anymore.")
                return
              }
            }
            true
          } else {
            false
          }
        }

        if (needFullStageRetry) {
          mapOutputTracker.unregisterAllMapAndMergeOutput(sms.shuffleDep.shuffleId)
          sms.shuffleDep.newShuffleMergeState()
        }
      case _ =>
    }

    // Figure out the indexes of partition ids to compute.
    val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()

    // Use the scheduling pool, job group, description, etc. from an ActiveJob associated
    // with this Stage
    val properties = jobIdToActiveJob(jobId).properties
    addPySparkConfigsToProperties(stage, properties)

    runningStages += stage
    // SparkListenerStageSubmitted should be posted before testing whether tasks are
    // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
    // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
    // event.
    stage match {
      case s: ShuffleMapStage =>
        outputCommitCoordinator.stageStart(stage = s.id, maxPartitionId = s.numPartitions - 1)
        // Only generate merger location for a given shuffle dependency once.
        if (s.shuffleDep.shuffleMergeAllowed) {
          if (!s.shuffleDep.isShuffleMergeFinalizedMarked) {
            prepareShuffleServicesForShuffleMapStage(s)
          } else {
            // Disable Shuffle merge for the retry/reuse of the same shuffle dependency if it has
            // already been merge finalized. If the shuffle dependency was previously assigned
            // merger locations but the corresponding shuffle map stage did not complete
            // successfully, we would still enable push for its retry.
            s.shuffleDep.setShuffleMergeAllowed(false)
            logInfo(log"Push-based shuffle disabled for ${MDC(STAGE, stage)} " +
              log"(${MDC(STAGE_NAME, stage.name)}) since it is already shuffle merge finalized")
          }
        }
      case s: ResultStage =>
        outputCommitCoordinator.stageStart(
          stage = s.id, maxPartitionId = s.rdd.partitions.length - 1)
    }
    val taskIdToLocations: Map[Int, Seq[TaskLocation]] = try {
      stage match {
        case s: ShuffleMapStage =>
          partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id))}.toMap
        case s: ResultStage =>
          partitionsToCompute.map { id =>
            val p = s.partitions(id)
            (id, getPreferredLocs(stage.rdd, p))
          }.toMap
      }
    } catch {
      case NonFatal(e) =>
        stage.makeNewStageAttempt(partitionsToCompute.size)
        listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo,
          Utils.cloneProperties(properties)))
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocations.values.toSeq)

    // If there are tasks to execute, record the submission time of the stage. Otherwise,
    // post the even without the submission time, which indicates that this stage was
    // skipped.
    if (partitionsToCompute.nonEmpty) {
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    }
    listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo,
      Utils.cloneProperties(properties)))

    // TODO: Maybe we can keep the taskBinary in Stage to avoid serializing it multiple times.
    // Broadcasted binary for the task, used to dispatch tasks to executors. Note that we broadcast
    // the serialized copy of the RDD and for each task we will deserialize it, which means each
    // task gets a different copy of the RDD. This provides stronger isolation between tasks that
    // might modify state of objects referenced in their closures. This is necessary in Hadoop
    // where the JobConf/Configuration object is not thread-safe.
    var taskBinary: Broadcast[Array[Byte]] = null
    var partitions: Array[Partition] = null
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      var taskBinaryBytes: Array[Byte] = null
      // taskBinaryBytes and partitions are both effected by the checkpoint status. We need
      // this synchronization in case another concurrent job is checkpointing this RDD, so we get a
      // consistent view of both variables.
      RDDCheckpointData.synchronized {
        taskBinaryBytes = stage match {
          case stage: ShuffleMapStage =>
            JavaUtils.bufferToArray(
              closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
          case stage: ResultStage =>
            JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.func): AnyRef))
        }

        partitions = stage.rdd.partitions
      }

      if (taskBinaryBytes.length > TaskSetManager.TASK_SIZE_TO_WARN_KIB * 1024) {
        logWarning(log"Broadcasting large task binary with size " +
          log"${MDC(NUM_BYTES, Utils.bytesToString(taskBinaryBytes.length))}")
      }
      taskBinary = sc.broadcast(taskBinaryBytes)
    } catch {
      // In the case of a failure during serialization, abort the stage.
      case e: NotSerializableException =>
        abortStage(stage, "Task not serializable: " + e.toString, Some(e))
        runningStages -= stage

        // Abort execution
        return
      case e: Throwable =>
        abortStage(stage, s"Task serialization failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage

        // Abort execution
        return
    }

    val artifacts = jobIdToActiveJob(jobId).artifacts

    val tasks: Seq[Task[_]] = try {
      val serializedTaskMetrics = closureSerializer.serialize(stage.latestInfo.taskMetrics).array()
      stage match {
        case stage: ShuffleMapStage =>
          stage.pendingPartitions.clear()
          partitionsToCompute.map { id =>
            val locs = taskIdToLocations(id)
            val part = partitions(id)
            stage.pendingPartitions += id
            new ShuffleMapTask(stage.id, stage.latestInfo.attemptNumber(), taskBinary,
              part, stage.numPartitions, locs, artifacts, properties, serializedTaskMetrics,
              Option(jobId), Option(sc.applicationId), sc.applicationAttemptId,
              stage.rdd.isBarrier())
          }

        case stage: ResultStage =>
          partitionsToCompute.map { id =>
            val p: Int = stage.partitions(id)
            val part = partitions(p)
            val locs = taskIdToLocations(id)
            new ResultTask(stage.id, stage.latestInfo.attemptNumber(),
              taskBinary, part, stage.numPartitions, locs, id, artifacts, properties,
              serializedTaskMetrics, Option(jobId), Option(sc.applicationId),
              sc.applicationAttemptId, stage.rdd.isBarrier())
          }
      }
    } catch {
      case NonFatal(e) =>
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    if (tasks.nonEmpty) {
      logInfo(log"Submitting ${MDC(NUM_TASKS, tasks.size)} missing tasks from " +
        log"${MDC(STAGE, stage)} (${MDC(RDD_ID, stage.rdd)}) (first 15 tasks are " +
        log"for partitions ${MDC(PARTITION_IDS, tasks.take(15).map(_.partitionId))})")
      val shuffleId = stage match {
        case s: ShuffleMapStage => Some(s.shuffleDep.shuffleId)
        case _: ResultStage => None
      }

      taskScheduler.submitTasks(new TaskSet(
        tasks.toArray, stage.id, stage.latestInfo.attemptNumber(), jobId, properties,
        stage.resourceProfileId, shuffleId))
    } else {
      // Because we posted SparkListenerStageSubmitted earlier, we should mark
      // the stage as completed here in case there are no tasks to run
      stage match {
        case stage: ShuffleMapStage =>
          logDebug(s"Stage ${stage} is actually done; " +
              s"(available: ${stage.isAvailable}," +
              s"available outputs: ${stage.numAvailableOutputs}," +
              s"partitions: ${stage.numPartitions})")
          if (!stage.shuffleDep.isShuffleMergeFinalizedMarked &&
            stage.shuffleDep.getMergerLocs.nonEmpty) {
            checkAndScheduleShuffleMergeFinalize(stage)
          } else {
            processShuffleMapStageCompletion(stage)
          }
        case stage : ResultStage =>
          logDebug(s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})")
          markStageAsFinished(stage)
          submitWaitingChildStages(stage)
      }
    }
  }
```

**关键步骤：**

1. **确定缺失分区**
   
   ```scala
   val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()
   ```

2. **计算任务偏好位置**
   
   ```scala
   val taskIdToLocations: Map[Int, Seq[TaskLocation]] = ...
   ```

3. **创建 Stage Attempt**
   
   ```scala
   stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocations.values.toSeq)
   ```

4. **序列化任务二进制**
   
   - ShuffleMapStage：序列化 `(rdd, shuffleDep)`
   - ResultStage：序列化 `(rdd, func)`

5. **创建任务**
   
   - ShuffleMapStage：创建 `ShuffleMapTask`
   - ResultStage：创建 `ResultTask`

6. **创建并提交 TaskSet**
   
   ```scala
   taskScheduler.submitTasks(new TaskSet(
     tasks.toArray, stage.id, stage.latestInfo.attemptNumber(), jobId, properties,
     stage.resourceProfileId, shuffleId))
   ```

---

## 划分逻辑详解

### 1. **按 Stage 和 Attempt 划分**

**规则：**

- **一个 Stage 的一个 Attempt = 一个 TaskSet**
- TaskSet ID = `stageId.stageAttemptId`

**示例：**

```
Stage 0, Attempt 0 → TaskSet "0.0"
Stage 0, Attempt 1 → TaskSet "0.1"  (重试)
Stage 1, Attempt 0 → TaskSet "1.0"
```

---

### 2. **包含所有缺失分区的任务**

**规则：**

- TaskSet 包含该 Stage Attempt 中**所有缺失分区的任务**
- 不是所有分区，只是缺失的分区

**场景：**

#### 场景 1：首次执行

```
Stage 有 100 个分区
缺失分区：0-99（全部）
TaskSet 包含：100 个任务
```

#### 场景 2：部分失败重试

```
Stage 有 100 个分区
已完成：0-90（91 个）
缺失分区：91-99（9 个）
TaskSet 包含：9 个任务
```

#### 场景 3：完全失败重试

```
Stage 有 100 个分区
所有输出丢失
缺失分区：0-99（全部）
TaskSet 包含：100 个任务
```

---

### 3. **特殊处理：全 Stage 重试**

```1598:1601:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
        if (needFullStageRetry) {
          mapOutputTracker.unregisterAllMapAndMergeOutput(sms.shuffleDep.shuffleId)
          sms.shuffleDep.newShuffleMergeState()
        }
```

**触发条件：**

- Stage 是不确定的（indeterminate）
- 父 Stage 是不确定的
- 需要重新计算所有分区

**处理：**

- 取消注册所有 Map 输出
- 重置 Shuffle 合并状态
- 所有分区都会包含在 TaskSet 中

---

## TaskSet 与 Stage 的关系

### 关系图

```
Job
  ↓
Stage (逻辑概念)
  ↓
Stage Attempt (执行尝试)
  ↓
TaskSet (任务集合)
  ↓
Task (单个任务)
```

### 一对多关系

```
一个 Job → 多个 Stage
一个 Stage → 多个 Attempt（重试）
一个 Stage Attempt → 一个 TaskSet
一个 TaskSet → 多个 Task（每个分区一个）
```

---

## 任务创建逻辑

### ShuffleMapTask 创建

```1723:1733:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
        case stage: ShuffleMapStage =>
          stage.pendingPartitions.clear()
          partitionsToCompute.map { id =>
            val locs = taskIdToLocations(id)
            val part = partitions(id)
            stage.pendingPartitions += id
            new ShuffleMapTask(stage.id, stage.latestInfo.attemptNumber(), taskBinary,
              part, stage.numPartitions, locs, artifacts, properties, serializedTaskMetrics,
              Option(jobId), Option(sc.applicationId), sc.applicationAttemptId,
              stage.rdd.isBarrier())
          }
```

**特点：**

- 每个分区创建一个 ShuffleMapTask
- 任务包含 Shuffle 依赖信息
- 记录到 `pendingPartitions`

---

### ResultTask 创建

```1735:1744:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
        case stage: ResultStage =>
          partitionsToCompute.map { id =>
            val p: Int = stage.partitions(id)
            val part = partitions(p)
            val locs = taskIdToLocations(id)
            new ResultTask(stage.id, stage.latestInfo.attemptNumber(),
              taskBinary, part, stage.numPartitions, locs, id, artifacts, properties,
              serializedTaskMetrics, Option(jobId), Option(sc.applicationId),
              sc.applicationAttemptId, stage.rdd.isBarrier())
          }
```

**特点：**

- 每个分区创建一个 ResultTask
- 任务包含用户函数（func）
- 有 outputId（用于结果收集）

---

## 为什么这样划分？

### 1. **原子性保证**

**一个 TaskSet 中的所有任务：**

- 属于同一个 Stage
- 属于同一个 Attempt
- 使用相同的任务二进制（taskBinary）
- 共享相同的配置和属性

**好处：**

- 如果 Stage 失败，可以整体重试
- 任务之间的一致性得到保证

---

### 2. **故障恢复粒度**

**TaskSet 级别：**

- 如果部分任务失败，可以只重试失败的任务
- 如果整个 Stage 失败，创建新的 Attempt 和新的 TaskSet

**示例：**

```
TaskSet "0.0" 提交了 100 个任务
- 90 个成功
- 10 个失败

创建新的 TaskSet "0.1"（新的 Attempt）
- 只包含 10 个失败分区的任务
```

---

### 3. **资源管理**

**TaskSet 级别：**

- 可以统一管理资源需求（ResourceProfile）
- 可以统一处理任务本地性
- 可以统一进行推测执行

---

### 4. **调度效率**

**批量提交：**

- 一次提交多个任务，减少调度开销
- TaskScheduler 可以批量分配资源
- 提高调度效率

---

## 特殊情况处理

### 1. **空 TaskSet**

```1765:1785:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
    } else {
      // Because we posted SparkListenerStageSubmitted earlier, we should mark
      // the stage as completed here in case there are no tasks to run
      stage match {
        case stage: ShuffleMapStage =>
          logDebug(s"Stage ${stage} is actually done; " +
              s"(available: ${stage.isAvailable}," +
              s"available outputs: ${stage.numAvailableOutputs}," +
              s"partitions: ${stage.numPartitions})")
          if (!stage.shuffleDep.isShuffleMergeFinalizedMarked &&
            stage.shuffleDep.getMergerLocs.nonEmpty) {
            checkAndScheduleShuffleMergeFinalize(stage)
          } else {
            processShuffleMapStageCompletion(stage)
          }
        case stage : ResultStage =>
          logDebug(s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})")
          markStageAsFinished(stage)
          submitWaitingChildStages(stage)
      }
    }
```

**场景：**

- 所有分区都已计算完成
- 没有缺失分区

**处理：**

- 不创建 TaskSet
- 直接标记 Stage 完成
- 提交等待的子 Stage

---

### 2. **Barrier TaskSet**

**特点：**

- 所有任务必须同时启动
- 任务之间需要同步
- 一个任务失败，整个 TaskSet 失败

**处理：**

- TaskScheduler 会检查是否有足够的资源
- 只有所有任务都能分配资源时才启动

---

### 3. **部分完成 Stage 的重试**

**场景：**

- Stage 部分任务完成
- 部分任务失败或输出丢失

**处理：**

```1606:1606:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
    val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()
```

- 只包含缺失分区的任务
- 已完成的分区不会重新计算

---

## 划分流程图

```
用户调用 Action
    ↓
DAGScheduler.submitJob()
    ↓
创建 ResultStage
    ↓
DAGScheduler.submitStage()
    ↓
检查父 Stage
    ↓
如果父 Stage 未完成 → 递归提交父 Stage
    ↓
如果父 Stage 完成 → submitMissingTasks()
    ↓
stage.findMissingPartitions()
    ↓
确定缺失分区列表
    ↓
为每个缺失分区创建 Task
    ↓
创建 TaskSet (stageId, stageAttemptId, tasks)
    ↓
TaskScheduler.submitTasks(taskSet)
```

---

## 关键数据结构

### TaskSet 标识

```37:37:core/src/main/scala/org/apache/spark/scheduler/TaskSet.scala
  val id: String = s"$stageId.$stageAttemptId"
```

**格式：** `stageId.stageAttemptId`

**示例：**

- `"0.0"`：Stage 0 的第 0 次尝试
- `"0.1"`：Stage 0 的第 1 次尝试（重试）
- `"1.0"`：Stage 1 的第 0 次尝试

---

### TaskSetManager 管理

```251:252:core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala
      val stageTaskSets =
        taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])
```

**结构：**

```
taskSetsByStageIdAndAttempt: HashMap[StageId, HashMap[AttemptId, TaskSetManager]]
```

**说明：**

- 按 Stage ID 组织
- 每个 Stage 可以有多个 Attempt
- 每个 Attempt 对应一个 TaskSetManager

---

## 实际示例

### 示例 1：正常执行

```
Job: count()
  ↓
Stage 0 (ShuffleMapStage): 100 个分区
  ↓
TaskSet "0.0": 100 个 ShuffleMapTask
  ↓
Stage 1 (ResultStage): 10 个分区
  ↓
TaskSet "1.0": 10 个 ResultTask
```

---

### 示例 2：部分失败重试

```
Stage 0, Attempt 0: 100 个任务
  - 90 个成功
  - 10 个失败
  ↓
Stage 0, Attempt 1: 10 个任务（只重试失败的）
  ↓
TaskSet "0.1": 10 个 ShuffleMapTask
```

---

### 示例 3：完全失败重试

```
Stage 0, Attempt 0: 100 个任务
  - Executor 故障，所有输出丢失
  ↓
Stage 0, Attempt 1: 100 个任务（全部重试）
  ↓
TaskSet "0.1": 100 个 ShuffleMapTask
```

---

## 总结

### TaskSet 划分的核心原则

1. **一个 Stage 的一个 Attempt = 一个 TaskSet**
   
   - 这是最核心的划分规则
   - 保证了任务的一致性和原子性

2. **包含所有缺失分区的任务**
   
   - 不是所有分区，只是缺失的分区
   - 通过 `findMissingPartitions()` 确定

3. **按 Stage 类型创建不同类型的任务**
   
   - ShuffleMapStage → ShuffleMapTask
   - ResultStage → ResultTask

4. **共享相同的配置和资源**
   
   - 同一个 TaskSet 中的任务共享 taskBinary
   - 共享 ResourceProfile
   - 共享调度属性

### 划分的好处

✅ **原子性**：一个 TaskSet 中的任务属于同一个执行单元  
✅ **故障恢复**：可以按 TaskSet 粒度进行重试  
✅ **资源管理**：统一管理资源需求  
✅ **调度效率**：批量提交，提高效率  
✅ **一致性**：保证任务之间的一致性  

**TaskSet 的划分逻辑体现了 Spark 的容错和调度设计思想！**
