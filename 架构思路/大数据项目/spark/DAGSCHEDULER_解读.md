# DAGScheduler 代码解读

## 概述

`DAGScheduler` 是 Spark 的**高级调度层**，实现了**面向 Stage 的调度**。它负责将 RDD 的 DAG 分解为 Stage，跟踪哪些 RDD 和 Stage 输出已经物化，并找到运行 Job 的最小调度方案。

```58:79:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
* The high-level scheduling layer that implements stage-oriented scheduling. It computes a DAG of

* stages for each job, keeps track of which RDDs and stage outputs are materialized, and finds a

* minimal schedule to run the job. It then submits stages as TaskSets to an underlying

* TaskScheduler implementation that runs them on the cluster. A TaskSet contains fully independent

* tasks that can run right away based on the data that's already on the cluster (e.g. map output

* files from previous stages), though it may fail if this data becomes unavailable.

*

* Spark stages are created by breaking the RDD graph at shuffle boundaries. RDD operations with

* "narrow" dependencies, like map() and filter(), are pipelined together into one set of tasks

* in each stage, but operations with shuffle dependencies require multiple stages (one to write a

* set of map output files, and another to read those files after a barrier). In the end, every

* stage will have only shuffle dependencies on other stages, and may compute multiple operations

* inside it. The actual pipelining of these operations happens in the RDD.compute() functions of

* various RDDs

*

* In addition to coming up with a DAG of stages, the DAGScheduler also determines the preferred

* locations to run each task on, based on the current cache status, and passes these to the

* low-level TaskScheduler. Furthermore, it handles failures due to shuffle output files being

* lost, in which case old stages may need to be resubmitted. Failures *within* a stage that are

* not caused by shuffle file loss are handled by the TaskScheduler, which will retry each task

* a small number of times before cancelling the whole stage.
```

### 核心职责

1. **Stage 划分**：将 RDD DAG 分解为 Stage（在 Shuffle 边界处划分）

2. **Stage 调度**：确定 Stage 的执行顺序，提交 Stage 到 TaskScheduler

3. **任务本地性**：根据缓存状态确定任务的偏好位置

4. **故障恢复**：处理 Shuffle 文件丢失导致的失败，重新提交失败的 Stage

5. **状态管理**：跟踪 Job、Stage、Task 的状态

---

## 核心概念

### 1. Job（作业）

```83:85:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
* - Jobs (represented by [[ActiveJob]]) are the top-level work items submitted to the scheduler.

* For example, when the user calls an action, like count(), a job will be submitted through

* submitJob. Each Job may require the execution of multiple stages to build intermediate data.
```

- **定义**：用户调用 Action（如 `count()`, `collect()`）时提交的顶层工作项

- **表示**：`ActiveJob` 类

- **特点**：一个 Job 可能需要执行多个 Stage

### 2. Stage（阶段）

```87:92:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
* - Stages ([[Stage]]) are sets of tasks that compute intermediate results in jobs, where each

* task computes the same function on partitions of the same RDD. Stages are separated at shuffle

* boundaries, which introduce a barrier (where we must wait for the previous stage to finish to

* fetch outputs). There are two types of stages: [[ResultStage]], for the final stage that

* executes an action, and [[ShuffleMapStage]], which writes map output files for a shuffle.

* Stages are often shared across multiple jobs, if these jobs reuse the same RDDs.
```

- **定义**：一组计算相同 RDD 分区的任务集合

- **划分**：在 Shuffle 边界处划分

- **类型**：

- **ResultStage**：最终阶段，执行 Action

- **ShuffleMapStage**：写入 Shuffle map 输出文件

- **特点**：多个 Job 可能共享同一个 Stage

### 3. Task（任务）

```94:94:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
* - Tasks are individual units of work, each sent to one machine.
```

- **定义**：单个工作单元，发送到一台机器执行

- **类型**：

- **ShuffleMapTask**：ShuffleMapStage 中的任务

- **ResultTask**：ResultStage 中的任务

---

## 架构关系

```
用户代码 (Action)

↓

SparkContext.runJob()

↓

DAGScheduler.submitJob()

↓

创建 ResultStage

↓

DAGScheduler.submitStage()

↓

创建 TaskSet

↓

TaskScheduler.submitTasks()

↓

TaskScheduler.resourceOffers()

↓

Executor 执行任务
```

---

## 核心数据结构

### Job 管理

```152:161:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
private[scheduler] val jobIdToStageIds = new HashMap[Int, HashSet[Int]]

private[scheduler] val stageIdToStage = new HashMap[Int, Stage]

/**

* Mapping from shuffle dependency ID to the ShuffleMapStage that will generate the data for

* that dependency. Only includes stages that are part of currently running job (when the job(s)

* that require the shuffle stage complete, the mapping will be removed, and the only record of

* the shuffle data will be in the MapOutputTracker).

*/

private[scheduler] val shuffleIdToMapStage = new HashMap[Int, ShuffleMapStage]

private[scheduler] val jobIdToActiveJob = new HashMap[Int, ActiveJob]
```

### Stage 状态管理

```163:172:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
// Stages we need to run whose parents aren't done

private[scheduler] val waitingStages = new HashSet[Stage]



// Stages we are running right now

private[scheduler] val runningStages = new HashSet[Stage]



// Stages that must be resubmitted due to fetch failures

private[scheduler] val failedStages = new HashSet[Stage]



private[scheduler] val activeJobs = new HashSet[ActiveJob]
```

### 缓存位置跟踪

```180:190:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
/**

* Contains the locations that each RDD's partitions are cached on. This map's keys are RDD ids

* and its values are arrays indexed by partition numbers. Each array value is the set of

* locations where that RDD partition is cached.

*

* All accesses to this map should be guarded by synchronizing on it (see SPARK-4454).

* If you need to access any RDD while synchronizing on the cache locations,

* first synchronize on the RDD, and then synchronize on this map to avoid deadlocks. The RDD

* could try to access the cache locations after synchronizing on the RDD.

*/

private val cacheLocs = new HashMap[Int, IndexedSeq[Seq[TaskLocation]]]
```

---

## 核心方法

### 1. submitJob - 提交作业

```930:981:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
val jobId = nextJobId.getAndIncrement()

if (partitions.isEmpty) {

val clonedProperties = Utils.cloneProperties(properties)

if (sc.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION) == null) {

clonedProperties.setProperty(SparkContext.SPARK_JOB_DESCRIPTION, callSite.shortForm)

}

val time = clock.getTimeMillis()

listenerBus.post(

SparkListenerJobStart(jobId, time, Seq.empty, clonedProperties))

listenerBus.post(

SparkListenerJobEnd(jobId, time, JobSucceeded))

// Return immediately if the job is running 0 tasks

return new JobWaiter[U](this, jobId, 0, resultHandler)

}



assert(partitions.nonEmpty)

val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]

val waiter = new JobWaiter[U](this, jobId, partitions.size, resultHandler)

eventProcessLoop.post(JobSubmitted(

jobId, rdd, func2, partitions.toArray, callSite, waiter,

JobArtifactSet.getActiveOrDefault(sc),

Utils.cloneProperties(properties)))

waiter
```

**执行流程：**

1. 生成 Job ID

2. 如果分区为空，立即返回成功

3. 创建 `JobWaiter` 用于等待 Job 完成

4. **将 `JobSubmitted` 事件发送到事件循环**

### 2. runJob - 运行作业（同步）

```997:1021:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
def runJob[T, U](

rdd: RDD[T],

func: (TaskContext, Iterator[T]) => U,

partitions: Seq[Int],

callSite: CallSite,

resultHandler: (Int, U) => Unit,

properties: Properties): Unit = {

val start = System.nanoTime

val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)

ThreadUtils.awaitReady(waiter.completionFuture, Duration.Inf)

waiter.completionFuture.value.get match {

case scala.util.Success(_) =>

logInfo(log"Job ${MDC(LogKeys.JOB_ID, waiter.jobId)} finished: " +

log"${MDC(LogKeys.CALL_SITE_SHORT_FORM, callSite.shortForm)}, took " +

log"${MDC(LogKeys.TIME, (System.nanoTime - start) / 1e6)} ms")

case scala.util.Failure(exception) =>

logInfo(log"Job ${MDC(LogKeys.JOB_ID, waiter.jobId)} failed: " +

log"${MDC(LogKeys.CALL_SITE_SHORT_FORM, callSite.shortForm)}, took " +

log"${MDC(LogKeys.TIME, (System.nanoTime - start) / 1e6)} ms")

// SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.

val callerStackTrace = Thread.currentThread().getStackTrace.tail

exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)

throw exception

}

}
```

**特点：**

- 调用 `submitJob()` 提交作业

- **阻塞等待** Job 完成

- 处理成功或失败情况

### 3. handleJobSubmitted - 处理作业提交

```1328:1409:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
private[scheduler] def handleJobSubmitted(

jobId: Int,

finalRDD: RDD[_],

func: (TaskContext, Iterator[_]) => _,

partitions: Array[Int],

callSite: CallSite,

listener: JobListener,

artifacts: JobArtifactSet,

properties: Properties): Unit = {

// If this job belongs to a cancelled job group, skip running it

val jobGroupIdOpt = Option(properties).map(_.getProperty(SparkContext.SPARK_JOB_GROUP_ID))

if (jobGroupIdOpt.exists(cancelledJobGroups.contains(_))) {

listener.jobFailed(

SparkCoreErrors.sparkJobCancelledAsPartOfJobGroupError(jobId, jobGroupIdOpt.get))

logInfo(log"Skip running a job that belongs to the cancelled job group ${MDC(GROUP_ID, jobGroupIdOpt.get)}")

return

}



var finalStage: ResultStage = null

try {

// New stage creation may throw an exception if, for example, jobs are run on a

// HadoopRDD whose underlying HDFS files have been deleted.

finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite)

} catch {

case e: BarrierJobSlotsNumberCheckFailed =>

// If jobId doesn't exist in the map, Scala coverts its value null to 0: Int automatically.

val numCheckFailures = barrierJobIdToNumTasksCheckFailures.compute(jobId,

(_: Int, value: Int) => value + 1)



logWarning(log"Barrier stage in job ${MDC(JOB_ID, jobId)} " +

log"requires ${MDC(NUM_SLOTS, e.requiredConcurrentTasks)} slots, " +

log"but only ${MDC(MAX_SLOTS, e.maxConcurrentTasks)} are available. " +

log"Will retry up to ${MDC(NUM_RETRIES, maxFailureNumTasksCheck - numCheckFailures + 1)} " +

log"more times")



if (numCheckFailures <= maxFailureNumTasksCheck) {

messageScheduler.schedule(

new Runnable {

override def run(): Unit = eventProcessLoop.post(JobSubmitted(jobId, finalRDD, func,

partitions, callSite, listener, artifacts, properties))

},

timeIntervalNumTasksCheck,

TimeUnit.SECONDS

)

return

} else {

// Job failed, clear internal data.

barrierJobIdToNumTasksCheckFailures.remove(jobId)

listener.jobFailed(e)

return

}



case e: Exception =>

logWarning(log"Creating new stage failed due to exception - job: ${MDC(JOB_ID, jobId)}", e)

listener.jobFailed(e)

return

}

// Job submitted, clear internal data.

barrierJobIdToNumTasksCheckFailures.remove(jobId)



val job = new ActiveJob(jobId, finalStage, callSite, listener, artifacts, properties)

clearCacheLocs()

logInfo(

log"Got job ${MDC(LogKeys.JOB_ID, job.jobId)} (${MDC(CALL_SITE_SHORT_FORM, callSite.shortForm)}) " +

log"with ${MDC(NUM_PARTITIONS, partitions.length)} output partitions")

logInfo(log"Final stage: ${MDC(STAGE, finalStage)} " +

log"(${MDC(STAGE_NAME, finalStage.name)})")

logInfo(log"Parents of final stage: ${MDC(STAGES, finalStage.parents)}")

logInfo(log"Missing parents: ${MDC(MISSING_PARENT_STAGES, getMissingParentStages(finalStage))}")



val jobSubmissionTime = clock.getTimeMillis()

jobIdToActiveJob(jobId) = job

activeJobs += job

finalStage.setActiveJob(job)

val stageIds = jobIdToStageIds(jobId).toArray

val stageInfos =

stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo)).toImmutableArraySeq

listenerBus.post(

SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos,

Utils.cloneProperties(properties)))

submitStage(finalStage)

}
```

**执行流程：**

1. 检查 Job 是否属于已取消的 Job Group

2. **创建 ResultStage**（`createResultStage`）

3. 创建 `ActiveJob`

4. 注册 Job 到内部数据结构

5. 发送 `SparkListenerJobStart` 事件

6. **提交 Stage**（`submitStage`）

### 4. submitStage - 提交 Stage

```1459:1490:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
/** Submits stage, but first recursively submits any missing parents. */

private def submitStage(stage: Stage): Unit = {

val jobId = activeJobForStage(stage)

if (jobId.isDefined) {

logDebug(s"submitStage($stage (name=${stage.name};" +

s"jobs=${stage.jobIds.toSeq.sorted.mkString(",")}))")

if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {

if (stage.getNextAttemptId >= maxStageAttempts) {

val reason = s"$stage (name=${stage.name}) has been resubmitted for the maximum " +

s"allowable number of times: ${maxStageAttempts}, which is the max value of " +

s"config `${config.STAGE_MAX_ATTEMPTS.key}` and " +

s"`${config.STAGE_MAX_CONSECUTIVE_ATTEMPTS.key}`."

abortStage(stage, reason, None)

} else {

val missing = getMissingParentStages(stage).sortBy(_.id)

logInfo(log"Missing parents found for ${MDC(STAGE, stage)}: ${MDC(MISSING_PARENT_STAGES, missing)}")

if (missing.isEmpty) {

logInfo(log"Submitting ${MDC(STAGE, stage)} (${MDC(RDD_ID, stage.rdd)}), " +

log"which has no missing parents")

submitMissingTasks(stage, jobId.get)

} else {

for (parent <- missing) {

submitStage(parent)

}

waitingStages += stage

}

}

}

} else {

abortStage(stage, "No active job for stage " + stage.id, None)

}

}
```

**执行流程：**

1. 检查 Stage 是否有活跃的 Job

2. 检查 Stage 是否已经在等待、运行或失败状态

3. 检查 Stage 重试次数是否超过限制

4. **获取缺失的父 Stage**（`getMissingParentStages`）

5. 如果父 Stage 都已完成：
- 调用 `submitMissingTasks()` 提交任务
6. 如果还有父 Stage 未完成：
- **递归提交父 Stage**

- 将当前 Stage 加入 `waitingStages`

### 5. submitMissingTasks - 提交缺失的任务

```1554:1786:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
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

**执行流程：**

1. **查找缺失的分区**（`findMissingPartitions()`）

2. 获取任务的偏好位置（`getPreferredLocs()`）

3. 创建新的 Stage Attempt

4. 发送 `SparkListenerStageSubmitted` 事件

5. **序列化任务**（RDD + ShuffleDep 或 RDD + func）

6. **创建 Task 对象**（ShuffleMapTask 或 ResultTask）

7. **提交 TaskSet 到 TaskScheduler**

### 6. handleTaskCompletion - 处理任务完成

```1932:2368:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
private[scheduler] def handleTaskCompletion(event: CompletionEvent): Unit = {

val task = event.task

val stageId = task.stageId

outputCommitCoordinator.taskCompleted(

stageId,

task.stageAttemptId,

task.partitionId,

event.taskInfo.attemptNumber, // this is a task attempt number

event.reason)

if (!stageIdToStage.contains(task.stageId)) {

// The stage may have already finished when we get this event -- e.g. maybe it was a

// speculative task. It is important that we send the TaskEnd event in any case, so listeners

// are properly notified and can chose to handle it. For instance, some listeners are

// doing their own accounting and if they don't get the task end event they think

// tasks are still running when they really aren't.

postTaskEnd(event)

// Skip all the actions if the stage has been cancelled.

return

}

val stage = stageIdToStage(task.stageId)

// Make sure the task's accumulators are updated before any other processing happens, so that

// we can post a task end event before any jobs or stages are updated. The accumulators are

// only updated in certain cases.

event.reason match {

case Success =>

task match {

case rt: ResultTask[_, _] =>

val resultStage = stage.asInstanceOf[ResultStage]

resultStage.activeJob match {

case Some(job) =>

// Only update the accumulator once for each result task.

if (!job.finished(rt.outputId)) {

updateAccumulators(event)

}

case None => // Ignore update if task's job has finished.

}

case _ =>

updateAccumulators(event)

}

case _: ExceptionFailure | _: TaskKilled => updateAccumulators(event)

case _ =>

}

if (trackingCacheVisibility) {

// Update rdd blocks' visibility status.

blockManagerMaster.updateRDDBlockVisibility(

event.taskInfo.taskId, visible = event.reason == Success)

}

postTaskEnd(event)

event.reason match {

case Success =>

// An earlier attempt of a stage (which is zombie) may still have running tasks. If these

// tasks complete, they still count and we can mark the corresponding partitions as

// finished if the stage is determinate. Here we notify the task scheduler to skip running

// tasks for the same partition to save resource.

if (!stage.isIndeterminate && task.stageAttemptId < stage.latestInfo.attemptNumber()) {

taskScheduler.notifyPartitionCompletion(stageId, task.partitionId)

}

task match {

case rt: ResultTask[_, _] =>

// Cast to ResultStage here because it's part of the ResultTask

// TODO Refactor this out to a function that accepts a ResultStage

val resultStage = stage.asInstanceOf[ResultStage]

resultStage.activeJob match {

case Some(job) =>

if (!job.finished(rt.outputId)) {

job.finished(rt.outputId) = true

job.numFinished += 1

// If the whole job has finished, remove it

if (job.numFinished == job.numPartitions) {

markStageAsFinished(resultStage)

cancelRunningIndependentStages(job, s"Job ${job.jobId} is finished.")

cleanupStateForJobAndIndependentStages(job)

try {

// killAllTaskAttempts will fail if a SchedulerBackend does not implement

// killTask.

logInfo(log"Job ${MDC(JOB_ID, job.jobId)} is finished. Cancelling " +

log"potential speculative or zombie tasks for this job")

// ResultStage is only used by this job. It's safe to kill speculative or

// zombie tasks in this stage.

taskScheduler.killAllTaskAttempts(

stageId,

shouldInterruptTaskThread(job),

reason = "Stage finished")

} catch {

case e: UnsupportedOperationException =>

logWarning(log"Could not cancel tasks " +

log"for stage ${MDC(STAGE, stageId)}", e)

}

listenerBus.post(

SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))

}

// taskSucceeded runs some user code that might throw an exception. Make sure

// we are resilient against that.

try {

job.listener.taskSucceeded(rt.outputId, event.result)

} catch {

case e: Throwable if !Utils.isFatalError(e) =>

// TODO: Perhaps we want to mark the resultStage as failed?

job.listener.jobFailed(new SparkDriverExecutionException(e))

}

}

case None =>

logInfo(log"Ignoring result from ${MDC(RESULT, rt)} because its job has finished")

}

case smt: ShuffleMapTask =>

val shuffleStage = stage.asInstanceOf[ShuffleMapStage]

// Ignore task completion for old attempt of indeterminate stage

val ignoreIndeterminate = stage.isIndeterminate &&

task.stageAttemptId < stage.latestInfo.attemptNumber()

if (!ignoreIndeterminate) {

shuffleStage.pendingPartitions -= task.partitionId

val status = event.result.asInstanceOf[MapStatus]

val execId = status.location.executorId

logDebug("ShuffleMapTask finished on " + execId)

if (executorFailureEpoch.contains(execId) &&

smt.epoch <= executorFailureEpoch(execId)) {

logInfo(log"Ignoring possibly bogus ${MDC(STAGE, smt)} completion from " +

log"executor ${MDC(EXECUTOR_ID, execId)}")

} else {

// The epoch of the task is acceptable (i.e., the task was launched after the most

// recent failure we're aware of for the executor), so mark the task's output as

// available.

val isChecksumMismatched = mapOutputTracker.registerMapOutput(

shuffleStage.shuffleDep.shuffleId, smt.partitionId, status)

if (isChecksumMismatched) {

shuffleStage.isChecksumMismatched = isChecksumMismatched

// There could be multiple checksum mismatches detected for a single stage attempt.

// We check for stage abortion once and only once when we first detect checksum

// mismatch for each stage attempt. For example, assume that we have

// stage1 -> stage2, and we encounter checksum mismatch during the retry of stage1.

// In this case, we need to call abortUnrollbackableStages() for the succeeding

// stages. Assume that when stage2 is retried, some tasks finish and some tasks

// failed again with FetchFailed. In case that we encounter checksum mismatch again

// during the retry of stage1, we need to call abortUnrollbackableStages() again.

if (shuffleStage.maxChecksumMismatchedId < smt.stageAttemptId) {

shuffleStage.maxChecksumMismatchedId = smt.stageAttemptId

if (shuffleStage.shuffleDep.checksumMismatchFullRetryEnabled

&& shuffleStage.isStageIndeterminate) {

abortUnrollbackableStages(shuffleStage)

}

}

}

}

} else {

logInfo(log"Ignoring ${MDC(TASK_NAME, smt)} completion from an older attempt of indeterminate stage")

}

if (runningStages.contains(shuffleStage) && shuffleStage.pendingPartitions.isEmpty) {

if (!shuffleStage.shuffleDep.isShuffleMergeFinalizedMarked &&

shuffleStage.shuffleDep.getMergerLocs.nonEmpty) {

checkAndScheduleShuffleMergeFinalize(shuffleStage)

} else {

processShuffleMapStageCompletion(shuffleStage)

}

}

}

case FetchFailed(bmAddress, shuffleId, _, mapIndex, reduceId, failureMessage) =>

val failedStage = stageIdToStage(task.stageId)

val mapStage = shuffleIdToMapStage(shuffleId)

if (failedStage.latestInfo.attemptNumber() != task.stageAttemptId) {

logInfo(log"Ignoring fetch failure from " +

log"${MDC(TASK_ID, task)} as it's from " +

log"${MDC(FAILED_STAGE, failedStage)} attempt " +

log"${MDC(STAGE_ATTEMPT_ID, task.stageAttemptId)} and there is a more recent attempt for " +

log"that stage (attempt " +

log"${MDC(NUM_ATTEMPT, failedStage.latestInfo.attemptNumber())}) running")

} else {

val ignoreStageFailure = ignoreDecommissionFetchFailure &&

isExecutorDecommissioningOrDecommissioned(taskScheduler, bmAddress)

if (ignoreStageFailure) {

logInfo(log"Ignoring fetch failure from ${MDC(TASK_NAME, task)} of " +

log"${MDC(FAILED_STAGE, failedStage)} attempt " +

log"${MDC(STAGE_ATTEMPT_ID, task.stageAttemptId)} when count " +

log"${MDC(MAX_ATTEMPTS, config.STAGE_MAX_CONSECUTIVE_ATTEMPTS.key)} " +

log"as executor ${MDC(EXECUTOR_ID, bmAddress.executorId)} is decommissioned and " +

log"${MDC(CONFIG, config.STAGE_IGNORE_DECOMMISSION_FETCH_FAILURE.key)}=true")

} else {

failedStage.failedAttemptIds.add(task.stageAttemptId)

}

val shouldAbortStage =

failedStage.failedAttemptIds.size >= maxConsecutiveStageAttempts ||

disallowStageRetryForTest

// It is likely that we receive multiple FetchFailed for a single stage (because we have

// multiple tasks running concurrently on different executors). In that case, it is

// possible the fetch failure has already been handled by the scheduler.

if (runningStages.contains(failedStage)) {

logInfo(log"Marking ${MDC(FAILED_STAGE, failedStage)} " +

log"(${MDC(FAILED_STAGE_NAME, failedStage.name)}) as failed " +

log"due to a fetch failure from ${MDC(STAGE, mapStage)} " +

log"(${MDC(STAGE_NAME, mapStage.name)})")

markStageAsFinished(failedStage, errorMessage = Some(failureMessage),

willRetry = !shouldAbortStage)

} else {

logDebug(s"Received fetch failure from $task, but it's from $failedStage which is no " +

"longer running")

}

if (mapStage.rdd.isBarrier()) {

// Mark all the map as broken in the map stage, to ensure retry all the tasks on

// resubmitted stage attempt.

// TODO: SPARK-35547: Clean all push-based shuffle metadata like merge enabled and

// TODO: finalized as we are clearing all the merge results.

mapOutputTracker.unregisterAllMapAndMergeOutput(shuffleId)

} else if (mapIndex != -1) {

// Mark the map whose fetch failed as broken in the map stage

mapOutputTracker.unregisterMapOutput(shuffleId, mapIndex, bmAddress)

if (pushBasedShuffleEnabled) {

// Possibly unregister the merge result <shuffleId, reduceId>, if the FetchFailed

// mapIndex is part of the merge result of <shuffleId, reduceId>

mapOutputTracker.

unregisterMergeResult(shuffleId, reduceId, bmAddress, Option(mapIndex))

}

} else {

// Unregister the merge result of <shuffleId, reduceId> if there is a FetchFailed event

// and is not a MetaDataFetchException which is signified by bmAddress being null

if (bmAddress != null &&

bmAddress.executorId.equals(BlockManagerId.SHUFFLE_MERGER_IDENTIFIER)) {

assert(pushBasedShuffleEnabled, "Push based shuffle expected to " +

"be enabled when handling merge block fetch failure.")

mapOutputTracker.

unregisterMergeResult(shuffleId, reduceId, bmAddress, None)

}

}

if (failedStage.rdd.isBarrier()) {

failedStage match {

case failedMapStage: ShuffleMapStage =>

// Mark all the map as broken in the map stage, to ensure retry all the tasks on

// resubmitted stage attempt.

mapOutputTracker.unregisterAllMapAndMergeOutput(failedMapStage.shuffleDep.shuffleId)

case failedResultStage: ResultStage =>

// Abort the failed result stage since we may have committed output for some

// partitions.

val reason = "Could not recover from a failed barrier ResultStage. Most recent " +

s"failure reason: $failureMessage"

abortStage(failedResultStage, reason, None)

}

}

if (shouldAbortStage) {

val abortMessage = if (disallowStageRetryForTest) {

"Fetch failure will not retry stage due to testing config"

} else {

s"$failedStage (${failedStage.name}) has failed the maximum allowable number of " +

s"times: $maxConsecutiveStageAttempts. Most recent failure reason:\n" +

failureMessage

}

abortStage(failedStage, abortMessage, None)

} else { // update failedStages and make sure a ResubmitFailedStages event is enqueued

// TODO: Cancel running tasks in the failed stage -- cf. SPARK-17064

val noResubmitEnqueued = !failedStages.contains(failedStage)

failedStages += failedStage

failedStages += mapStage

if (noResubmitEnqueued) {

// If the map stage is INDETERMINATE, which means the map tasks may return

// different result when re-try, we need to re-try all the tasks of the failed

// stage and its succeeding stages, because the input data will be changed after the

// map tasks are re-tried.

// Note that, if map stage is UNORDERED, we are fine. The shuffle partitioner is

// guaranteed to be determinate, so the input data of the reducers will not change

// even if the map tasks are re-tried.

if (mapStage.isIndeterminate && !mapStage.shuffleDep.checksumMismatchFullRetryEnabled) {
```
