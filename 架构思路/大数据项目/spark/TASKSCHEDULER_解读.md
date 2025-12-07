# TaskScheduler 调度器解读

## 概述

`TaskScheduler` 是 Spark 的**任务调度器**，负责将 DAGScheduler 提交的 TaskSet 中的任务分配到集群中的 Executor 上执行。

### 核心职责

```27:34:core/src/main/scala/org/apache/spark/scheduler/TaskScheduler.scala
 * Low-level task scheduler interface, currently implemented exclusively by
 * [[org.apache.spark.scheduler.TaskSchedulerImpl]].
 * This interface allows plugging in different task schedulers. Each TaskScheduler schedules tasks
 * for a single SparkContext. These schedulers get sets of tasks submitted to them from the
 * DAGScheduler for each stage, and are responsible for sending the tasks to the cluster, running
 * them, retrying if there are failures, and mitigating stragglers. They return events to the
 * DAGScheduler.
```

**主要职责：**

1. **接收任务集**：从 DAGScheduler 接收 TaskSet
2. **资源分配**：将任务分配到可用的 Executor
3. **任务执行**：发送任务到 Executor 执行
4. **失败重试**：处理任务失败并重试
5. **处理慢任务**：通过推测执行缓解 straggler
6. **状态管理**：跟踪任务状态，通知 DAGScheduler

---

## 架构关系

```
DAGScheduler
    ↓ (提交 TaskSet)
TaskScheduler (TaskSchedulerImpl)
    ↓ (资源分配)
SchedulerBackend (与集群管理器通信)
    ↓ (发送任务)
Executor
```

### 核心组件

1. **TaskScheduler**：调度器接口
2. **TaskSchedulerImpl**：调度器实现
3. **SchedulerBackend**：与集群管理器通信的后端
4. **TaskSetManager**：管理单个 TaskSet 的任务调度
5. **Pool**：调度池（FIFO 或 Fair）

---

## 核心方法

### 1. submitTasks - 提交任务集

```243:285:core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala
  override def submitTasks(taskSet: TaskSet): Unit = {
    val tasks = taskSet.tasks
    logInfo(log"Adding task set " + taskSet.logId +
      log" with ${MDC(LogKeys.NUM_TASKS, tasks.length)} tasks resource profile " +
      log"${MDC(LogKeys.RESOURCE_PROFILE_ID, taskSet.resourceProfileId)}")
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
      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

      if (!isLocal && !hasReceivedTask) {
        starvationTimer.scheduleAtFixedRate(new TimerTask() {
          override def run(): Unit = {
            if (!hasLaunchedTask) {
              logWarning("Initial job has not accepted any resources; " +
                "check your cluster UI to ensure that workers are registered " +
                "and have sufficient resources")
            } else {
              this.cancel()
            }
          }
        }, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS, TimeUnit.MILLISECONDS)
      }
      hasReceivedTask = true
    }
    backend.reviveOffers()
  }
```

**执行流程：**

1. 创建 `TaskSetManager` 管理该 TaskSet
2. 将旧的 TaskSetManager 标记为 zombie（处理 Stage 重试）
3. 将 TaskSetManager 添加到调度池
4. 启动饥饿检测定时器（如果首次接收任务）
5. **调用 `backend.reviveOffers()`** 触发资源分配

---

### 2. resourceOffers - 资源分配核心方法

```488:736:core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala
  def resourceOffers(
      offers: IndexedSeq[WorkerOffer],
      isAllFreeResources: Boolean = true): Seq[Seq[TaskDescription]] = synchronized {
    // Mark each worker as alive and remember its hostname
    // Also track if new executor is added
    var newExecAvail = false
    for (o <- offers) {
      if (!hostToExecutors.contains(o.host)) {
        hostToExecutors(o.host) = new HashSet[String]()
      }
      if (!executorIdToRunningTaskIds.contains(o.executorId)) {
        hostToExecutors(o.host) += o.executorId
        executorAdded(o.executorId, o.host)
        executorIdToHost(o.executorId) = o.host
        executorIdToRunningTaskIds(o.executorId) = HashSet[Long]()
        newExecAvail = true
      }
    }
    val hosts = offers.map(_.host).distinct
    for ((host, Some(rack)) <- hosts.zip(getRacksForHosts(hosts))) {
      hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += host
    }

    // Before making any offers, include any nodes whose expireOnFailure timeout has expired. Do
    // this here to avoid a separate thread and added synchronization overhead, and also because
    // updating the excluded executors and nodes is only relevant when task offers are being made.
    healthTrackerOpt.foreach(_.applyExcludeOnFailureTimeout())

    val filteredOffers = healthTrackerOpt.map { healthTracker =>
      offers.filter { offer =>
        !healthTracker.isNodeExcluded(offer.host) &&
          !healthTracker.isExecutorExcluded(offer.executorId)
      }
    }.getOrElse(offers)

    val shuffledOffers = shuffleOffers(filteredOffers)
    // Build a list of tasks to assign to each worker.
    // Note the size estimate here might be off with different ResourceProfiles but should be
    // close estimate
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores / CPUS_PER_TASK))
    val availableResources = shuffledOffers.map(_.resources).toArray
    val availableCpus = shuffledOffers.map(o => o.cores).toArray
    val resourceProfileIds = shuffledOffers.map(o => o.resourceProfileId).toArray
    val sortedTaskSets = rootPool.getSortedTaskSetQueue
    for (taskSet <- sortedTaskSets) {
      logDebug("parentName: %s, name: %s, runningTasks: %s".format(
        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
      if (newExecAvail) {
        taskSet.executorAdded()
      }
    }

    // Take each TaskSet in our scheduling order, and then offer it to each node in increasing order
    // of locality levels so that it gets a chance to launch local tasks on all of them.
    // NOTE: the preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
    for (taskSet <- sortedTaskSets) {
      // we only need to calculate available slots if using barrier scheduling, otherwise the
      // value is -1
      val numBarrierSlotsAvailable = if (taskSet.isBarrier) {
        val rpId = taskSet.taskSet.resourceProfileId
        val resAmounts = availableResources.map(_.resourceAddressAmount)
        calculateAvailableSlots(this, conf, rpId, resourceProfileIds, availableCpus, resAmounts)
      } else {
        -1
      }
      // Skip the barrier taskSet if the available slots are less than the number of pending tasks.
      if (taskSet.isBarrier && numBarrierSlotsAvailable < taskSet.numTasks) {
        // Skip the launch process.
        // TODO SPARK-24819 If the job requires more slots than available (both busy and free
        // slots), fail the job on submit.
        logInfo(log"Skip current round of resource offers for barrier stage " +
          log"${MDC(LogKeys.STAGE_ID, taskSet.stageId)} because the barrier taskSet requires " +
          log"${MDC(LogKeys.TASK_SET_NAME, taskSet.numTasks)} slots, while the total " +
          log"number of available slots is ${MDC(LogKeys.NUM_SLOTS, numBarrierSlotsAvailable)}.")
      } else {
        var launchedAnyTask = false
        var noDelaySchedulingRejects = true
        var globalMinLocality: Option[TaskLocality] = None
        for (currentMaxLocality <- taskSet.myLocalityLevels) {
          var launchedTaskAtCurrentMaxLocality = false
          do {
            val (noDelayScheduleReject, minLocality) = resourceOfferSingleTaskSet(
              taskSet, currentMaxLocality, shuffledOffers, availableCpus,
              availableResources, tasks)
            launchedTaskAtCurrentMaxLocality = minLocality.isDefined
            launchedAnyTask |= launchedTaskAtCurrentMaxLocality
            noDelaySchedulingRejects &= noDelayScheduleReject
            globalMinLocality = minTaskLocality(globalMinLocality, minLocality)
          } while (launchedTaskAtCurrentMaxLocality)
        }

        if (!legacyLocalityWaitReset) {
          if (noDelaySchedulingRejects) {
            if (launchedAnyTask &&
              (isAllFreeResources || noRejectsSinceLastReset.getOrElse(taskSet.taskSet, true))) {
              taskSet.resetDelayScheduleTimer(globalMinLocality)
              noRejectsSinceLastReset.update(taskSet.taskSet, true)
            }
          } else {
            noRejectsSinceLastReset.update(taskSet.taskSet, false)
          }
        }

        if (!launchedAnyTask) {
          taskSet.getCompletelyExcludedTaskIfAny(hostToExecutors).foreach { taskIndex =>
              // If the taskSet is unschedulable we try to find an existing idle excluded
              // executor and kill the idle executor and kick off an abortTimer which if it doesn't
              // schedule a task within the timeout will abort the taskSet if we were unable to
              // schedule any task from the taskSet.
              // Note 1: We keep track of schedulability on a per taskSet basis rather than on a per
              // task basis.
              // Note 2: The taskSet can still be aborted when there are more than one idle
              // excluded executors and dynamic allocation is on. This can happen when a killed
              // idle executor isn't replaced in time by ExecutorAllocationManager as it relies on
              // pending tasks and doesn't kill executors on idle timeouts, resulting in the abort
              // timer to expire and abort the taskSet.
              //
              // If there are no idle executors and dynamic allocation is enabled, then we would
              // notify ExecutorAllocationManager to allocate more executors to schedule the
              // unschedulable tasks else we will abort immediately.
              executorIdToRunningTaskIds.find(x => !isExecutorBusy(x._1)) match {
                case Some ((executorId, _)) =>
                  if (!unschedulableTaskSetToExpiryTime.contains(taskSet)) {
                    healthTrackerOpt.foreach(blt => blt.killExcludedIdleExecutor(executorId))
                    updateUnschedulableTaskSetTimeoutAndStartAbortTimer(taskSet, taskIndex)
                  }
                case None =>
                  //  Notify ExecutorAllocationManager about the unschedulable task set,
                  // in order to provision more executors to make them schedulable
                  if (Utils.isDynamicAllocationEnabled(conf)) {
                    if (!unschedulableTaskSetToExpiryTime.contains(taskSet)) {
                      logInfo(log"Notifying ExecutorAllocationManager to allocate more executors to" +
                        log" schedule the unschedulable task before aborting" +
                        log" stage ${MDC(LogKeys.STAGE_ID, taskSet.stageId)}.")
                      dagScheduler.unschedulableTaskSetAdded(taskSet.taskSet.stageId,
                        taskSet.taskSet.stageAttemptId)
                      updateUnschedulableTaskSetTimeoutAndStartAbortTimer(taskSet, taskIndex)
                    }
                  } else {
                    // Abort Immediately
                    logInfo(log"Cannot schedule any task because all executors excluded from " +
                      log"failures. No idle executors can be found to kill. Aborting stage " +
                      log"${MDC(LogKeys.STAGE_ID, taskSet.stageId)}.")
                    taskSet.abortSinceCompletelyExcludedOnFailure(taskIndex)
                  }
              }
          }
        } else {
          // We want to defer killing any taskSets as long as we have a non excluded executor
          // which can be used to schedule a task from any active taskSets. This ensures that the
          // job can make progress.
          // Note: It is theoretically possible that a taskSet never gets scheduled on a
          // non-excluded executor and the abort timer doesn't kick in because of a constant
          // submission of new TaskSets. See the PR for more details.
          if (unschedulableTaskSetToExpiryTime.nonEmpty) {
            logInfo(log"Clearing the expiry times for all unschedulable taskSets as a task " +
              log"was recently scheduled.")
            // Notify ExecutorAllocationManager as well as other subscribers that a task now
            // recently becomes schedulable
            dagScheduler.unschedulableTaskSetRemoved(taskSet.taskSet.stageId,
              taskSet.taskSet.stageAttemptId)
            unschedulableTaskSetToExpiryTime.clear()
          }
        }

        if (launchedAnyTask && taskSet.isBarrier) {
          val barrierPendingLaunchTasks = taskSet.barrierPendingLaunchTasks.values.toArray
          // Check whether the barrier tasks are partially launched.
          if (barrierPendingLaunchTasks.length != taskSet.numTasks) {
            if (legacyLocalityWaitReset) {
              // Legacy delay scheduling always reset the timer when there's a task that is able
              // to be scheduled. Thus, whenever there's a timer reset could happen during a single
              // round resourceOffer, tasks that don't get or have the preferred locations would
              // always reject the offered resources. As a result, the barrier taskset can't get
              // launched. And if we retry the resourceOffer, we'd go through the same path again
              // and get into the endless loop in the end.
              val logMsg = log"Fail resource offers for barrier stage " +
                log"${MDC(STAGE_ID, taskSet.stageId)} because only " +
                log"${MDC(NUM_PENDING_LAUNCH_TASKS, barrierPendingLaunchTasks.length)} " +
                log"out of a total number " +
                log"of ${MDC(NUM_TASKS, taskSet.numTasks)} tasks got resource offers. " +
                log"We highly recommend you to use the non-legacy delay scheduling by setting " +
                log"${MDC(CONFIG, LEGACY_LOCALITY_WAIT_RESET.key)} to false " +
                log"to get rid of this error."
              logWarning(logMsg)
              taskSet.abort(logMsg.message)
              throw SparkCoreErrors.sparkError(logMsg.message)
            } else {
              val curTime = clock.getTimeMillis()
              if (curTime - taskSet.lastResourceOfferFailLogTime >
                TaskSetManager.BARRIER_LOGGING_INTERVAL) {
                logInfo(log"Releasing the assigned resource offers since only partial tasks can " +
                  log"be launched. Waiting for later round resource offers.")
                taskSet.lastResourceOfferFailLogTime = curTime
              }
              barrierPendingLaunchTasks.foreach { task =>
                // revert all assigned resources
                availableCpus(task.assignedOfferIndex) += task.assignedCores
                availableResources(task.assignedOfferIndex).release(
                  task.assignedResources)
                // re-add the task to the schedule pending list
                taskSet.addPendingTask(task.index)
              }
            }
          } else {
            // All tasks are able to launch in this barrier task set. Let's do
            // some preparation work before launching them.
            val launchTime = clock.getTimeMillis()
            val addressesWithDescs = barrierPendingLaunchTasks.map { task =>
              val taskDesc = taskSet.prepareLaunchingTask(
                task.execId,
                task.host,
                task.index,
                task.taskLocality,
                false,
                task.assignedCpus,
                task.assignedResources,
                launchTime)
              addRunningTask(taskDesc.taskId, taskDesc.executorId, taskSet)
              tasks(task.assignedOfferIndex) += taskDesc
              shuffledOffers(task.assignedOfferIndex).address.get -> taskDesc
            }

            // materialize the barrier coordinator.
            maybeInitBarrierCoordinator()

            // Update the taskInfos into all the barrier task properties.
            val addressesStr = addressesWithDescs
              // Addresses ordered by partitionId
              .sortBy(_._2.partitionId)
              .map(_._1)
              .mkString(",")
            addressesWithDescs.foreach(_._2.properties.setProperty("addresses", addressesStr))

            logInfo(log"Successfully scheduled all the ${MDC(LogKeys.NUM_TASKS, addressesWithDescs.length)} " +
              log"tasks for barrier stage ${MDC(LogKeys.STAGE_ID, taskSet.stageId)}.")
          }
          taskSet.barrierPendingLaunchTasks.clear()
        }
      }
    }

    // TODO SPARK-24823 Cancel a job that contains barrier stage(s) if the barrier tasks don't get
    // launched within a configured time.
    if (tasks.nonEmpty) {
      hasLaunchedTask = true
    }
    return tasks.map(_.toSeq)
  }
```

**执行流程：**

1. **更新 Executor 信息**
   
   - 记录每个 Executor 的主机名
   - 跟踪新增的 Executor

2. **过滤资源**
   
   - 排除被排除的节点和 Executor（基于健康跟踪器）

3. **打乱资源顺序**
   
   - 随机打乱，避免总是使用相同的 Executor

4. **按优先级遍历 TaskSet**
   
   - 从调度池获取排序后的 TaskSet 队列
   - 按优先级顺序处理

5. **任务本地性调度**
   
   - 按本地性级别（PROCESS_LOCAL → NODE_LOCAL → NO_PREF → RACK_LOCAL → ANY）尝试分配
   - 使用延迟调度（Delay Scheduling）优化数据本地性

6. **资源分配**
   
   - 检查资源是否满足任务需求（CPU、自定义资源）
   - 调用 `TaskSetManager.resourceOffer()` 获取任务
   - 更新可用资源

7. **Barrier 任务处理**
   
   - 如果是 Barrier 任务，需要等待所有任务都分配成功才启动

---

### 3. statusUpdate - 任务状态更新

```774:802:core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala
  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer): Unit = {
    synchronized {
      try {
        Option(taskIdToTaskSetManager.get(tid)) match {
          case Some(taskSet) =>
            assert(state != TaskState.LOST)
            if (TaskState.isFinished(state)) {
              cleanupTaskState(tid)
              taskSet.removeRunningTask(tid)
              if (state == TaskState.FINISHED) {
                taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
              } else if (Set(TaskState.FAILED, TaskState.KILLED).contains(state)) {
                taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
              }
            }
            if (state == TaskState.RUNNING) {
              taskSet.taskInfos(tid).launchSucceeded()
            }
          case None =>
            logError(log"Ignoring update with state ${MDC(LogKeys.TASK_STATE, state)} for " +
              log"TID ${MDC(LogKeys.TASK_ID, tid)} because its task set is gone (this is " +
              log"likely the result of receiving duplicate task finished status updates) or its " +
              log"executor has been marked as failed.")
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
  }
```

**处理逻辑：**

- 任务完成：清理状态，将结果加入结果队列
- 任务失败：清理状态，将失败信息加入失败队列
- 任务运行中：标记启动成功

---

## 调度算法

### FIFO（先进先出）

```214:215:core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
```

**特点：**

- 按提交顺序执行
- 先提交的 Job 优先获得资源
- 简单高效，适合单用户场景

### Fair（公平调度）

```216:217:core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala
        case SchedulingMode.FAIR =>
          new FairSchedulableBuilder(rootPool, sc)
```

**特点：**

- 多用户共享资源
- 支持调度池（Pool）配置权重
- 保证公平性，适合多租户场景

---

## 任务本地性（Locality）

### 本地性级别

```542:542:core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala
    // NOTE: the preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
```

**优先级（从高到低）：**

1. **PROCESS_LOCAL**：数据在同一个 JVM 进程中（最佳）
2. **NODE_LOCAL**：数据在同一节点上
3. **NO_PREF**：无偏好
4. **RACK_LOCAL**：数据在同一机架上
5. **ANY**：任意位置（最差）

### 延迟调度（Delay Scheduling）

```71:81:core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala
 * Delay Scheduling:
 *  Delay scheduling is an optimization that sacrifices job fairness for data locality in order to
 *  improve cluster and workload throughput. One useful definition of "delay" is how much time
 *  has passed since the TaskSet was using its fair share of resources. Since it is impractical to
 *  calculate this delay without a full simulation, the heuristic used is the time since the
 *  TaskSetManager last launched a task and has not rejected any resources due to delay scheduling
 *  since it was last offered its "fair share". A "fair share" offer is when [[resourceOffers]]'s
 *  parameter "isAllFreeResources" is set to true. A "delay scheduling reject" is when a resource
 *  is not utilized despite there being pending tasks (implemented inside [[TaskSetManager]]).
 *  The legacy heuristic only measured the time since the [[TaskSetManager]] last launched a task,
 *  and can be re-enabled by setting spark.locality.wait.legacyResetOnTaskLaunch to true.
```

**原理：**

- 为了数据本地性，可以等待一段时间
- 如果等待时间超过阈值，降低本地性要求
- 平衡数据本地性和资源利用率

---

## 推测执行（Speculation）

### 检查推测任务

```952:961:core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala
  // Check for speculatable tasks in all our active jobs.
  def checkSpeculatableTasks(): Unit = {
    var shouldRevive = false
    synchronized {
      shouldRevive = rootPool.checkSpeculatableTasks(MIN_TIME_TO_SPECULATION)
    }
    if (shouldRevive) {
      backend.reviveOffers()
    }
  }
```

**启动推测执行线程：**

```231:236:core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala
    if (!isLocal && conf.get(SPECULATION_ENABLED)) {
      logInfo("Starting speculative execution thread")
      speculationScheduler.scheduleWithFixedDelay(
        () => Utils.tryOrStopSparkContext(sc) { checkSpeculatableTasks() },
        SPECULATION_INTERVAL_MS, SPECULATION_INTERVAL_MS, TimeUnit.MILLISECONDS)
    }
```

**机制：**

- 定期检查运行中的任务
- 如果任务运行时间明显长于其他任务，启动推测副本
- 哪个先完成就用哪个结果

---

## 资源分配

### 资源检查

```456:467:core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala
  private def resourcesMeetTaskRequirements(
      taskSet: TaskSetManager,
      availCpus: Int,
      availWorkerResources: ExecutorResourcesAmounts): Option[Map[String, Map[String, Long]]] = {
    val rpId = taskSet.taskSet.resourceProfileId
    val taskSetProf = sc.resourceProfileManager.resourceProfileFromId(rpId)
    val taskCpus = ResourceProfile.getTaskCpusOrDefaultForProfile(taskSetProf, conf)
    // check if the ResourceProfile has cpus first since that is common case
    if (availCpus < taskCpus) return None
    // only look at the resource other than cpus
    availWorkerResources.assignAddressesCustomResources(taskSetProf)
  }
```

**检查项：**

1. CPU 资源是否足够
2. 自定义资源是否满足（GPU、内存等）
3. 资源配置文件（ResourceProfile）是否匹配

---

## 主要使用场景

### 1. 正常任务调度

**场景**：DAGScheduler 提交 TaskSet 后，TaskScheduler 负责分配和执行

**流程：**

```
DAGScheduler.submitStage()
  ↓
创建 TaskSet
  ↓
TaskScheduler.submitTasks()
  ↓
创建 TaskSetManager
  ↓
SchedulerBackend.reviveOffers()
  ↓
TaskScheduler.resourceOffers()
  ↓
TaskSetManager.resourceOffer()
  ↓
分配任务到 Executor
```

### 2. 任务失败重试

**场景**：任务执行失败，需要重新调度

**流程：**

```
Executor 报告任务失败
  ↓
TaskScheduler.statusUpdate()
  ↓
TaskSetManager.handleFailedTask()
  ↓
标记任务需要重试
  ↓
backend.reviveOffers() 重新分配资源
```

### 3. 推测执行

**场景**：检测到慢任务，启动推测副本

**流程：**

```
定期检查（checkSpeculatableTasks）
  ↓
识别慢任务
  ↓
提交推测任务
  ↓
backend.reviveOffers()
  ↓
在另一个 Executor 上启动推测副本
```

### 4. Executor 故障处理

**场景**：Executor 丢失，需要重新调度任务

**流程：**

```
检测 Executor 丢失
  ↓
TaskScheduler.executorLost()
  ↓
清理该 Executor 上的任务
  ↓
标记任务需要重试
  ↓
通知 DAGScheduler
  ↓
重新分配资源
```

### 5. 动态资源分配

**场景**：任务无法调度，需要更多 Executor

**流程：**

```
检测任务无法调度
  ↓
检查是否有空闲 Executor
  ↓
通知 ExecutorAllocationManager
  ↓
申请新 Executor
  ↓
Executor 注册后重新分配任务
```

### 6. Barrier 任务调度

**场景**：需要同时启动所有任务的 Stage（如分布式训练）

**流程：**

```
检测 Barrier TaskSet
  ↓
计算可用槽位
  ↓
等待所有任务都分配到资源
  ↓
同时启动所有任务
  ↓
BarrierCoordinator 协调同步
```

---

## 关键数据结构

### TaskSetManager 管理

```124:124:core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala
  private val taskSetsByStageIdAndAttempt = new HashMap[Int, HashMap[Int, TaskSetManager]]
```

**说明：**

- 按 Stage ID 和 Attempt ID 组织
- 支持 Stage 重试（多个 Attempt）

### 任务跟踪

```133:135:core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala
  private[scheduler] val taskIdToTaskSetManager = new ConcurrentHashMap[Long, TaskSetManager]
  // Protected by `this`
  val taskIdToExecutorId = new HashMap[Long, String]
```

**说明：**

- `taskIdToTaskSetManager`：任务 ID 到 TaskSetManager 的映射
- `taskIdToExecutorId`：任务 ID 到 Executor ID 的映射

### Executor 跟踪

```145:146:core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala
  // IDs of the tasks running on each executor
  private val executorIdToRunningTaskIds = new HashMap[String, HashSet[Long]]
```

**说明：**

- 跟踪每个 Executor 上运行的任务

---

## 执行流程图

### 完整调度流程

```
1. DAGScheduler 提交 TaskSet
   ↓
2. TaskScheduler.submitTasks()
   - 创建 TaskSetManager
   - 添加到调度池
   ↓
3. SchedulerBackend.reviveOffers()
   - 获取可用资源（WorkerOffer）
   ↓
4. TaskScheduler.resourceOffers()
   - 过滤资源（排除故障节点）
   - 打乱资源顺序
   - 按优先级遍历 TaskSet
   ↓
5. 任务本地性调度
   - PROCESS_LOCAL → NODE_LOCAL → RACK_LOCAL → ANY
   - 延迟调度优化
   ↓
6. TaskSetManager.resourceOffer()
   - 选择任务
   - 创建 TaskDescription
   ↓
7. 返回任务分配结果
   ↓
8. SchedulerBackend 发送任务到 Executor
   ↓
9. Executor 执行任务
   ↓
10. 任务状态更新
    - statusUpdate()
    - 处理成功/失败
```

---

## 性能优化特性

### 1. 延迟调度（Delay Scheduling）

- **目的**：提高数据本地性
- **机制**：等待一段时间以获得更好的本地性
- **配置**：`spark.locality.wait.*`

### 2. 推测执行（Speculation）

- **目的**：缓解 straggler 问题
- **机制**：为慢任务启动推测副本
- **配置**：`spark.speculation.*`

### 3. 资源打乱（Shuffle Offers）

- **目的**：负载均衡
- **机制**：随机打乱资源顺序，避免热点

### 4. 健康跟踪（Health Tracker）

- **目的**：排除故障节点
- **机制**：跟踪 Executor 失败，临时排除

---

## 配置参数

### 调度模式

- `spark.scheduler.mode`：FIFO 或 FAIR

### 本地性等待

- `spark.locality.wait`：默认等待时间
- `spark.locality.wait.process`：PROCESS_LOCAL 等待时间
- `spark.locality.wait.node`：NODE_LOCAL 等待时间
- `spark.locality.wait.rack`：RACK_LOCAL 等待时间

### 推测执行

- `spark.speculation`：是否启用
- `spark.speculation.interval`：检查间隔
- `spark.speculation.multiplier`：慢任务判定倍数

### 任务资源

- `spark.task.cpus`：每个任务需要的 CPU 数
- `spark.executor.cores`：每个 Executor 的 CPU 数

---

## 总结

### TaskScheduler 的核心作用

1. **任务分配**：将任务分配到 Executor
2. **资源管理**：跟踪和管理集群资源
3. **故障处理**：处理任务失败和 Executor 故障
4. **性能优化**：延迟调度、推测执行
5. **状态跟踪**：跟踪任务执行状态

### 主要使用场景

1. **正常任务调度**：日常任务分配和执行
2. **故障恢复**：任务失败重试、Executor 故障处理
3. **性能优化**：推测执行、延迟调度
4. **资源管理**：动态资源分配、资源隔离
5. **特殊任务**：Barrier 任务、资源密集型任务

### 设计特点

- **异步处理**：资源分配和任务执行异步进行
- **多线程安全**：使用同步机制保护共享状态
- **可扩展性**：支持不同的调度算法和后端
- **容错性**：完善的故障处理和重试机制

**TaskScheduler 是 Spark 执行引擎的核心组件，负责将逻辑执行计划转换为物理执行！**
