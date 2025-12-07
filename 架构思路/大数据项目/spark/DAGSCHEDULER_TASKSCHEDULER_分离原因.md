# 为什么 Spark 要分离 DAGScheduler 和 TaskScheduler？

## 概述

Spark 将调度器分为 **DAGScheduler**（高层调度器）和 **TaskScheduler**（低层调度器）两个组件，这是基于**关注点分离（Separation of Concerns）**和**单一职责原则（Single Responsibility Principle）**的架构设计。

---

## 各自的作用

### DAGScheduler（高层调度器）

**职责：逻辑层面的调度**

```58:79:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
/**
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

**主要功能：**

1. **DAG 构建和 Stage 划分**
   
   - 将 RDD 依赖图转换为 Stage DAG
   - 在 Shuffle 边界处切分 Stage
   - 识别窄依赖和宽依赖

2. **Stage 依赖管理**
   
   - 跟踪 Stage 之间的依赖关系
   - 确定哪些 Stage 可以并行执行
   - 管理等待执行的 Stage（waitingStages）

3. **任务位置优化**
   
   - 基于缓存位置确定任务偏好位置
   - 考虑数据本地性（data locality）

4. **故障恢复（Stage 级别）**
   
   - 处理 Shuffle 输出文件丢失
   - 重新提交失败的 Stage
   - 处理 Stage 级别的重试

5. **缓存和 Shuffle 输出跟踪**
   
   - 跟踪哪些 RDD 已缓存
   - 跟踪哪些 ShuffleMapStage 已产生输出

---

### TaskScheduler（低层调度器）

**职责：物理层面的调度**

```27:34:core/src/main/scala/org/apache/spark/scheduler/TaskScheduler.scala
/**
 * Low-level task scheduler interface, currently implemented exclusively by
 * [[org.apache.spark.scheduler.TaskSchedulerImpl]].
 * This interface allows plugging in different task schedulers. Each TaskScheduler schedules tasks
 * for a single SparkContext. These schedulers get sets of tasks submitted to them from the
 * DAGScheduler for each stage, and are responsible for sending the tasks to the cluster, running
 * them, retrying if there are failures, and mitigating stragglers. They return events to the
 * DAGScheduler.
 */
```

**主要功能：**

1. **任务分配**
   
   - 将 TaskSet 中的任务分配到 Executor
   - 考虑资源可用性（CPU、内存、自定义资源）
   - 实现任务本地性调度（PROCESS_LOCAL → NODE_LOCAL → RACK_LOCAL → ANY）

2. **资源管理**
   
   - 跟踪 Executor 状态
   - 管理 Executor 资源分配
   - 处理 Executor 故障

3. **任务执行管理**
   
   - 发送任务到 Executor
   - 跟踪任务执行状态
   - 处理任务失败重试（Task 级别）

4. **性能优化**
   
   - 延迟调度（Delay Scheduling）
   - 推测执行（Speculation）
   - 负载均衡

5. **调度策略**
   
   - FIFO 调度
   - Fair 调度
   - 跨 Job 的资源分配

---

## 为什么需要分离？

### 1. **关注点分离（Separation of Concerns）**

#### DAGScheduler 关注：

- **逻辑层面**：RDD 依赖关系、Stage 划分、数据流
- **应用语义**：理解 Spark 应用的计算逻辑
- **数据依赖**：知道哪些数据需要先计算

#### TaskScheduler 关注：

- **物理层面**：集群资源、Executor 状态、网络拓扑
- **执行语义**：如何将任务放到机器上执行
- **资源管理**：CPU、内存、网络等物理资源

**如果合并会怎样？**

- 一个组件需要同时理解 RDD 依赖图和集群物理拓扑
- 代码复杂度急剧增加
- 难以维护和扩展

---

### 2. **职责边界清晰**

```
用户代码 (Action)
    ↓
DAGScheduler: "这个 Job 需要哪些 Stage？Stage 之间的依赖是什么？"
    ↓
TaskScheduler: "这些 Task 应该放到哪些 Executor 上？"
    ↓
Executor: 执行任务
```

**清晰的边界：**

- DAGScheduler：**知道做什么**（What to do）
- TaskScheduler：**知道怎么做**（How to do）

---

### 3. **故障处理层次不同**

#### DAGScheduler 处理：

- **Stage 级别的故障**
  - Shuffle 输出文件丢失 → 重新提交整个 Stage
  - Stage 依赖不满足 → 等待父 Stage 完成
  - 需要理解应用逻辑

#### TaskScheduler 处理：

- **Task 级别的故障**
  - 任务执行失败 → 重试单个任务
  - Executor 故障 → 重新分配任务
  - 不需要理解应用逻辑

**分离的好处：**

- 故障处理逻辑清晰
- 不同级别的故障由不同组件处理
- 避免故障处理逻辑混杂

---

### 4. **可扩展性和可插拔性**

#### TaskScheduler 是可插拔的：

```36:36:core/src/main/scala/org/apache/spark/scheduler/TaskScheduler.scala
private[spark] trait TaskScheduler {
```

- 可以有不同的实现（虽然目前只有 TaskSchedulerImpl）
- 可以与不同的集群管理器集成（Standalone、YARN、Kubernetes、Mesos）
- 可以有不同的调度策略（FIFO、Fair）

**如果合并：**

- 难以支持不同的集群管理器
- 调度策略和 DAG 逻辑耦合
- 扩展性差

---

### 5. **并发模型不同**

#### DAGScheduler：

```281:281:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
  private[spark] var eventProcessLoop = new DAGSchedulerEventProcessLoop(this)
```

- **单线程事件循环**：所有事件串行处理
- 保证状态一致性
- 简化并发控制

#### TaskScheduler：

```55:64:core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala
 * THREADING: [[SchedulerBackend]]s and task-submitting clients can call this class from multiple
 * threads, so it needs locks in public API methods to maintain its state. In addition, some
 * [[SchedulerBackend]]s synchronize on themselves when they want to send events here, and then
 * acquire a lock on us, so we need to make sure that we don't try to lock the backend while
 * we are holding a lock on ourselves.  This class is called from many threads, notably:
 *   * The DAGScheduler Event Loop
 *   * The RPCHandler threads, responding to status updates from Executors
 *   * Periodic revival of all offers from the CoarseGrainedSchedulerBackend, to accommodate delay
 *      scheduling
 *   * task-result-getter threads
```

- **多线程**：需要处理来自多个线程的调用
- 需要同步机制（synchronized）
- 处理 Executor 心跳、任务状态更新等

**如果合并：**

- 需要同时处理单线程和多线程场景
- 并发控制复杂
- 容易产生死锁

---

### 6. **性能优化策略不同**

#### DAGScheduler 优化：

- **数据本地性**：基于 RDD 缓存位置
- **Stage 共享**：多个 Job 共享 Stage
- **避免重复计算**：跟踪已计算的 Stage

#### TaskScheduler 优化：

- **任务本地性**：PROCESS_LOCAL → NODE_LOCAL → RACK_LOCAL
- **延迟调度**：等待更好的本地性
- **推测执行**：处理慢任务
- **负载均衡**：打乱资源分配

**分离的好处：**

- 不同层次的优化策略独立
- 可以分别优化
- 不会相互干扰

---

## 交互流程

### 正常执行流程

```
1. 用户调用 Action (如 rdd.count())
   ↓
2. DAGScheduler.submitJob()
   - 创建 Job
   - 构建 Stage DAG
   ↓
3. DAGScheduler.submitStage()
   - 检查父 Stage 是否完成
   - 如果父 Stage 未完成，递归提交父 Stage
   ↓
4. DAGScheduler.submitMissingTasks()
   - 创建 TaskSet
   - 确定任务偏好位置
   ↓
5. TaskScheduler.submitTasks()
   - 创建 TaskSetManager
   - 添加到调度池
   ↓
6. TaskScheduler.resourceOffers()
   - 从集群获取资源
   - 分配任务到 Executor
   ↓
7. Executor 执行任务
   ↓
8. TaskScheduler.statusUpdate()
   - 接收任务状态更新
   ↓
9. DAGScheduler.handleTaskCompletion()
   - 处理任务完成
   - 检查 Stage 是否完成
   - 如果完成，提交子 Stage
```

### 故障处理流程

```
任务失败 (FetchFailed)
   ↓
TaskScheduler.statusUpdate()
   ↓
DAGScheduler.handleTaskCompletion()
   - 识别为 FetchFailed
   - 标记 Stage 失败
   ↓
DAGScheduler.resubmitFailedStages()
   - 重新提交失败的 Stage
   ↓
TaskScheduler.submitTasks()
   - 重新分配任务
```

---

## 如果合并会有什么问题？

### 1. **代码复杂度急剧增加**

一个组件需要同时处理：

- RDD 依赖图分析
- Stage 划分
- 任务位置计算
- 资源分配
- 任务调度
- 故障恢复（Stage 和 Task 级别）
- 性能优化（多个层次）

**结果：** 代码难以理解和维护

---

### 2. **难以支持不同的集群管理器**

不同的集群管理器（YARN、Kubernetes、Mesos）有不同的：

- 资源模型
- 通信协议
- 故障处理机制

**如果合并：**

- DAG 逻辑和集群管理逻辑耦合
- 难以适配新的集群管理器
- 代码重复

**分离后：**

- TaskScheduler 可以适配不同的 SchedulerBackend
- DAGScheduler 不需要关心底层集群

---

### 3. **测试困难**

**分离后：**

- 可以单独测试 DAGScheduler（模拟 TaskScheduler）
- 可以单独测试 TaskScheduler（模拟 DAGScheduler）
- 测试用例清晰

**如果合并：**

- 需要同时模拟 RDD 依赖和集群资源
- 测试用例复杂
- 难以隔离测试

---

### 4. **性能优化受限**

**分离后：**

- DAGScheduler 可以专注于 Stage 级别的优化
- TaskScheduler 可以专注于任务级别的优化
- 互不干扰

**如果合并：**

- 优化策略可能冲突
- 难以分别优化
- 性能调优困难

---

### 5. **并发控制复杂**

**分离后：**

- DAGScheduler：单线程事件循环，简单
- TaskScheduler：多线程，但职责清晰

**如果合并：**

- 需要同时处理单线程和多线程场景
- 锁的粒度难以控制
- 容易产生死锁

---

## 设计模式

这种分离体现了多个设计模式：

### 1. **分层架构（Layered Architecture）**

```
应用层：用户代码
    ↓
逻辑层：DAGScheduler（理解应用逻辑）
    ↓
执行层：TaskScheduler（执行任务）
    ↓
资源层：SchedulerBackend（管理资源）
```

### 2. **策略模式（Strategy Pattern）**

TaskScheduler 可以有不同的调度策略（FIFO、Fair）

### 3. **观察者模式（Observer Pattern）**

TaskScheduler 通过事件通知 DAGScheduler

### 4. **单一职责原则（SRP）**

每个组件只负责一个明确的职责

---

## 总结

### 分离的核心原因

1. **关注点不同**
   
   - DAGScheduler：逻辑层面，理解应用
   - TaskScheduler：物理层面，管理资源

2. **职责不同**
   
   - DAGScheduler：决定做什么（What）
   - TaskScheduler：决定怎么做（How）

3. **故障处理层次不同**
   
   - DAGScheduler：Stage 级别
   - TaskScheduler：Task 级别

4. **并发模型不同**
   
   - DAGScheduler：单线程事件循环
   - TaskScheduler：多线程

5. **可扩展性**
   
   - 支持不同的集群管理器
   - 支持不同的调度策略

### 分离的好处

✅ **代码清晰**：职责明确，易于理解  
✅ **易于维护**：修改一个组件不影响另一个  
✅ **易于测试**：可以单独测试  
✅ **易于扩展**：可以独立扩展  
✅ **性能优化**：不同层次可以独立优化  

### 如果合并的问题

❌ **代码复杂**：一个组件承担太多职责  
❌ **难以扩展**：耦合度高  
❌ **难以测试**：测试用例复杂  
❌ **性能受限**：优化策略可能冲突  
❌ **并发控制困难**：需要处理多种并发场景  

---

**这种分离是 Spark 架构设计的核心思想之一，体现了良好的软件工程实践！** 、
