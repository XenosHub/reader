# Spark 划分 Stage、TaskSet、Task 的作用和原因

## 概述

Spark 将计算任务组织成三个层次：**Stage → TaskSet → Task**。这种层次结构是 Spark 调度系统的核心设计，每个层次都有其特定的作用和设计目的。

---

## 层次结构图

```
Job (作业)
  ↓
Stage (阶段) - 在 Shuffle 边界划分
  ↓
TaskSet (任务集) - Stage 的一个 Attempt
  ↓
Task (任务) - 单个分区的计算单元
```

---

## 一、Stage（阶段）

### 定义和作用

```27:36:core/src/main/scala/org/apache/spark/scheduler/Stage.scala
/**
 * A stage is a set of parallel tasks all computing the same function that need to run as part
 * of a Spark job, where all the tasks have the same shuffle dependencies. Each DAG of tasks run
 * by the scheduler is split up into stages at the boundaries where shuffle occurs, and then the
 * DAGScheduler runs these stages in topological order.
 *
 * Each Stage can either be a shuffle map stage, in which case its tasks' results are input for
 * other stage(s), or a result stage, in which case its tasks directly compute a Spark action
 * (e.g. count(), save(), etc) by running a function on an RDD. For shuffle map stages, we also
 * track the nodes that each output partition is on.
 */
```

**核心特点：**
- **在 Shuffle 边界划分**：Stage 在 Shuffle 依赖处切分
- **拓扑顺序执行**：Stage 按依赖关系顺序执行
- **两种类型**：ShuffleMapStage 和 ResultStage

---

### 为什么要在 Shuffle 边界划分 Stage？

#### 1. **Shuffle 是 Barrier（屏障）**

```66:72:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
 * Spark stages are created by breaking the RDD graph at shuffle boundaries. RDD operations with
 * "narrow" dependencies, like map() and filter(), are pipelined together into one set of tasks
 * in each stage, but operations with shuffle dependencies require multiple stages (one to write a
 * set of map output files, and another to read those files after a barrier). In the end, every
 * stage will have only shuffle dependencies on other stages, and may compute multiple operations
 * inside it. The actual pipelining of these operations happens in the RDD.compute() functions of
 * various RDDs
```

**关键原因：**

**Shuffle 操作的特点：**
- 需要**等待前一个 Stage 的所有任务完成**
- 需要**物化中间结果**（写入磁盘）
- 需要**网络传输**数据
- 是**同步点**（Barrier）

**示例：**
```
RDD1.map().filter() → RDD2.reduceByKey() → RDD3.collect()
     ↑                    ↑
  窄依赖（Pipeline）    Shuffle（Barrier）
```

**执行流程：**
```
Stage 0: map + filter (可以 Pipeline)
  ↓ 等待 Stage 0 完成
Shuffle: 写入磁盘，网络传输
  ↓
Stage 1: reduceByKey (需要读取 Shuffle 输出)
  ↓ 等待 Stage 1 完成
Stage 2: collect (结果 Stage)
```

---

#### 2. **Narrow 依赖可以 Pipeline（流水线）**

```47:50:core/src/main/scala/org/apache/spark/Dependency.scala
/**
 * :: DeveloperApi ::
 * Base class for dependencies where each partition of the child RDD depends on a small number
 * of partitions of the parent RDD. Narrow dependencies allow for pipelined execution.
 */
```

**Narrow 依赖的特点：**
- 子 RDD 的每个分区只依赖父 RDD 的**少量分区**
- 可以**流水线执行**，不需要等待
- 可以放在**同一个 Stage**

**示例：**
```
RDD1.map().filter().map()
  ↑    ↑      ↑
  └────┴──────┘
   同一个 Stage
   
原因：map 和 filter 是窄依赖
- 可以连续执行
- 不需要物化中间结果
- 内存中直接传递数据
```

---

#### 3. **Shuffle 依赖需要物化**

**Shuffle 操作需要：**
- **写入磁盘**：Map 端写入 Shuffle 文件
- **网络传输**：Reduce 端从不同节点读取
- **等待完成**：必须等待所有 Map 任务完成

**因此必须：**
- 在 Shuffle 前创建一个 Stage（ShuffleMapStage）
- 在 Shuffle 后创建另一个 Stage（ResultStage 或下一个 ShuffleMapStage）

---

### Stage 的作用

#### 1. **依赖管理**

```51:51:core/src/main/scala/org/apache/spark/scheduler/Stage.scala
 * @param parents List of stages that this stage depends on (through shuffle dependencies).
```

- 管理 Stage 之间的依赖关系
- 确保父 Stage 完成后再执行子 Stage
- 支持 Stage 级别的故障恢复

---

#### 2. **并行优化**

- 同一个 Stage 内的任务可以**并行执行**
- 不同 Stage 之间**串行执行**（按依赖顺序）
- 最大化并行度，同时保证正确性

---

#### 3. **故障恢复粒度**

```74:79:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
 * In addition to coming up with a DAG of stages, the DAGScheduler also determines the preferred
 * locations to run each task on, based on the current cache status, and passes these to the
 * low-level TaskScheduler. Furthermore, it handles failures due to shuffle output files being
 * lost, in which case old stages may need to be resubmitted. Failures *within* a stage that are
 * not caused by shuffle file loss are handled by the TaskScheduler, which will retry each task
 * a small number of times before cancelling the whole stage.
```

- **Stage 级别故障**：Shuffle 输出丢失 → 重新提交整个 Stage
- **Task 级别故障**：单个任务失败 → TaskScheduler 重试任务

---

#### 4. **缓存和重用**

```96:98:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
 *  - Cache tracking: the DAGScheduler figures out which RDDs are cached to avoid recomputing them
 *    and likewise remembers which shuffle map stages have already produced output files to avoid
 *    redoing the map side of a shuffle.
```

- 跟踪哪些 Stage 的输出已缓存
- 避免重复计算
- 支持 Stage 在多个 Job 间共享

---

## 二、TaskSet（任务集）

### 定义和作用

```25:28:core/src/main/scala/org/apache/spark/scheduler/TaskSet.scala
/**
 * A set of tasks submitted together to the low-level TaskScheduler, usually representing
 * missing partitions of a particular stage.
 */
```

**核心特点：**
- **一个 Stage 的一个 Attempt = 一个 TaskSet**
- 包含该 Attempt 中**所有缺失分区的任务**
- 作为**原子单元**提交给 TaskScheduler

---

### 为什么需要 TaskSet？

#### 1. **Attempt 管理**

```42:44:core/src/main/scala/org/apache/spark/scheduler/Stage.scala
 * Finally, a single stage can be re-executed in multiple attempts due to fault recovery. In that
 * case, the Stage object will track multiple StageInfo objects to pass to listeners or the web UI.
 * The latest one will be accessible through latestInfo.
```

**原因：**
- 一个 Stage 可能执行多次（重试）
- 每次执行是一个 **Attempt**
- 每个 Attempt 需要独立的 TaskSet

**示例：**
```
Stage 0, Attempt 0 → TaskSet "0.0" (100 个任务)
  ↓ 部分失败
Stage 0, Attempt 1 → TaskSet "0.1" (10 个任务，只重试失败的)
```

---

#### 2. **原子性保证**

**同一个 TaskSet 中的任务：**
- 属于同一个 Stage
- 属于同一个 Attempt
- 使用相同的任务二进制（taskBinary）
- 共享相同的配置和属性

**好处：**
- 如果 Stage 失败，可以整体重试
- 任务之间的一致性得到保证
- 简化故障恢复逻辑

---

#### 3. **批量调度**

```62:64:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
 * TaskScheduler implementation that runs them on the cluster. A TaskSet contains fully independent
 * tasks that can run right away based on the data that's already on the cluster (e.g. map output
 * files from previous stages), though it may fail if this data becomes unavailable.
```

**好处：**
- **批量提交**：一次提交多个任务，减少调度开销
- **统一管理**：TaskScheduler 可以统一分配资源
- **提高效率**：减少调度器调用次数

---

#### 4. **资源管理**

**TaskSet 级别：**
- 统一管理资源需求（ResourceProfile）
- 统一处理任务本地性
- 统一进行推测执行

---

## 三、Task（任务）

### 定义和作用

```94:94:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
 *  - Tasks are individual units of work, each sent to one machine.
```

**核心特点：**
- **单个分区的计算单元**
- 发送到**一个 Executor** 执行
- 是**最小的执行单元**

---

### 为什么需要 Task？

#### 1. **并行执行的基本单位**

**每个 Task：**
- 处理一个分区
- 在单个 Executor 上执行
- 可以并行执行（不同分区）

**示例：**
```
Stage 有 100 个分区
  ↓
创建 100 个 Task
  ↓
分配到不同的 Executor 并行执行
```

---

#### 2. **数据本地性**

```100:101:core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
 *  - Preferred locations: the DAGScheduler also computes where to run each task in a stage based
 *    on the preferred locations of its underlying RDDs, or the location of cached or shuffle data.
```

**每个 Task：**
- 有**偏好位置**（Preferred Locations）
- 可以调度到数据所在的节点
- 减少网络传输

**本地性级别：**
- PROCESS_LOCAL：数据在同一进程
- NODE_LOCAL：数据在同一节点
- RACK_LOCAL：数据在同一机架
- ANY：任意位置

---

#### 3. **故障恢复粒度**

**Task 级别故障：**
- 单个任务失败 → 重试该任务
- 不影响其他任务
- TaskScheduler 负责重试

**Stage 级别故障：**
- Shuffle 输出丢失 → 重新提交整个 Stage
- DAGScheduler 负责处理

---

#### 4. **资源分配**

**每个 Task：**
- 需要 CPU 资源
- 需要内存资源
- 可以指定资源需求

---

## 四、为什么需要这样的层次结构？

### 1. **职责分离**

```
DAGScheduler (逻辑层面)
  ↓ 管理 Stage 依赖
  ↓ 决定 Stage 执行顺序
  ↓ 处理 Stage 级别故障
  
TaskScheduler (物理层面)
  ↓ 管理 TaskSet 调度
  ↓ 分配资源给 Task
  ↓ 处理 Task 级别故障
```

**好处：**
- 逻辑和物理分离
- 各司其职，易于维护
- 可以独立优化

---

### 2. **故障恢复层次**

```
Job 失败
  ↓
Stage 失败 (Shuffle 输出丢失)
  ↓ 重新提交 Stage
  ↓ 创建新的 TaskSet
  ↓
Task 失败 (执行错误)
  ↓ TaskScheduler 重试
  ↓ 最多重试几次
  ↓ 如果还失败，导致 Stage 失败
```

**不同层次的故障：**
- **Task 级别**：快速重试，不影响其他任务
- **Stage 级别**：需要重新计算，但可以重用已完成的部分
- **Job 级别**：整个作业失败

---

### 3. **调度效率**

**层次化调度：**
- **Stage 级别**：按依赖关系顺序执行
- **TaskSet 级别**：批量提交，减少开销
- **Task 级别**：细粒度资源分配

**好处：**
- 最大化并行度
- 最小化调度开销
- 优化资源利用

---

### 4. **可扩展性**

**层次结构支持：**
- **动态资源分配**：TaskSet 级别管理资源
- **推测执行**：TaskSet 级别管理慢任务
- **多 Job 调度**：Stage 可以在多个 Job 间共享

---

## 五、实际执行流程

### 完整示例

```
用户代码：
rdd.map().filter().reduceByKey().collect()

RDD 依赖图：
RDD1 → map → RDD2 → filter → RDD3 → reduceByKey → RDD4 → collect
  ↑      ↑      ↑       ↑       ↑         ↑
  └──────┴──────┴───────┴───────┴─────────┘
     窄依赖（Pipeline）        Shuffle（Barrier）

Stage 划分：
Stage 0 (ShuffleMapStage):
  - RDD1.map().filter()
  - 100 个分区
  ↓
TaskSet "0.0":
  - 100 个 ShuffleMapTask
  - 每个任务处理一个分区
  ↓
Shuffle:
  - 写入磁盘
  - 网络传输
  ↓
Stage 1 (ResultStage):
  - RDD3.reduceByKey().collect()
  - 10 个分区
  ↓
TaskSet "1.0":
  - 10 个 ResultTask
  - 每个任务处理一个分区
```

---

## 六、设计优势总结

### 1. **性能优化**

✅ **Pipeline 执行**：窄依赖操作流水线执行，减少物化开销  
✅ **并行最大化**：同一 Stage 内的任务并行执行  
✅ **本地性优化**：Task 可以调度到数据所在节点  
✅ **批量调度**：TaskSet 批量提交，减少调度开销  

---

### 2. **容错能力**

✅ **细粒度恢复**：Task 级别快速重试  
✅ **粗粒度恢复**：Stage 级别重新计算  
✅ **部分重用**：已完成的分区不需要重新计算  
✅ **状态管理**：清晰的故障恢复层次  

---

### 3. **资源管理**

✅ **统一配置**：TaskSet 级别统一资源需求  
✅ **灵活分配**：Task 级别细粒度资源分配  
✅ **动态调整**：支持动态资源分配和推测执行  

---

### 4. **可维护性**

✅ **职责清晰**：每个层次有明确的职责  
✅ **易于调试**：层次结构便于问题定位  
✅ **易于扩展**：可以独立优化每个层次  

---

## 七、关键设计思想

### 1. **Shuffle 是 Barrier**

**核心思想：**
- Shuffle 操作是**同步点**
- 必须等待前一个 Stage 完成
- 因此必须在 Shuffle 边界划分 Stage

**好处：**
- 保证数据一致性
- 简化依赖管理
- 支持故障恢复

---

### 2. **Pipeline 窄依赖**

**核心思想：**
- 窄依赖可以**流水线执行**
- 不需要物化中间结果
- 可以放在同一个 Stage

**好处：**
- 减少 I/O 开销
- 提高执行效率
- 减少内存占用

---

### 3. **层次化故障恢复**

**核心思想：**
- 不同层次的故障由不同组件处理
- Task 级别快速重试
- Stage 级别重新计算

**好处：**
- 快速恢复
- 最小化重算
- 提高容错能力

---

### 4. **批量调度**

**核心思想：**
- TaskSet 批量提交
- 减少调度开销
- 提高调度效率

**好处：**
- 减少网络通信
- 提高吞吐量
- 优化资源利用

---

## 总结

### 核心原则

1. **Stage 在 Shuffle 边界划分**
   - Shuffle 是 Barrier，需要等待
   - 窄依赖可以 Pipeline，放在同一 Stage

2. **TaskSet 是 Stage 的一个 Attempt**
   - 管理重试和故障恢复
   - 保证任务原子性

3. **Task 是单个分区的计算单元**
   - 并行执行的基本单位
   - 支持数据本地性

### 设计优势

✅ **性能**：Pipeline 执行，最大化并行度  
✅ **容错**：层次化故障恢复  
✅ **效率**：批量调度，减少开销  
✅ **可维护**：职责清晰，易于扩展  

**这种层次结构是 Spark 高性能、高容错的核心设计！**

