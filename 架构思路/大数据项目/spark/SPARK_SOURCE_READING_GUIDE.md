# Spark 源码阅读指南

## 目录

1. [整体架构概览](#整体架构概览)
2. [阅读顺序建议](#阅读顺序建议)
3. [核心模块详解](#核心模块详解)
4. [执行流程追踪](#执行流程追踪)
5. [学习路径](#学习路径)

---

## 整体架构概览

Apache Spark 是一个统一的大规模数据处理分析引擎，主要包含以下核心模块：

### 模块结构

```
spark/
├── common/              # 公共基础模块
│   ├── kvstore/        # 键值存储
│   ├── network-common/  # 网络通信基础
│   ├── network-shuffle/ # Shuffle 网络层
│   ├── unsafe/          # 内存管理（Unsafe）
│   └── utils/           # 工具类
├── core/                # 核心引擎 ⭐
│   ├── rdd/            # RDD 抽象和实现
│   ├── scheduler/      # 调度器（DAG、Task）
│   ├── storage/        # 存储管理
│   ├── shuffle/        # Shuffle 实现
│   └── executor/       # Executor 实现
├── sql/                 # Spark SQL ⭐
│   ├── catalyst/       # 查询优化器
│   ├── core/           # SQL 核心实现
│   └── connect/        # Spark Connect
├── streaming/           # 流处理
├── mllib/              # 机器学习库
├── graphx/             # 图计算
└── resource-managers/  # 资源管理器（YARN、K8s）
```

---

## 阅读顺序建议

### 阶段一：基础理解（1-2周）

#### 1. 从入口开始：SparkContext

**文件位置：** `core/src/main/scala/org/apache/spark/SparkContext.scala`

**为什么从这里开始：**

- SparkContext 是 Spark 应用的入口点
- 理解 Spark 应用的初始化流程
- 了解核心组件的创建和配置

**重点阅读：**

- 构造函数：了解 SparkContext 如何初始化
- `runJob` 方法：理解作业提交的入口
- 核心组件初始化：DAGScheduler、TaskScheduler、SparkEnv

**关键代码：**

```scala
// 核心初始化流程
private[spark] val dagScheduler = new DAGScheduler(this)
taskScheduler = createTaskScheduler()
taskScheduler.start()
dagScheduler.start()
```

#### 2. 配置系统：SparkConf

**文件位置：** `core/src/main/scala/org/apache/spark/SparkConf.scala`

**学习目标：**

- 理解 Spark 配置的加载和优先级
- 掌握常用配置项

#### 3. 环境管理：SparkEnv

**文件位置：** `core/src/main/scala/org/apache/spark/SparkEnv.scala`

**学习目标：**

- 理解 Spark 运行环境的构建
- 了解各个组件的创建和依赖关系

---

### 阶段二：RDD 核心（2-3周）

#### 4. RDD 抽象：理解弹性分布式数据集

**文件位置：** `core/src/main/scala/org/apache/spark/rdd/RDD.scala`

**为什么重要：**

- RDD 是 Spark 的核心抽象
- 理解 RDD 的五大特性
- 掌握 RDD 的转换和行动操作

**重点阅读：**

- `compute()` 方法：理解分区计算逻辑
- `dependencies`：理解依赖关系（窄依赖 vs 宽依赖）
- `partitions`：理解分区机制
- `persist()` / `cache()`：理解持久化机制

**关键概念：**

- 窄依赖（Narrow Dependency）
- 宽依赖（Wide Dependency / Shuffle Dependency）
- 分区（Partition）
- 检查点（Checkpoint）

#### 5. 常见 RDD 实现

**文件位置：** `core/src/main/scala/org/apache/spark/rdd/`

**推荐阅读顺序：**

1. `ParallelCollectionRDD.scala` - 最简单的 RDD 实现
2. `MapPartitionsRDD.scala` - 理解 map 操作的实现
3. `HadoopRDD.scala` - 理解如何读取 HDFS 数据
4. `ShuffledRDD.scala` - 理解 Shuffle 操作
5. `CoGroupedRDD.scala` - 理解 Join 操作

#### 6. RDD 操作实现

**文件位置：** `core/src/main/scala/org/apache/spark/rdd/`

**重点文件：**

- `PairRDDFunctions.scala` - 键值对 RDD 操作
- `DoubleRDDFunctions.scala` - 数值 RDD 操作

---

### 阶段三：调度系统（3-4周）

#### 7. DAG 调度器：DAGScheduler

**文件位置：** `core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala`

**为什么重要：**

- DAGScheduler 负责将 RDD 图转换为执行计划
- 理解 Stage 的划分逻辑
- 理解任务调度策略

**重点阅读：**

- `submitJob()` - 作业提交入口
- `submitStage()` - Stage 提交逻辑
- `createResultStage()` / `getOrCreateShuffleMapStage()` - Stage 创建
- `handleTaskCompletion()` - 任务完成处理
- `handleJobSubmitted()` - 作业提交处理

**关键概念：**

- Job（作业）
- Stage（阶段）：ResultStage、ShuffleMapStage
- Task（任务）：ResultTask、ShuffleMapTask
- 依赖关系处理

#### 8. Task 调度器：TaskScheduler

**文件位置：** `core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala`

**学习目标：**

- 理解任务如何被调度到 Executor
- 理解调度算法（FIFO、Fair）
- 理解任务本地性（Locality）

**重点阅读：**

- `submitTasks()` - 任务集提交
- `resourceOffers()` - 资源分配
- `TaskSetManager` - 任务集管理

#### 9. Stage 和 Task

**文件位置：** `core/src/main/scala/org/apache/spark/scheduler/`

**重点文件：**

- `Stage.scala` - Stage 抽象
- `ResultStage.scala` - 结果 Stage
- `ShuffleMapStage.scala` - Shuffle Map Stage
- `Task.scala` - Task 抽象
- `ResultTask.scala` - 结果任务
- `ShuffleMapTask.scala` - Shuffle Map 任务

---

### 阶段四：存储和 Shuffle（2-3周）

#### 10. 存储系统：BlockManager

**文件位置：** `core/src/main/scala/org/apache/spark/storage/BlockManager.scala`

**学习目标：**

- 理解 Spark 的存储架构
- 理解内存和磁盘存储策略
- 理解 Block 的管理

**重点阅读：**

- `putBlockData()` - 存储数据块
- `getBlockData()` - 获取数据块
- `MemoryStore` / `DiskStore` - 存储实现

#### 11. Shuffle 机制

**文件位置：** `core/src/main/scala/org/apache/spark/shuffle/`

**为什么重要：**

- Shuffle 是 Spark 性能的关键
- 理解 Shuffle 的读写过程

**重点文件：**

- `ShuffleManager.scala` - Shuffle 管理器接口
- `SortShuffleManager.scala` - 排序 Shuffle 实现
- `ShuffleWriter.scala` - Shuffle 写入
- `ShuffleReader.scala` - Shuffle 读取

**相关文件：**

- `common/network-shuffle/` - Shuffle 网络层

#### 12. 内存管理

**文件位置：** `core/src/main/scala/org/apache/spark/memory/`

**重点文件：**

- `MemoryManager.scala` - 内存管理器
- `UnifiedMemoryManager.scala` - 统一内存管理
- `common/unsafe/` - Unsafe 内存操作

---

### 阶段五：Executor 执行（2周）

#### 13. Executor 实现

**文件位置：** `core/src/main/scala/org/apache/spark/executor/Executor.scala`

**学习目标：**

- 理解 Executor 如何执行任务
- 理解任务执行的完整流程

**重点阅读：**

- `launchTask()` - 启动任务
- `run()` - 任务运行逻辑
- `TaskRunner` - 任务运行器

#### 14. 任务执行上下文

**文件位置：** `core/src/main/scala/org/apache/spark/`

**重点文件：**

- `TaskContext.scala` - 任务上下文
- `TaskContextImpl.scala` - 任务上下文实现
- `PartitionEvaluator.scala` - 分区评估器

---

### 阶段六：Spark SQL（4-5周）

#### 15. SQL 入口：SparkSession

**文件位置：** `sql/core/src/main/scala/org/apache/spark/sql/SparkSession.scala`

**学习目标：**

- 理解 Spark SQL 的入口
- 理解 DataFrame/Dataset API

#### 16. Catalyst 优化器

**文件位置：** `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/`

**为什么重要：**

- Catalyst 是 Spark SQL 的核心优化器
- 理解查询优化流程

**推荐阅读顺序：**

1. `parser/` - SQL 解析
2. `analysis/` - 逻辑分析
3. `optimizer/` - 优化规则
4. `planner/` - 物理计划生成
5. `execution/` - 执行计划

**重点文件：**

- `SparkSqlParser.scala` - SQL 解析器
- `Analyzer.scala` - 分析器
- `Optimizer.scala` - 优化器
- `SparkPlanner.scala` - 物理计划器

#### 17. SQL 执行引擎

**文件位置：** `sql/core/src/main/scala/org/apache/spark/sql/execution/`

**重点文件：**

- `SparkPlan.scala` - 执行计划抽象
- `WholeStageCodegenExec.scala` - 全阶段代码生成
- `ProjectExec.scala` - 投影执行
- `FilterExec.scala` - 过滤执行
- `SortExec.scala` - 排序执行

---

### 阶段七：高级主题（按需）

#### 18. 流处理：Structured Streaming

**文件位置：** `sql/core/src/main/scala/org/apache/spark/sql/streaming/`

#### 19. 资源管理

**文件位置：** `resource-managers/`

#### 20. 网络通信

**文件位置：** `common/network-common/`

---

## 核心模块详解

### 1. Core 模块结构

```
core/src/main/scala/org/apache/spark/
├── SparkContext.scala          # 应用入口
├── SparkConf.scala             # 配置管理
├── SparkEnv.scala              # 运行环境
├── rdd/                        # RDD 实现
│   ├── RDD.scala              # RDD 抽象
│   ├── ParallelCollectionRDD.scala
│   ├── MapPartitionsRDD.scala
│   ├── HadoopRDD.scala
│   └── ShuffledRDD.scala
├── scheduler/                  # 调度系统
│   ├── DAGScheduler.scala     # DAG 调度器
│   ├── TaskSchedulerImpl.scala # Task 调度器
│   ├── Stage.scala
│   └── Task.scala
├── storage/                    # 存储系统
│   ├── BlockManager.scala
│   └── MemoryStore.scala
├── shuffle/                    # Shuffle
│   └── SortShuffleManager.scala
├── executor/                   # Executor
│   └── Executor.scala
└── memory/                     # 内存管理
    └── MemoryManager.scala
```

### 2. SQL 模块结构

```
sql/
├── catalyst/                   # 查询优化器
│   ├── parser/                # SQL 解析
│   ├── analysis/              # 逻辑分析
│   ├── optimizer/            # 优化规则
│   ├── planner/              # 物理计划
│   └── execution/            # 执行
├── core/                      # SQL 核心
│   ├── SparkSession.scala
│   └── execution/
└── connect/                   # Spark Connect
```

---

## 执行流程追踪

### 完整执行流程

```
用户代码
  ↓
RDD Action (如 collect(), count())
  ↓
SparkContext.runJob()
  ↓
DAGScheduler.runJob()
  ↓
DAGScheduler.submitJob()
  ↓
DAGScheduler.handleJobSubmitted()
  ↓
创建 ResultStage
  ↓
DAGScheduler.submitStage()
  ↓
递归提交父 Stage
  ↓
TaskScheduler.submitTasks()
  ↓
TaskSchedulerImpl.resourceOffers()
  ↓
分配任务到 Executor
  ↓
Executor.launchTask()
  ↓
TaskRunner.run()
  ↓
执行任务代码
  ↓
返回结果
```

### 关键代码追踪路径

#### 1. 作业提交路径

```
SparkContext.runJob()
  → DAGScheduler.runJob()
    → DAGScheduler.submitJob()
      → eventProcessLoop.post(JobSubmitted)
        → DAGScheduler.handleJobSubmitted()
```

#### 2. Stage 创建路径

```
DAGScheduler.handleJobSubmitted()
  → createResultStage()
    → getOrCreateShuffleMapStage()
      → 递归创建父 Stage
```

#### 3. 任务提交路径

```
DAGScheduler.submitStage()
  → submitMissingTasks()
    → TaskScheduler.submitTasks()
      → TaskSchedulerImpl.submitTasks()
```

#### 4. 任务执行路径

```
Executor.launchTask()
  → new TaskRunner()
    → TaskRunner.run()
      → task.run()
        → RDD.compute()
```

---

## 学习路径

### 初学者路径（3-4个月）

**第1-2周：环境准备**

- 搭建 Spark 开发环境
- 阅读 Spark 官方文档
- 运行示例程序

**第3-4周：入口理解**

- 深入阅读 `SparkContext.scala`
- 理解配置系统 `SparkConf.scala`
- 理解环境构建 `SparkEnv.scala`

**第5-7周：RDD 核心**

- 深入理解 `RDD.scala`
- 阅读常见 RDD 实现
- 理解依赖关系

**第8-11周：调度系统**

- 深入理解 `DAGScheduler.scala`
- 理解 `TaskSchedulerImpl.scala`
- 理解 Stage 和 Task

**第12-14周：存储和 Shuffle**

- 理解 `BlockManager.scala`
- 理解 Shuffle 机制
- 理解内存管理

**第15-16周：Executor**

- 理解 `Executor.scala`
- 理解任务执行流程

### 进阶路径（2-3个月）

**第1-2周：Spark SQL 基础**

- 理解 `SparkSession`
- 理解 DataFrame/Dataset API

**第3-5周：Catalyst 优化器**

- 理解 SQL 解析
- 理解逻辑分析和优化
- 理解物理计划生成

**第6-7周：SQL 执行引擎**

- 理解执行计划
- 理解代码生成
- 理解各种执行算子

**第8周：流处理**

- 理解 Structured Streaming
- 理解流处理执行

### 专家路径（按需）

- 网络通信机制
- 资源管理器（YARN、K8s）
- 机器学习库（MLlib）
- 图计算（GraphX）

---

## 阅读技巧

### 1. 使用 IDE

- 推荐使用 IntelliJ IDEA
- 配置 Scala 插件
- 使用 "Go to Definition" 追踪调用链

### 2. 调试技巧

- 在关键方法设置断点
- 使用 Spark 本地模式调试
- 查看日志输出

### 3. 测试用例

- 阅读对应的测试用例
- 理解功能的使用场景
- 测试用例是最好的文档

### 4. 文档和注释

- 阅读代码注释
- 查看官方文档
- 阅读设计文档（如果有）

### 5. 画图理解

- 画出执行流程图
- 画出类图
- 画出数据流图

---

## 推荐阅读顺序总结

### 快速入门（1周）

1. `SparkContext.scala` - 理解入口
2. `RDD.scala` - 理解核心抽象
3. `DAGScheduler.scala` - 理解调度

### 深入理解（1个月）

1. RDD 实现（`rdd/` 目录）
2. 调度系统（`scheduler/` 目录）
3. 存储系统（`storage/` 目录）
4. Shuffle（`shuffle/` 目录）

### 全面掌握（3个月）

1. Core 模块全部
2. SQL 模块（Catalyst + 执行引擎）
3. 流处理
4. 资源管理

---

## 关键文件索引

### 必读文件（Core）

- `core/src/main/scala/org/apache/spark/SparkContext.scala`
- `core/src/main/scala/org/apache/spark/rdd/RDD.scala`
- `core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala`
- `core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala`
- `core/src/main/scala/org/apache/spark/executor/Executor.scala`
- `core/src/main/scala/org/apache/spark/storage/BlockManager.scala`

### 必读文件（SQL）

- `sql/core/src/main/scala/org/apache/spark/sql/SparkSession.scala`
- `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/Analyzer.scala`
- `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala`
- `sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlan.scala`

---

## 学习资源

1. **官方文档**
   
   - https://spark.apache.org/docs/latest/
   - https://spark.apache.org/developer-tools.html

2. **源码位置**
   
   - GitHub: https://github.com/apache/spark

3. **设计文档**
   
   - Spark 论文：Resilient Distributed Datasets
   - Catalyst 论文：Spark SQL: Relational Data Processing in Spark

4. **社区资源**
   
   - Spark 邮件列表
   - Stack Overflow
   - Spark Summit 视频

---

## 注意事项

1. **版本差异**：不同版本的 Spark 代码可能有差异，注意版本号
2. **Scala 语言**：Spark 主要用 Scala 编写，需要熟悉 Scala
3. **并发编程**：理解 Actor 模型、Future、Promise 等
4. **分布式系统**：理解分布式系统的基本概念
5. **耐心**：Spark 代码量很大，需要耐心逐步理解

---

**祝学习顺利！** 🚀
