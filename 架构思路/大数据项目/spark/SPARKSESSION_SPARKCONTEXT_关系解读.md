# SparkSession 和 SparkContext 关系解读

## 概述

`SparkSession` 和 `SparkContext` 是 Spark 中两个核心入口点，它们有着密切的关系，但服务于不同的目的。

### 核心关系

**SparkSession 内部持有 SparkContext，一个 SparkContext 可以被多个 SparkSession 共享。**

---

## 1. 基本定义

### SparkContext

- **作用**：Spark 应用的**核心入口点**，代表与 Spark 集群的连接
- **职责**：
  - 创建 RDD
  - 管理作业提交和执行
  - 协调调度器、存储、网络等核心组件
- **特点**：每个 JVM 只能有一个活跃的 SparkContext

### SparkSession

- **作用**：Spark SQL 的**统一入口点**，提供 Dataset/DataFrame API
- **职责**：
  - 创建 DataFrame 和 Dataset
  - 执行 SQL 查询
  - 管理临时视图、UDF、配置等
- **特点**：可以创建多个 SparkSession，共享同一个 SparkContext

---

## 2. 代码层面的关系

### SparkSession 持有 SparkContext

```92:93:sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala
class SparkSession private(
    @transient val sparkContext: SparkContext,
```

**关键点：**

- `SparkSession` 的构造函数接收一个 `SparkContext` 作为参数
- `sparkContext` 是 `SparkSession` 的一个**成员变量**（`val`，不可变）
- 使用 `@transient` 标记，因为 SparkContext 不可序列化

### SparkSession 依赖 SparkContext 的状态

```889:889:sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala
  override private[sql] def isUsable: Boolean = !sparkContext.isStopped
```

**说明：**

- SparkSession 的可用性取决于 SparkContext 是否已停止
- 如果 SparkContext 停止，SparkSession 也不可用

### 关闭时调用 SparkContext.stop()

```828:830:sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala
  override def close(): Unit = {
    sparkContext.stop()
  }
```

**说明：**

- 关闭 SparkSession 会同时停止 SparkContext
- 这会影响所有共享该 SparkContext 的 SparkSession

---

## 3. 创建流程

### SparkSession.builder().getOrCreate() 的流程

```981:1033:sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala
    private def build(forceCreate: Boolean): SparkSession = synchronized {
      val sparkConf = new SparkConf()
      options.foreach { case (k, v) => sparkConf.set(k, v) }

      if (!sparkConf.get(EXECUTOR_ALLOW_SPARK_CONTEXT)) {
        assertOnDriver()
      }

      // Get the session from current thread's active session.
      val active = getActiveSession
      if (!forceCreate && active.isDefined) {
        val session = active.get
        applyModifiableSettings(session, new java.util.HashMap[String, String](options.asJava))
        return session
      }

      // Global synchronization so we will only set the default session once.
      SparkSession.synchronized {
        // If the current thread does not have an active session, get it from the global session.
        val default = getDefaultSession
        if (!forceCreate && default.isDefined) {
          val session = default.get
          applyModifiableSettings(session, new java.util.HashMap[String, String](options.asJava))
          return session
        }

        // No active nor global default session. Create a new one.
        val sparkContext = userSuppliedContext.getOrElse {
          // Override appName with the submitted appName
          sparkConf.getOption("spark.submit.appName")
            .map(sparkConf.setAppName)
          // set a random app name if not given.
          if (!sparkConf.contains("spark.app.name")) {
            sparkConf.setAppName(java.util.UUID.randomUUID().toString)
          }

          SparkContext.getOrCreate(sparkConf)
          // Do not update `SparkConf` for existing `SparkContext`, as it's shared by all sessions.
        }

        loadExtensions(extensions)
        applyExtensions(sparkContext, extensions)

        val session = new SparkSession(sparkContext,
          existingSharedState = None,
          parentSessionState = None,
          extensions,
          initialSessionOptions = options.toMap,
          parentManagedJobTags = Map.empty)
        setDefaultAndActiveSession(session)
        registerContextListener(sparkContext)
        session
      }
    }
```

**创建流程：**

1. **检查现有 Session**
   
   - 先检查当前线程是否有活跃的 SparkSession
   - 再检查全局默认 SparkSession
   - 如果存在且不需要强制创建，直接返回

2. **获取或创建 SparkContext**
   
   ```scala
   val sparkContext = userSuppliedContext.getOrElse {
     SparkContext.getOrCreate(sparkConf)
   }
   ```
   
   - 如果用户提供了 SparkContext，使用它
   - 否则调用 `SparkContext.getOrCreate()` 获取或创建
   - **关键**：`SparkContext.getOrCreate()` 是单例模式，如果已存在就复用

3. **创建 SparkSession**
   
   - 使用获取到的 SparkContext 创建新的 SparkSession
   - 多个 SparkSession 可以共享同一个 SparkContext

---

## 4. 状态管理

### SharedState（共享状态）

```175:177:sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala
  lazy val sharedState: SharedState = {
    existingSharedState.getOrElse(new SharedState(sparkContext, initialSessionOptions))
  }
```

**SharedState 包含：**

- SparkContext（共享）
- 全局 Catalog（表、数据库）
- 缓存管理器（CacheManager）
- 外部数据源注册
- SQL App 状态存储

**特点：**

- 多个 SparkSession 可以共享同一个 SharedState
- 通过 `existingSharedState` 参数实现共享

### SessionState（会话状态）

```182:191:sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala
  lazy val sessionState: SessionState = {
    parentSessionState
      .map(_.clone(this))
      .getOrElse {
        val state = SparkSession.instantiateSessionState(
          SparkSession.sessionStateClassName(sharedState.conf),
          self)
        state
      }
  }
```

**SessionState 包含：**

- SQL 配置（SQLConf）
- 临时视图（Temporary Views）
- 注册的 UDF
- 流查询管理器
- 执行监听器
- 会话级别的 Catalog

**特点：**

- 每个 SparkSession 有独立的 SessionState
- 可以继承父 Session 的状态（通过 `parentSessionState`）

---

## 5. 多 Session 共享 Context

### 创建新 Session（共享 SparkContext）

```239:247:sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala
  def newSession(): SparkSession = {
    new SparkSession(
      sparkContext,
      Some(sharedState),
      parentSessionState = None,
      extensions,
      initialSessionOptions,
      parentManagedJobTags = Map.empty)
  }
```

**说明：**

- `newSession()` 创建新的 SparkSession
- **共享**同一个 `sparkContext`
- **共享**同一个 `sharedState`
- **独立**的 `sessionState`（临时视图、UDF 等）

### 克隆 Session（完全复制）

```261:273:sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala
  private[sql] def cloneSession(): SparkSession = {
    val result = new SparkSession(
      sparkContext,
      Some(sharedState),
      Some(sessionState),
      extensions,
      Map.empty,
      managedJobTags.get().toMap)
    result.sessionState // force copy of SessionState
    result.sessionState.artifactManager // force copy of ArtifactManager and its resources
    result.managedJobTags // force copy of managedJobTags
    result
  }
```

**说明：**

- `cloneSession()` 创建完全相同的副本
- 共享 SparkContext 和 SharedState
- **复制** SessionState（包括配置、临时视图、UDF 等）

---

## 6. 架构层次关系

```
┌─────────────────────────────────────────┐
│         Spark Application               │
├─────────────────────────────────────────┤
│                                         │
│  ┌──────────────────────────────────┐  │
│  │      SparkContext (单例)          │  │
│  │  - DAGScheduler                   │  │
│  │  - TaskScheduler                  │  │
│  │  - BlockManager                   │  │
│  │  - SparkEnv                        │  │
│  └──────────────────────────────────┘  │
│              ▲                          │
│              │                          │
│  ┌───────────┴───────────┐              │
│  │                       │              │
│  │  ┌──────────────┐    ┌──────────────┐│
│  │  │ SparkSession │    │ SparkSession ││
│  │  │   (Session1) │    │   (Session2) ││
│  │  └──────────────┘    └──────────────┘│
│  │         │                    │       │
│  │  ┌──────┴──────┐    ┌───────┴──────┐│
│  │  │ SharedState │    │ SharedState  ││
│  │  │  (共享)     │    │   (共享)     ││
│  │  └─────────────┘    └──────────────┘│
│  │         │                    │       │
│  │  ┌──────┴──────┐    ┌───────┴──────┐│
│  │  │SessionState │    │SessionState  ││
│  │  │  (独立)     │    │   (独立)     ││
│  │  └─────────────┘    └──────────────┘│
│  │                                     │
│  └─────────────────────────────────────┘
│                                         │
└─────────────────────────────────────────┘
```

**层次说明：**

1. **SparkContext**：最底层，单例，管理核心资源
2. **SharedState**：中间层，多个 Session 共享（Catalog、缓存等）
3. **SessionState**：最上层，每个 Session 独立（配置、临时视图、UDF）

---

## 7. 使用场景对比

### 使用 SparkContext 的场景

```scala
// 创建 RDD
val sc = new SparkContext(conf)
val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5))
val result = rdd.map(_ * 2).collect()

// 广播变量
val broadcastVar = sc.broadcast(Array(1, 2, 3))

// 累加器
val accum = sc.longAccumulator("My Accumulator")
```

**适用场景：**

- 使用 RDD API
- 需要广播变量或累加器
- 底层控制需求

### 使用 SparkSession 的场景

```scala
// 创建 DataFrame
val spark = SparkSession.builder().getOrCreate()
val df = spark.read.json("path/to/json")

// 执行 SQL
df.createOrReplaceTempView("people")
val result = spark.sql("SELECT * FROM people WHERE age > 20")

// 使用 Dataset API
val ds = spark.createDataset(Seq(1, 2, 3, 4, 5))
```

**适用场景：**

- 使用 DataFrame/Dataset API
- 执行 SQL 查询
- 结构化数据处理
- 流处理（Structured Streaming）

---

## 8. 生命周期关系

### 创建顺序

```
1. SparkContext.getOrCreate()  // 创建或获取 SparkContext
   ↓
2. new SparkSession(sparkContext, ...)  // 使用 SparkContext 创建 SparkSession
   ↓
3. SharedState 初始化（延迟）
   ↓
4. SessionState 初始化（延迟）
```

### 关闭顺序

```
1. sparkSession.close() 或 sparkSession.stop()
   ↓
2. sparkContext.stop()  // 停止 SparkContext
   ↓
3. 所有共享该 SparkContext 的 SparkSession 都失效
```

**重要：**

- 关闭任何一个 SparkSession 都会停止 SparkContext
- 停止 SparkContext 会影响所有相关的 SparkSession

---

## 9. 配置管理

### SparkContext 配置

```scala
val conf = new SparkConf()
  .setMaster("local[*]")
  .setAppName("MyApp")
  .set("spark.executor.memory", "2g")

val sc = new SparkContext(conf)
```

**配置范围：**

- 集群配置（master、executor 内存等）
- 调度配置
- 存储配置
- 网络配置

### SparkSession 配置

```scala
val spark = SparkSession.builder()
  .appName("MyApp")
  .config("spark.sql.shuffle.partitions", "200")
  .config("spark.sql.adaptive.enabled", "true")
  .getOrCreate()
```

**配置范围：**

- SQL 相关配置（shuffle partitions、adaptive execution 等）
- 数据源配置
- 优化器配置
- 会话级别配置

**配置优先级：**

1. SparkSession 配置（最高优先级）
2. SparkContext 配置
3. 系统属性
4. 默认值

---

## 10. 实际使用建议

### 推荐方式：使用 SparkSession

```scala
// ✅ 推荐：使用 SparkSession（现代方式）
val spark = SparkSession.builder()
  .appName("MyApp")
  .master("local[*]")
  .getOrCreate()

// 可以访问 SparkContext
val sc = spark.sparkContext

// 使用 DataFrame API
val df = spark.read.json("data.json")

// 也可以使用 RDD API
val rdd = sc.parallelize(Seq(1, 2, 3))
```

**优势：**

- 统一入口，支持所有 API（RDD、DataFrame、Dataset、SQL）
- 自动管理 SparkContext
- 更好的配置管理
- 支持多 Session

### 传统方式：直接使用 SparkContext

```scala
// ⚠️ 传统方式：直接创建 SparkContext
val sc = new SparkContext(conf)

// 只能使用 RDD API
val rdd = sc.parallelize(Seq(1, 2, 3))
```

**限制：**

- 只能使用 RDD API
- 需要手动管理生命周期
- 不支持 SQL 和 DataFrame

---

## 11. 关键代码片段解析

### SparkSession 如何获取 SparkContext

```1008:1019:sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala
        val sparkContext = userSuppliedContext.getOrElse {
          // Override appName with the submitted appName
          sparkConf.getOption("spark.submit.appName")
            .map(sparkConf.setAppName)
          // set a random app name if not given.
          if (!sparkConf.contains("spark.app.name")) {
            sparkConf.setAppName(java.util.UUID.randomUUID().toString)
          }

          SparkContext.getOrCreate(sparkConf)
          // Do not update `SparkConf` for existing `SparkContext`, as it's shared by all sessions.
        }
```

**关键点：**

1. 优先使用用户提供的 SparkContext（`userSuppliedContext`）
2. 否则调用 `SparkContext.getOrCreate()` 获取或创建
3. **重要**：如果 SparkContext 已存在，不会更新其配置（因为被多个 Session 共享）

### 注册 Context 监听器

```1130:1141:sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala
  private def registerContextListener(sparkContext: SparkContext): Unit = {
    if (!listenerRegistered.get()) {
      sparkContext.addSparkListener(new SparkListener {
        override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
          clearDefaultSession()
          clearActiveSession()
          listenerRegistered.set(false)
        }
      })
      listenerRegistered.set(true)
    }
  }
```

**说明：**

- 当 SparkContext 停止时，自动清理 SparkSession
- 确保状态一致性

---

## 12. 总结

### 核心关系

| 特性     | SparkContext      | SparkSession               |
| ------ | ----------------- | -------------------------- |
| **数量** | 每个 JVM 一个（单例）     | 可以有多个                      |
| **关系** | 被 SparkSession 持有 | 持有 SparkContext            |
| **共享** | 可以被多个 Session 共享  | 每个 Session 独立              |
| **职责** | 核心引擎（调度、存储等）      | SQL/DataFrame API          |
| **状态** | 全局状态              | SharedState + SessionState |

### 设计模式

1. **组合模式**：SparkSession 包含 SparkContext
2. **单例模式**：SparkContext 是单例
3. **共享模式**：多个 SparkSession 共享 SparkContext 和 SharedState
4. **隔离模式**：每个 SparkSession 有独立的 SessionState

### 最佳实践

1. **优先使用 SparkSession**：统一入口，功能更全
2. **共享 SparkContext**：多个 Session 共享同一个 Context，节省资源
3. **独立 SessionState**：每个 Session 有独立的配置和临时视图
4. **正确关闭**：关闭 SparkSession 会停止 SparkContext，影响所有相关 Session

---

**关键理解：**

- **SparkContext 是引擎，SparkSession 是接口**
- **一个引擎可以服务多个接口**
- **接口关闭会关闭引擎，影响所有接口**
