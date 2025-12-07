# SparkContext.scala 源码解读

## 概述

`SparkContext` 是 Spark 应用的**主入口点**，代表与 Spark 集群的连接。它负责：
- 创建 RDD、累加器和广播变量
- 管理应用的生命周期
- 协调各个核心组件
- 提供作业提交和执行接口

**重要约束**：每个 JVM 只能有一个活跃的 `SparkContext`。

---

## 类结构

### 1. 类定义和基础属性

```86:131:core/src/main/scala/org/apache/spark/SparkContext.scala
class SparkContext(config: SparkConf) extends Logging {

  // The call site where this SparkContext was constructed.
  private val creationSite: CallSite = Utils.getCallSite()

  private var stopSite: Option[CallSite] = None

  if (!config.get(EXECUTOR_ALLOW_SPARK_CONTEXT)) {
    // In order to prevent SparkContext from being created in executors.
    SparkContext.assertOnDriver()
  }

  // In order to prevent multiple SparkContexts from being active at the same time, mark this
  // context as having started construction.
  // NOTE: this must be placed at the beginning of the SparkContext constructor.
  SparkContext.markPartiallyConstructed(this)

  val startTime = System.currentTimeMillis()

  private[spark] val stopped: AtomicBoolean = new AtomicBoolean(false)

  private[spark] def assertNotStopped(): Unit = {
    if (stopped.get()) {
      val activeContext = SparkContext.activeContext.get()
      val activeCreationSite =
        if (activeContext == null) {
          "(No active SparkContext.)"
        } else {
          activeContext.creationSite.longForm
        }
      throw new IllegalStateException(
        s"""Cannot call methods on a stopped SparkContext.
           |This stopped SparkContext was created at:
           |
           |${creationSite.longForm}
           |
           |And it was stopped at:
           |
           |${stopSite.getOrElse(CallSite.empty).longForm}
           |
           |The currently active SparkContext was created at:
           |
           |$activeCreationSite
         """.stripMargin)
    }
  }
```

**关键点：**
- `creationSite`：记录创建位置，用于调试
- `assertOnDriver()`：确保只在 Driver 端创建
- `markPartiallyConstructed()`：防止并发创建多个 SparkContext
- `stopped`：原子布尔值，标记是否已停止

---

### 2. 核心私有变量

```216:246:core/src/main/scala/org/apache/spark/SparkContext.scala
  private var _conf: SparkConf = _
  private var _eventLogDir: Option[URI] = None
  private var _eventLogCodec: Option[String] = None
  private var _listenerBus: LiveListenerBus = _
  private var _env: SparkEnv = _
  private var _statusTracker: SparkStatusTracker = _
  private var _progressBar: Option[ConsoleProgressBar] = None
  private var _ui: Option[SparkUI] = _
  private var _hadoopConfiguration: Configuration = _
  private var _executorMemory: Int = _
  private var _schedulerBackend: SchedulerBackend = _
  private var _taskScheduler: TaskScheduler = _
  private var _heartbeatReceiver: RpcEndpointRef = _
  @volatile private var _dagScheduler: DAGScheduler = _
  private var _applicationId: String = _
  private var _applicationAttemptId: Option[String] = None
  private var _eventLogger: Option[EventLoggingListener] = None
  private var _driverLogger: Option[DriverLogger] = None
  private var _executorAllocationManager: Option[ExecutorAllocationManager] = None
  private var _cleaner: Option[ContextCleaner] = None
  private var _listenerBusStarted: Boolean = false
  private var _jars: Seq[String] = _
  private var _files: Seq[String] = _
  private var _archives: Seq[String] = _
  private var _shutdownHookRef: AnyRef = _
  private var _statusStore: AppStatusStore = _
  private var _heartbeater: Heartbeater = _
  private var _resources: immutable.Map[String, ResourceInformation] = _
  private var _shuffleDriverComponents: ShuffleDriverComponents = _
  private var _plugins: Option[PluginContainer] = None
  private var _resourceProfileManager: ResourceProfileManager = _
```

**核心组件说明：**

| 组件 | 作用 |
|------|------|
| `_conf` | Spark 配置对象 |
| `_env` | Spark 运行环境（包含 BlockManager、RPC 等） |
| `_dagScheduler` | DAG 调度器，负责 Stage 划分和任务调度 |
| `_taskScheduler` | 任务调度器，负责将任务分配到 Executor |
| `_schedulerBackend` | 调度后端，与集群管理器通信 |
| `_listenerBus` | 事件总线，用于监听 Spark 事件 |
| `_statusStore` | 状态存储，用于 UI 和历史服务器 |
| `_cleaner` | 上下文清理器，清理不再使用的 RDD、Shuffle 数据等 |

---

## 初始化流程

初始化在 `try-catch` 块中进行，确保异常时能正确清理。

### 阶段 1：配置验证和基础设置

```409:425:core/src/main/scala/org/apache/spark/SparkContext.scala
  try {
    _conf = config.clone()
    _conf.get(SPARK_LOG_LEVEL).foreach { level =>
      if (Logging.setLogLevelPrinted) {
        System.err.printf("Setting Spark log level to \"%s\".\n", level)
      }
      setLogLevel(level)
    }
    _conf.validateSettings()
    _conf.set("spark.app.startTime", startTime.toString)

    if (!_conf.contains("spark.master")) {
      throw new SparkException("A master URL must be set in your configuration")
    }
    if (!_conf.contains("spark.app.name")) {
      throw new SparkException("An application name must be set in your configuration")
    }
```

**关键步骤：**
1. 克隆配置对象（避免修改原始配置）
2. 设置日志级别
3. 验证配置（必须包含 `spark.master` 和 `spark.app.name`）

### 阶段 2：创建事件总线和状态存储

```492:498:core/src/main/scala/org/apache/spark/SparkContext.scala
    _listenerBus = new LiveListenerBus(_conf)

    // Initialize the app status store and listener before SparkEnv is created so that it gets
    // all events.
    val appStatusSource = AppStatusSource.createSource(conf)
    _statusStore = AppStatusStore.createLiveStore(conf, appStatusSource)
    listenerBus.addToStatusQueue(_statusStore.listener.get)
```

**说明：**
- `LiveListenerBus`：异步事件总线，用于发布和监听 Spark 事件
- `AppStatusStore`：存储应用状态，供 UI 和历史服务器使用

### 阶段 3：创建 SparkEnv（运行环境）

```500:502:core/src/main/scala/org/apache/spark/SparkContext.scala
    // Create the Spark execution environment (cache, map output tracker, etc)
    _env = createSparkEnv(_conf, isLocal, listenerBus)
    SparkEnv.set(_env)
```

**SparkEnv 包含：**
- RPC 环境（RpcEnv）
- 序列化器（Serializer）
- BlockManager（块管理器）
- MapOutputTracker（Map 输出跟踪器）
- ShuffleManager（Shuffle 管理器）
- 内存管理器（MemoryManager）
- 安全管理器（SecurityManager）

### 阶段 4：创建 UI 和状态跟踪器

```510:529:core/src/main/scala/org/apache/spark/SparkContext.scala
    _statusTracker = new SparkStatusTracker(this, _statusStore)

    _progressBar =
      if (_conf.get(UI_SHOW_CONSOLE_PROGRESS)) {
        Some(new ConsoleProgressBar(this))
      } else {
        None
      }

    _ui =
      if (conf.get(UI_ENABLED)) {
        Some(SparkUI.create(Some(this), _statusStore, _conf, _env.securityManager, appName, "",
          startTime))
      } else {
        // For tests, do not enable the UI
        None
      }
    // Bind the UI before starting the task scheduler to communicate
    // the bound port to the cluster manager properly
    _ui.foreach(_.bind())
```

**说明：**
- `SparkStatusTracker`：跟踪作业、Stage、任务状态
- `ConsoleProgressBar`：控制台进度条（可选）
- `SparkUI`：Web UI（默认端口 4040）

### 阶段 5：注册 HeartbeatReceiver

```588:591:core/src/main/scala/org/apache/spark/SparkContext.scala
    // We need to register "HeartbeatReceiver" before "createTaskScheduler" because Executor will
    // retrieve "HeartbeatReceiver" in the constructor. (SPARK-6640)
    _heartbeatReceiver = env.rpcEnv.setupEndpoint(
      HeartbeatReceiver.ENDPOINT_NAME, new HeartbeatReceiver(this))
```

**说明：**
- `HeartbeatReceiver` 接收 Executor 的心跳
- 必须在创建 TaskScheduler 之前注册（Executor 构造时会获取）

### 阶段 6：创建调度器

```593:604:core/src/main/scala/org/apache/spark/SparkContext.scala
    // Initialize any plugins before initializing the task scheduler and resource profile manager.
    _plugins = PluginContainer(this, _resources.asJava)
    _resourceProfileManager = new ResourceProfileManager(_conf, _listenerBus)
    _env.initializeShuffleManager()
    _env.initializeMemoryManager(SparkContext.numDriverCores(master, conf))

    // Create and start the scheduler
    val (sched, ts) = SparkContext.createTaskScheduler(this, master)
    _schedulerBackend = sched
    _taskScheduler = ts
    _dagScheduler = new DAGScheduler(this)
    _heartbeatReceiver.ask[Boolean](TaskSchedulerIsSet)
```

**关键步骤：**
1. 初始化插件容器
2. 创建资源配置文件管理器
3. 初始化 Shuffle 管理器和内存管理器
4. **创建 TaskScheduler 和 SchedulerBackend**（根据 master URL）
5. **创建 DAGScheduler**

**TaskScheduler 创建逻辑：**

```3288:3392:core/src/main/scala/org/apache/spark/SparkContext.scala
  private def createTaskScheduler(
      sc: SparkContext,
      master: String): (SchedulerBackend, TaskScheduler) = {
    import SparkMasterRegex._

    // When running locally, don't try to re-execute tasks on failure.
    val MAX_LOCAL_TASK_FAILURES = 1

    // Ensure that default executor's resources satisfies one or more tasks requirement.
    // This function is for cluster managers that don't set the executor cores config, for
    // others its checked in ResourceProfile.
    def checkResourcesPerTask(executorCores: Int): Unit = {
      val taskCores = sc.conf.get(CPUS_PER_TASK)
      if (!sc.conf.get(SKIP_VALIDATE_CORES_TESTING)) {
        validateTaskCpusLargeEnough(sc.conf, executorCores, taskCores)
      }
      val defaultProf = sc.resourceProfileManager.defaultResourceProfile
      ResourceUtils.warnOnWastedResources(defaultProf, sc.conf, Some(executorCores))
    }

    master match {
      case "local" =>
        checkResourcesPerTask(1)
        val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
        val backend = new LocalSchedulerBackend(sc.getConf, scheduler, 1)
        scheduler.initialize(backend)
        (backend, scheduler)

      case LOCAL_N_REGEX(threads) =>
        def localCpuCount: Int = Runtime.getRuntime.availableProcessors()
        // local[*] estimates the number of cores on the machine; local[N] uses exactly N threads.
        val threadCount = if (threads == "*") localCpuCount else threads.toInt
        if (threadCount <= 0) {
          throw new SparkException(s"Asked to run locally with $threadCount threads")
        }
        checkResourcesPerTask(threadCount)
        val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
        val backend = new LocalSchedulerBackend(sc.getConf, scheduler, threadCount)
        scheduler.initialize(backend)
        (backend, scheduler)

      case LOCAL_N_FAILURES_REGEX(threads, maxFailures) =>
        def localCpuCount: Int = Runtime.getRuntime.availableProcessors()
        // local[*, M] means the number of cores on the computer with M failures
        // local[N, M] means exactly N threads with M failures
        val threadCount = if (threads == "*") localCpuCount else threads.toInt
        checkResourcesPerTask(threadCount)
        val scheduler = new TaskSchedulerImpl(sc, maxFailures.toInt, isLocal = true)
        val backend = new LocalSchedulerBackend(sc.getConf, scheduler, threadCount)
        scheduler.initialize(backend)
        (backend, scheduler)

      case SPARK_REGEX(sparkUrl) =>
        val scheduler = new TaskSchedulerImpl(sc)
        val masterUrls = sparkUrl.split(",").map("spark://" + _)
        val backend = new StandaloneSchedulerBackend(scheduler, sc, masterUrls)
        scheduler.initialize(backend)
        (backend, scheduler)

      case LOCAL_CLUSTER_REGEX(numWorkers, coresPerWorker, memoryPerWorker) =>
        checkResourcesPerTask(coresPerWorker.toInt)
        // Check to make sure memory requested <= memoryPerWorker. Otherwise Spark will just hang.
        val memoryPerWorkerInt = memoryPerWorker.toInt
        if (sc.executorMemory > memoryPerWorkerInt) {
          throw new SparkException(
            "Asked to launch cluster with %d MiB/worker but requested %d MiB/executor".format(
              memoryPerWorkerInt, sc.executorMemory))
        }

        // For host local mode setting the default of SHUFFLE_HOST_LOCAL_DISK_READING_ENABLED
        // to false because this mode is intended to be used for testing and in this case all the
        // executors are running on the same host. So if host local reading was enabled here then
        // testing of the remote fetching would be secondary as setting this config explicitly to
        // false would be required in most of the unit test (despite the fact that remote fetching
        // is much more frequent in production).
        sc.conf.setIfMissing(SHUFFLE_HOST_LOCAL_DISK_READING_ENABLED, false)

        val scheduler = new TaskSchedulerImpl(sc)
        val localCluster = LocalSparkCluster(
          numWorkers.toInt, coresPerWorker.toInt, memoryPerWorkerInt, sc.conf)
        val masterUrls = localCluster.start()
        val backend = new StandaloneSchedulerBackend(scheduler, sc, masterUrls)
        scheduler.initialize(backend)
        backend.shutdownCallback = (backend: StandaloneSchedulerBackend) => {
          localCluster.stop()
        }
        (backend, scheduler)

      case masterUrl =>
        val cm = getClusterManager(masterUrl) match {
          case Some(clusterMgr) => clusterMgr
          case None => throw new SparkException("Could not parse Master URL: '" + master + "'")
        }
        try {
          val scheduler = cm.createTaskScheduler(sc, masterUrl)
          val backend = cm.createSchedulerBackend(sc, masterUrl, scheduler)
          cm.initialize(scheduler, backend)
          (backend, scheduler)
        } catch {
          case se: SparkException => throw se
          case NonFatal(e) =>
            throw new SparkException("External scheduler cannot be instantiated", e)
        }
    }
  }
```

**支持的 Master URL：**
- `local`：单线程本地模式
- `local[N]`：N 线程本地模式
- `local[*]`：使用所有可用核心
- `local[N, M]`：N 线程，最多 M 次失败重试
- `spark://host:port`：Standalone 集群
- `local-cluster[N, cores, memory]`：本地伪集群
- `yarn`：YARN 集群
- `k8s://...`：Kubernetes 集群

### 阶段 7：启动调度器

```627:637:core/src/main/scala/org/apache/spark/SparkContext.scala
    // start TaskScheduler after taskScheduler sets DAGScheduler reference in DAGScheduler's
    // constructor
    _taskScheduler.start()

    _applicationId = _taskScheduler.applicationId()
    _applicationAttemptId = _taskScheduler.applicationAttemptId()
    _conf.set("spark.app.id", _applicationId)
    _applicationAttemptId.foreach { attemptId =>
      _conf.set(APP_ATTEMPT_ID, attemptId)
      _env.blockManager.blockStoreClient.setAppAttemptId(attemptId)
    }
```

**说明：**
- 启动 TaskScheduler（会连接到集群管理器）
- 获取应用 ID（由集群管理器分配）

### 阶段 8：初始化其他组件

```639:676:core/src/main/scala/org/apache/spark/SparkContext.scala
    // initialize after application id and attempt id has been initialized
    _shuffleDriverComponents = ShuffleDataIOUtils.loadShuffleDataIO(_conf).driver()
    _shuffleDriverComponents.initializeApplication().asScala.foreach { case (k, v) =>
      _conf.set(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX + k, v)
    }

    if (_conf.get(UI_REVERSE_PROXY)) {
      val proxyUrl = _conf.get(UI_REVERSE_PROXY_URL).getOrElse("").stripSuffix("/")
      System.setProperty("spark.ui.proxyBase", proxyUrl + "/proxy/" + _applicationId)
    }
    _ui.foreach(_.setAppId(_applicationId))
    _env.blockManager.initialize(_applicationId)
    FallbackStorage.registerBlockManagerIfNeeded(
      _env.blockManager.master, _conf, _hadoopConfiguration)

    // The metrics system for Driver need to be set spark.app.id to app ID.
    // So it should start after we get app ID from the task scheduler and set spark.app.id.
    _env.metricsSystem.start(_conf.get(METRICS_STATIC_SOURCES_ENABLED))

    _eventLogger =
      if (isEventLogEnabled) {
        val logger =
          new EventLoggingListener(_applicationId, _applicationAttemptId, _eventLogDir.get,
            _conf, _hadoopConfiguration)
        logger.start()
        listenerBus.addToEventLogQueue(logger)
        Some(logger)
      } else {
        None
      }

    _cleaner =
      if (_conf.get(CLEANER_REFERENCE_TRACKING)) {
        Some(new ContextCleaner(this, _shuffleDriverComponents))
      } else {
        None
      }
    _cleaner.foreach(_.start())
```

**关键组件：**
- `ShuffleDriverComponents`：Shuffle 驱动端组件
- `BlockManager`：块管理器初始化
- `MetricsSystem`：指标系统
- `EventLoggingListener`：事件日志监听器（可选）
- `ContextCleaner`：上下文清理器（可选）

### 阶段 9：动态资源分配（可选）

```678:693:core/src/main/scala/org/apache/spark/SparkContext.scala
    val dynamicAllocationEnabled = Utils.isDynamicAllocationEnabled(_conf)
    _executorAllocationManager =
      if (dynamicAllocationEnabled) {
        schedulerBackend match {
          case b: ExecutorAllocationClient =>
            Some(new ExecutorAllocationManager(
              schedulerBackend.asInstanceOf[ExecutorAllocationClient], listenerBus, _conf,
              cleaner = cleaner, resourceProfileManager = resourceProfileManager,
              reliableShuffleStorage = _shuffleDriverComponents.supportsReliableStorage()))
          case _ =>
            None
        }
      } else {
        None
      }
    _executorAllocationManager.foreach(_.start())
```

**说明：**
- 如果启用动态资源分配，创建 `ExecutorAllocationManager`
- 根据工作负载动态添加/移除 Executor

### 阶段 10：启动事件总线和发布事件

```695:697:core/src/main/scala/org/apache/spark/SparkContext.scala
    setupAndStartListenerBus()
    postEnvironmentUpdate()
    postApplicationStart()
```

**说明：**
- 启动事件总线
- 发布环境更新事件
- 发布应用启动事件

### 阶段 11：注册关闭钩子

```704:717:core/src/main/scala/org/apache/spark/SparkContext.scala
    // Make sure the context is stopped if the user forgets about it. This avoids leaving
    // unfinished event logs around after the JVM exits cleanly. It doesn't help if the JVM
    // is killed, though.
    logDebug("Adding shutdown hook") // force eager creation of logger
    _shutdownHookRef = ShutdownHookManager.addShutdownHook(
      ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY) { () =>
      logInfo("Invoking stop() from shutdown hook")
      try {
        stop()
      } catch {
        case e: Throwable =>
          logWarning("Ignoring Exception while stopping SparkContext from shutdown hook", e)
      }
    }
```

**说明：**
- 注册 JVM 关闭钩子，确保 SparkContext 正确停止

### 阶段 12：完成初始化

```2984:2988:core/src/main/scala/org/apache/spark/SparkContext.scala
  // In order to prevent multiple SparkContexts from being active at the same time, mark this
  // context as having finished construction.
  // NOTE: this must be placed at the end of the SparkContext constructor.
  SparkContext.setActiveContext(this)
```

**说明：**
- 将当前 SparkContext 设置为活跃上下文

---

## 核心方法

### 1. runJob - 作业执行入口

```2481:2499:core/src/main/scala/org/apache/spark/SparkContext.scala
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit): Unit = {
    if (stopped.get()) {
      throw new IllegalStateException("SparkContext has been shutdown")
    }
    val callSite = getCallSite()
    val cleanedFunc = clean(func)
    logInfo(log"Starting job: ${MDC(LogKeys.CALL_SITE_SHORT_FORM, callSite.shortForm)}")
    if (conf.getBoolean("spark.logLineage", false)) {
      logInfo(log"RDD's recursive dependencies:\n" +
        log"${MDC(LogKeys.RDD_DEBUG_STRING, rdd.toDebugString)}")
    }
    dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get)
    progressBar.foreach(_.finishAll())
    rdd.doCheckpoint()
  }
```

**执行流程：**
1. 检查 SparkContext 是否已停止
2. 获取调用位置（用于日志和调试）
3. 清理闭包（序列化准备）
4. 记录 RDD 依赖关系（如果启用）
5. **调用 DAGScheduler.runJob**（核心执行逻辑）
6. 完成进度条
7. 执行 RDD 检查点（如果配置）

**这是所有 RDD Action 操作的最终入口！**

### 2. submitJob - 异步作业提交

```2634:2652:core/src/main/scala/org/apache/spark/SparkContext.scala
  def submitJob[T, U, R](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit,
      resultFunc: => R): SimpleFutureAction[R] =
  {
    assertNotStopped()
    val cleanF = clean(processPartition)
    val callSite = getCallSite()
    val waiter = dagScheduler.submitJob(
      rdd,
      (context: TaskContext, iter: Iterator[T]) => cleanF(iter),
      partitions,
      callSite,
      resultHandler,
      localProperties.get)
    new SimpleFutureAction(waiter, resultFunc)
  }
```

**说明：**
- 异步提交作业，返回 `FutureAction`
- 可以用于非阻塞的作业执行

### 3. 作业控制方法

```2671:2775:core/src/main/scala/org/apache/spark/SparkContext.scala
  def cancelJobGroup(groupId: String, reason: String): Unit = {
    assertNotStopped()
    dagScheduler.cancelJobGroup(groupId, cancelFutureJobs = false, Option(reason))
  }

  def cancelJobGroup(groupId: String): Unit = {
    assertNotStopped()
    dagScheduler.cancelJobGroup(groupId, cancelFutureJobs = false, None)
  }

  def cancelJobGroupAndFutureJobs(groupId: String, reason: String): Unit = {
    assertNotStopped()
    dagScheduler.cancelJobGroup(groupId, cancelFutureJobs = true, Option(reason))
  }

  def cancelJobGroupAndFutureJobs(groupId: String): Unit = {
    assertNotStopped()
    dagScheduler.cancelJobGroup(groupId, cancelFutureJobs = true, None)
  }

  def cancelJobsWithTag(tag: String, reason: String): Unit = {
    SparkContext.throwIfInvalidTag(tag)
    assertNotStopped()
    dagScheduler.cancelJobsWithTag(tag, Option(reason), cancelledJobs = None)
  }

  def cancelJobsWithTag(tag: String): Unit = {
    SparkContext.throwIfInvalidTag(tag)
    assertNotStopped()
    dagScheduler.cancelJobsWithTag(tag, reason = None, cancelledJobs = None)
  }

  /** Cancel all jobs that have been scheduled or are running.  */
  def cancelAllJobs(): Unit = {
    assertNotStopped()
    dagScheduler.cancelAllJobs()
  }

  def cancelJob(jobId: Int, reason: String): Unit = {
    dagScheduler.cancelJob(jobId, Option(reason))
  }

  def cancelJob(jobId: Int): Unit = {
    dagScheduler.cancelJob(jobId, None)
  }

  def cancelStage(stageId: Int, reason: String): Unit = {
    dagScheduler.cancelStage(stageId, Option(reason))
  }

  def cancelStage(stageId: Int): Unit = {
    dagScheduler.cancelStage(stageId, None)
  }
```

**说明：**
- 支持按组、标签、作业 ID、Stage ID 取消作业
- 所有取消操作都委托给 DAGScheduler

### 4. 本地属性管理

```829:993:core/src/main/scala/org/apache/spark/SparkContext.scala
  def setLocalProperty(key: String, value: String): Unit = {
    if (value == null) {
      localProperties.get.remove(key)
    } else {
      localProperties.get.setProperty(key, value)
    }
  }

  def getLocalProperty(key: String): String =
    Option(localProperties.get).map(_.getProperty(key)).orNull

  def setJobGroup(groupId: String,
      description: String, interruptOnCancel: Boolean = false): Unit = {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, description)
    setLocalProperty(SparkContext.SPARK_JOB_GROUP_ID, groupId)
    setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, interruptOnCancel.toString)
  }

  def clearJobGroup(): Unit = {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, null)
    setLocalProperty(SparkContext.SPARK_JOB_GROUP_ID, null)
    setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, null)
  }

  def addJobTag(tag: String): Unit = addJobTags(Set(tag))

  def addJobTags(tags: Set[String]): Unit = {
    tags.foreach(SparkContext.throwIfInvalidTag)
    val existingTags = getJobTags()
    val newTags = (existingTags ++ tags).mkString(SparkContext.SPARK_JOB_TAGS_SEP)
    setLocalProperty(SparkContext.SPARK_JOB_TAGS, newTags)
  }

  def removeJobTag(tag: String): Unit = removeJobTags(Set(tag))

  def removeJobTags(tags: Set[String]): Unit = {
    tags.foreach(SparkContext.throwIfInvalidTag)
    val existingTags = getJobTags()
    val newTags = (existingTags -- tags).mkString(SparkContext.SPARK_JOB_TAGS_SEP)
    if (newTags.isEmpty) {
      clearJobTags()
    } else {
      setLocalProperty(SparkContext.SPARK_JOB_TAGS, newTags)
    }
  }

  def getJobTags(): Set[String] = {
    Option(getLocalProperty(SparkContext.SPARK_JOB_TAGS))
      .map(_.split(SparkContext.SPARK_JOB_TAGS_SEP).toSet)
      .getOrElse(Set())
      .filter(!_.isEmpty)
  }

  def clearJobTags(): Unit = {
    setLocalProperty(SparkContext.SPARK_JOB_TAGS, null)
  }
```

**说明：**
- `localProperties`：线程本地属性，用于传递作业组、标签等信息
- 子线程会继承父线程的属性
- 用于作业分组、取消、调度池等

---

## 对象方法（SparkContext 伴生对象）

### 1. getOrCreate - 单例模式

```3071:3084:core/src/main/scala/org/apache/spark/SparkContext.scala
  def getOrCreate(config: SparkConf): SparkContext = {
    // Synchronize to ensure that multiple create requests don't trigger an exception
    // from assertNoOtherContextIsRunning within setActiveContext
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      if (activeContext.get() == null) {
        setActiveContext(new SparkContext(config))
      } else {
        if (config.getAll.nonEmpty) {
          logWarning("Using an existing SparkContext; some configuration may not take effect.")
        }
      }
      activeContext.get()
    }
  }
```

**说明：**
- 获取或创建 SparkContext（单例模式）
- 如果已存在，返回现有的；否则创建新的
- 线程安全

### 2. 上下文管理

```3018:3146:core/src/main/scala/org/apache/spark/SparkContext.scala
  private[spark] def markPartiallyConstructed(sc: SparkContext): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      assertNoOtherContextIsRunning(sc)
      contextBeingConstructed = Some(sc)
    }
  }

  private[spark] def setActiveContext(sc: SparkContext): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      assertNoOtherContextIsRunning(sc)
      contextBeingConstructed = None
      activeContext.set(sc)
    }
  }

  private[spark] def clearActiveContext(): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      activeContext.set(null)
    }
  }
```

**说明：**
- `markPartiallyConstructed`：标记正在构造（防止并发创建）
- `setActiveContext`：设置为活跃上下文（构造完成）
- `clearActiveContext`：清除活跃上下文（停止时调用）

---

## 执行流程总结

### 完整执行链路

```
用户代码（RDD Action）
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
递归创建父 Stage（ShuffleMapStage）
    ↓
DAGScheduler.submitStage()
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

---

## 关键设计模式

### 1. 单例模式
- 每个 JVM 只能有一个活跃的 SparkContext
- 通过 `activeContext` 和锁机制保证

### 2. 建造者模式
- 通过多个构造函数支持不同的创建方式
- 最终都调用主构造函数

### 3. 策略模式
- `createTaskScheduler` 根据 master URL 选择不同的调度器实现

### 4. 观察者模式
- `LiveListenerBus` 用于事件发布和订阅

---

## 重要注意事项

1. **线程安全**
   - 使用 `AtomicBoolean`、`AtomicReference` 等原子类
   - 使用 `SPARK_CONTEXT_CONSTRUCTOR_LOCK` 保护关键区域

2. **异常处理**
   - 构造函数使用 `try-catch`，异常时调用 `stop()` 清理

3. **资源清理**
   - 注册关闭钩子，确保资源正确释放
   - `ContextCleaner` 自动清理不再使用的资源

4. **初始化顺序**
   - 严格按照顺序初始化组件（存在依赖关系）
   - 例如：HeartbeatReceiver 必须在 TaskScheduler 之前创建

---

## 总结

`SparkContext` 是 Spark 应用的**核心控制器**，负责：

1. **初始化**：创建和配置所有核心组件
2. **协调**：协调 DAGScheduler、TaskScheduler、Executor 等组件
3. **执行**：提供作业提交和执行接口
4. **管理**：管理应用生命周期、资源、事件等

理解 `SparkContext` 是理解 Spark 整体架构的基础！

