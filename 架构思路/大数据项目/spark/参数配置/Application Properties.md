## Spark Application Properties（应用属性）配置解析

### 一、主要功能分类

#### 1. 应用基本信息配置

- spark.app.name：应用名称，显示在 UI 和日志中

- spark.master：集群管理器连接地址

- spark.submit.deployMode：部署模式（client 或 cluster）

- spark.log.callerContext：应用信息，写入 Yarn RM 日志/HDFS 审计日志

- spark.log.level：日志级别覆盖设置

#### 2. 驱动器（Driver）资源配置

- 内存配置

- spark.driver.memory：驱动器堆内存大小

- spark.driver.memoryOverhead：非堆内存开销（VM 开销、字符串驻留等）

- spark.driver.minMemoryOverhead：最小非堆内存开销

- spark.driver.memoryOverheadFactor：内存开销因子（默认 0.10，Kubernetes 非 JVM 作业为 0.40）

- spark.driver.maxResultSize：序列化结果总大小限制

- CPU 配置

- spark.driver.cores：驱动器使用的核心数（仅集群模式）

- 资源发现配置

- spark.driver.resource.{resourceName}.amount：特定资源类型的数量

- spark.driver.resource.{resourceName}.discoveryScript：资源发现脚本

- spark.driver.resource.{resourceName}.vendor：资源供应商（如 GPU：nvidia.com）

#### 3. 执行器（Executor）资源配置

- 内存配置

- spark.executor.memory：每个执行器的堆内存大小

- spark.executor.pyspark.memory：PySpark 执行器内存限制

- spark.executor.memoryOverhead：执行器非堆内存开销

- spark.executor.minMemoryOverhead：最小非堆内存开销

- spark.executor.memoryOverheadFactor：内存开销因子（默认 0.10，Kubernetes 非 JVM 作业为 0.40）

- 资源发现配置

- spark.executor.resource.{resourceName}.amount：每个执行器的资源数量

- spark.executor.resource.{resourceName}.discoveryScript：资源发现脚本

- spark.executor.resource.{resourceName}.vendor：资源供应商

#### 4. 资源发现插件配置

- spark.resources.discoveryPlugin：资源发现插件类列表，用于自定义资源发现实现

#### 5. 日志配置

- 驱动器日志

- spark.driver.log.localDir：驱动器日志本地目录

- spark.driver.log.dfsDir：驱动器日志 DFS 基础目录

- spark.driver.log.persistToDfs.enabled：是否持久化驱动器日志到 DFS

- spark.driver.log.layout：驱动器日志布局格式

- spark.driver.log.allowErasureCoding：是否允许使用纠删码

- 通用日志

- spark.logConf：是否在启动时记录有效 SparkConf

- spark.extraListeners：额外的 SparkListener 类列表

#### 6. 驱动器生命周期管理

- spark.driver.supervise：是否在失败时自动重启驱动器（仅 Standalone 模式）

- spark.driver.timeout：驱动器超时时间（分钟），0 表示无限

#### 7. 执行器生命周期与故障处理

- spark.executor.maxNumFailures：应用失败前的最大执行器故障数

- spark.executor.failuresValidityInterval：执行器故障有效性间隔，超过此间隔的故障不计入失败计数

#### 8. 去委派（Decommission）配置

- spark.decommission.enabled：是否启用优雅关闭执行器

- spark.executor.decommission.killInterval：去委派后强制终止的时间间隔

- spark.executor.decommission.forceKillTimeout：强制终止去委派执行器的超时时间

- spark.executor.decommission.signal：触发执行器开始去委派的信号（默认 PWR）

#### 9. 存储配置

- spark.local.dir：Spark 临时目录，用于 map 输出文件和存储在磁盘上的 RDD

------

### 二、应用场景

#### 1. 资源密集型作业场景

- 调整 spark.driver.memory、spark.executor.memory 和内存开销参数

- 配置 spark.driver.maxResultSize 防止驱动器 OOM

- 使用 spark.local.dir 配置多个快速本地磁盘

#### 2. PySpark 应用场景

- 设置 spark.executor.pyspark.memory 限制 Python 内存

- Kubernetes 非 JVM 作业使用更高的内存开销因子（0.40）

#### 3. GPU/特殊硬件加速场景

- 通过 spark.driver.resource.{resourceName}.* 和 spark.executor.resource.{resourceName}.* 配置 GPU

- 使用 spark.resources.discoveryPlugin 集成自定义资源发现

#### 4. 容器化部署场景（YARN/Kubernetes）

- 配置内存开销和最小内存开销

- 使用资源发现脚本定位容器内资源

- 设置资源供应商（如 nvidia.com）

#### 5. 高可用与故障恢复场景

- 启用 spark.driver.supervise（Standalone）

- 配置 spark.executor.maxNumFailures 和 spark.executor.failuresValidityInterval 控制故障容忍度

- 启用去委派功能，优雅迁移数据

#### 6. 日志审计与监控场景

- 配置 spark.log.callerContext 写入审计日志

- 使用 spark.driver.log.* 持久化驱动器日志到 DFS

- 添加 spark.extraListeners 实现自定义监控

#### 7. 集群维护场景

- 启用 spark.decommission.enabled 优雅下线节点

- 配置去委派超时和信号，确保数据迁移完成

#### 8. 调试与开发场景

- 使用 spark.log.level 调整日志级别

- 启用 spark.logConf 查看有效配置

- 配置 spark.driver.log.localDir 查看本地日志

------

### 三、总结

Application Properties 是 Spark 应用的核心配置，涵盖资源分配、生命周期管理、日志和监控。

核心价值：

1. 资源管理：精确控制驱动器和执行器的内存、CPU 和特殊资源（如 GPU）

1. 内存优化：通过内存开销配置避免 OOM，适配不同运行时环境

1. 高可用性：支持驱动器监控、执行器故障容忍和优雅去委派

1. 可观测性：日志持久化、审计日志和自定义监听器

1. 部署灵活性：支持多种部署模式和资源发现机制

关键配置原则：

- 内存配置：堆内存 + 非堆内存开销 = 容器总内存需求

- 故障容忍：根据作业重要性设置 spark.executor.maxNumFailures

- 日志管理：生产环境启用日志持久化，便于问题诊断

- 资源发现：容器化环境使用资源发现脚本定位硬件资源

- 去委派：维护场景启用去委派，确保数据不丢失

注意事项：

- Client 模式下，spark.driver.memory 需通过命令行或配置文件设置，不能通过 SparkConf

- Kubernetes 非 JVM 作业默认使用更高的内存开销因子（0.40）

- 内存开销包括 VM 开销、字符串驻留、PySpark 内存等

- 去委派超时应设置较大值，确保数据迁移完成

这些配置共同支撑 Spark 应用在不同环境下的稳定运行和性能优化。