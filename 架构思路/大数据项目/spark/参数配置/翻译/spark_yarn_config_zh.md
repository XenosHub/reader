# Spark YARN 配置选项解析

## 一、主要功能分类

### 1. **Application Master 资源配置**

#### 内存配置
- `spark.yarn.am.memory`：YARN Application Master 的内存大小（默认 512m）
  - 仅用于 client 模式
  - cluster 模式使用 `spark.driver.memory`
  - 格式：512m, 2g 等

- `spark.yarn.am.memoryOverhead`：AM 的非堆内存开销（默认 AM 内存 × 0.10，最小 384MB）
  - 与 `spark.driver.memoryOverhead` 类似，但用于 client 模式的 AM

#### CPU 配置
- `spark.yarn.am.cores`：AM 使用的核心数（默认 1）
  - 仅用于 client 模式
  - cluster 模式使用 `spark.driver.cores`

#### 特殊资源配置
- `spark.yarn.am.resource.{resource-type}.amount`：AM 的特殊资源数量（client 模式）
  - 需要 YARN 3.0+
  - 示例：`spark.yarn.am.resource.yarn.io/gpu.amount=1`

- `spark.yarn.driver.resource.{resource-type}.amount`：驱动器的特殊资源数量（cluster 模式）
  - 需要 YARN 3.0+
  - 示例：`spark.yarn.driver.resource.yarn.io/gpu.amount=1`

- `spark.yarn.executor.resource.{resource-type}.amount`：每个执行器的特殊资源数量
  - 需要 YARN 3.0+
  - 示例：`spark.yarn.executor.resource.yarn.io/gpu.amount=1`

#### 资源类型映射
- `spark.yarn.resourceGpuDeviceName`：GPU 资源类型映射（默认 `yarn.io/gpu`）
  - 如果 YARN 使用自定义资源类型，可以重新映射

- `spark.yarn.resourceFpgaDeviceName`：FPGA 资源类型映射（默认 `yarn.io/fpga`）
  - 如果 YARN 使用自定义资源类型，可以重新映射

### 2. **文件分发配置**

#### 文件分发
- `spark.yarn.dist.files`：分发到执行器工作目录的文件列表（逗号分隔）
  - 支持 glob 模式

- `spark.yarn.dist.jars`：分发到执行器工作目录的 JAR 文件列表（逗号分隔）
  - 支持 glob 模式

- `spark.yarn.dist.archives`：分发到执行器工作目录的归档文件列表（逗号分隔）
  - 会被解压到工作目录

- `spark.yarn.dist.forceDownloadSchemes`：强制下载到本地磁盘的协议列表（逗号分隔）
  - 用于 YARN 不支持的协议（如 http, https, ftp）
  - 支持通配符 `*` 表示所有协议

#### Spark JAR 分发
- `spark.yarn.jars`：包含 Spark 代码的库列表
  - 默认使用本地安装的 Spark JAR
  - 可以设置为 HDFS 路径（如 `hdfs:///some/path`）
  - 支持 glob 模式
  - 允许 YARN 缓存，避免每次运行都分发

- `spark.yarn.archive`：包含所需 Spark JAR 的归档文件
  - 如果设置，会替换 `spark.yarn.jars`
  - 归档文件在根目录应包含 JAR 文件
  - 可以托管在 HDFS 上以加速文件分发

#### 文件管理
- `spark.yarn.submit.file.replication`：上传到 HDFS 的文件副本数（默认 HDFS 默认副本数，通常为 3）
  - 包括 Spark JAR、应用 JAR 和分布式缓存文件

- `spark.yarn.stagingDir`：提交应用时使用的暂存目录（默认用户主目录）

- `spark.yarn.preserve.staging.files`：是否在作业结束时保留暂存文件（默认 false）
  - true：保留 Spark JAR、应用 JAR、分布式缓存文件
  - false：删除暂存文件

### 3. **心跳和通信配置**

- `spark.yarn.scheduler.heartbeat.interval-ms`：AM 向 YARN ResourceManager 心跳的间隔（默认 3000ms）
  - 值被限制为 YARN 过期间隔的一半（`yarn.am.liveness-monitor.expiry-interval-ms`）

- `spark.yarn.scheduler.initial-allocation.interval`：初始分配间隔（默认 200ms）
  - 当有待分配的容器请求时，AM 急切心跳的初始间隔
  - 不应大于 `spark.yarn.scheduler.heartbeat.interval-ms`
  - 如果仍有待分配的容器，分配间隔会在连续急切心跳时翻倍，直到达到 `heartbeat.interval-ms`

- `spark.yarn.am.waitTime`：AM 等待 SparkContext 初始化的时间（默认 100s）
  - 仅用于 cluster 模式

- `spark.yarn.report.interval`：cluster 模式下报告当前 Spark 作业状态的间隔（默认 1s）

- `spark.yarn.clientLaunchMonitorInterval`：client 模式启动应用时请求状态的间隔（默认 1s）

- `spark.yarn.report.loggingFrequency`：记录应用状态前处理的最大应用报告数（默认 30）
  - 如果状态发生变化，无论处理了多少报告，都会记录应用状态

### 4. **环境变量和类路径配置**

- `spark.yarn.appMasterEnv.[EnvironmentVariableName]`：添加到 AM 进程的环境变量
  - cluster 模式：控制 Spark 驱动器的环境
  - client 模式：仅控制执行器启动器的环境
  - 可以设置多个环境变量

- `spark.yarn.am.extraJavaOptions`：传递给 AM 的额外 JVM 选项（client 模式）
  - cluster 模式使用 `spark.driver.extraJavaOptions`
  - 不能设置最大堆大小（-Xmx），应使用 `spark.yarn.am.memory`

- `spark.yarn.am.extraLibraryPath`：启动 AM 时使用的特殊库路径（client 模式）

- `spark.yarn.populateHadoopClasspath`：是否从 YARN 类路径填充 Hadoop 类路径
  - with-hadoop Spark 发行版：false
  - no-hadoop 发行版：true
  - 如果设置为 false，需要 with-Hadoop Spark 发行版或单独提供 Hadoop 安装

- `spark.yarn.containerLauncherMaxThreads`：AM 中用于启动执行器容器的最大线程数（默认 25）

### 5. **应用类型和元数据配置**

- `spark.yarn.applicationType`：定义更具体的应用类型（默认 SPARK）
  - 可选值：SPARK, SPARK-SQL, SPARK-STREAMING, SPARK-MLLIB, SPARK-GRAPH
  - 注意不要超过 20 个字符

- `spark.yarn.queue`：提交应用的 YARN 队列名称（默认 default）

- `spark.yarn.tags`：传递给 YARN 的应用标签列表（逗号分隔）
  - 出现在 YARN ApplicationReports 中
  - 可用于查询 YARN 应用时的过滤

- `spark.yarn.priority`：YARN 应用优先级
  - 整数值，值越大优先级越高
  - 目前 YARN 仅在 FIFO 排序策略下支持应用优先级

### 6. **节点标签和调度配置**

- `spark.yarn.am.nodeLabelExpression`：限制 AM 调度节点集的 YARN 节点标签表达式
  - 需要 YARN 2.6+
  - 早期版本会忽略此属性

- `spark.yarn.executor.nodeLabelExpression`：限制执行器调度节点集的 YARN 节点标签表达式
  - 需要 YARN 2.6+
  - 早期版本会忽略此属性

- `spark.yarn.exclude.nodes`：从资源分配中排除的 YARN 节点名称列表（逗号分隔）

### 7. **故障处理和重试配置**

- `spark.yarn.maxAppAttempts`：提交应用的最大尝试次数（默认 YARN 的 `yarn.resourcemanager.am.max-attempts`）
  - 不应大于 YARN 配置中的全局最大尝试次数

- `spark.yarn.am.attemptFailuresValidityInterval`：AM 故障跟踪的有效性间隔
  - 如果 AM 运行时间至少达到定义的间隔，AM 故障计数将重置
  - 如果未配置，此功能不启用

- `spark.yarn.am.clientModeTreatDisconnectAsFailed`：将 yarn-client 模式的异常断开视为失败（默认 false）
  - true：如果 AM 异常断开（没有正确的关闭握手），应用将以 FAILED 状态终止
  - false：应用总是以 SUCCESS 状态完成（因为无法知道是用户终止还是真实错误）

- `spark.yarn.am.clientModeExitOnError`：client 模式下的错误退出行为（默认 false）
  - true：如果驱动器收到最终状态为 KILLED 或 FAILED 的应用报告，将停止 SparkContext 并以代码 1 退出程序
  - 注意：如果从另一个应用调用，也会终止父应用

- `spark.yarn.executor.launch.excludeOnFailure.enabled`：启用排除有 YARN 资源分配问题的节点（默认 false）
  - 排除的错误限制可通过 `spark.excludeOnFailure.application.maxFailedExecutorsPerNode` 配置

### 8. **日志配置**

- `spark.yarn.rolledLog.includePattern`：Java 正则表达式，过滤匹配包含模式的日志文件
  - 匹配的日志文件将以滚动方式聚合
  - 与 YARN 的滚动日志聚合一起使用
  - 需要在 YARN 端配置 `yarn.nodemanager.log-aggregation.roll-monitoring-interval-seconds`
  - Spark log4j appender 需要改为使用 FileAppender 或可以处理文件被删除的 appender
  - 示例：`spark*` 匹配所有以 spark 开头的日志文件

- `spark.yarn.rolledLog.excludePattern`：Java 正则表达式，过滤匹配排除模式的日志文件
  - 匹配的日志文件不会以滚动方式聚合
  - 如果日志文件名同时匹配包含和排除模式，最终会被排除

- `spark.yarn.includeDriverLogsLink`：cluster 模式下，客户端应用报告是否包含驱动器容器日志的链接（默认 false）
  - 需要轮询 ResourceManager 的 REST API，会对 RM 产生额外负载

### 9. **历史服务器和监控配置**

- `spark.yarn.historyServer.address`：Spark 历史服务器地址（如 host.com:18080）
  - 地址不应包含方案（http://）
  - 默认不设置，因为历史服务器是可选服务
  - 应用完成时，此地址会提供给 YARN ResourceManager
  - 用于从 ResourceManager UI 链接到 Spark 历史服务器 UI
  - 可以使用 YARN 属性作为变量，例如：`${hadoopconf-yarn.resourcemanager.hostname}:18080`

- `spark.yarn.metrics.namespace`：AM 指标报告的根命名空间
  - 如果未设置，则使用 YARN 应用 ID

### 10. **异构集群配置**

- `spark.yarn.config.gatewayPath`：网关主机上有效的路径
  - 可能与集群中其他节点的同一资源路径不同
  - 与 `spark.yarn.config.replacementPath` 配合使用
  - 用于支持异构配置的集群

- `spark.yarn.config.replacementPath`：替换路径
  - 通常包含对 YARN 导出的环境变量的引用
  - 与 `spark.yarn.config.gatewayPath` 配合使用
  - 示例：如果网关节点在 `/disk1/hadoop` 安装 Hadoop，YARN 将 Hadoop 安装位置导出为 `HADOOP_HOME` 环境变量，设置 `gatewayPath` 为 `/disk1/hadoop`，`replacementPath` 为 `$HADOOP_HOME`

### 11. **安全配置**

- `spark.yarn.am.tokenConfRegex`：用于从作业配置文件（如 hdfs-site.xml）中 grep 配置项列表的正则表达式
  - 发送给 RM，用于续订委托令牌
  - 典型用例：支持 YARN 集群需要与多个下游 HDFS 集群通信的环境
  - 示例：`^dfs.nameservices$|^dfs.namenode.rpc-address.*$|^dfs.ha.namenodes.*$`

- `spark.yarn.shuffle.server.recovery.disabled`：是否禁用 shuffle 服务器恢复（默认 false）
  - true：对于有更高安全要求的应用，偏好不将密钥保存在数据库中
  - 此类应用的 shuffle 数据在外部 Shuffle 服务重启后不会恢复

### 12. **其他配置**

- `spark.executor.instances`：静态分配的执行器数量（默认 2）
  - 使用 `spark.dynamicAllocation.enabled` 时，初始执行器集至少为此数量

- `spark.yarn.submit.waitAppCompletion`：YARN cluster 模式下，客户端是否等待应用完成（默认 true）
  - true：客户端进程保持活动状态，报告应用状态
  - false：客户端进程在提交后退出

- `spark.yarn.unmanagedAM.enabled`：client 模式下，是否使用非托管 AM 将 Application Master 服务作为客户端的一部分启动（默认 false）

## 二、应用场景

### 1. **GPU 加速场景**

**场景描述**：使用 GPU 进行机器学习训练或计算

**配置方案**：
```properties
# 请求 GPU 资源
spark.yarn.am.resource.yarn.io/gpu.amount=1  # client 模式
spark.yarn.driver.resource.yarn.io/gpu.amount=1  # cluster 模式
spark.yarn.executor.resource.yarn.io/gpu.amount=1  # 每个执行器

# 如果 YARN 使用自定义 GPU 资源类型
spark.yarn.resourceGpuDeviceName=custom-gpu-resource
```

**效果**：从 YARN 请求 GPU 资源，分配给 AM、驱动器和执行器

### 2. **大规模文件分发场景**

**场景描述**：需要分发大量文件或 JAR 到执行器

**配置方案**：
```properties
# 将 Spark JAR 放在 HDFS 上，允许 YARN 缓存
spark.yarn.jars=hdfs:///spark/jars/*.jar

# 或使用归档文件
spark.yarn.archive=hdfs:///spark/spark-libs.zip

# 分发应用文件
spark.yarn.dist.files=/path/to/config.properties
spark.yarn.dist.jars=/path/to/app.jar
spark.yarn.dist.archives=/path/to/data.zip

# 强制下载 HTTP/HTTPS 资源
spark.yarn.dist.forceDownloadSchemes=http,https
```

**效果**：减少文件分发时间，提高启动速度

### 3. **多队列调度场景**

**场景描述**：需要将应用提交到特定 YARN 队列

**配置方案**：
```properties
# 指定队列
spark.yarn.queue=production

# 设置应用优先级
spark.yarn.priority=10

# 添加应用标签
spark.yarn.tags=production,ml-training
```

**效果**：应用提交到指定队列，便于资源管理和监控

### 4. **节点标签调度场景**

**场景描述**：需要将 AM 或执行器调度到特定类型的节点

**配置方案**：
```properties
# AM 调度到 GPU 节点
spark.yarn.am.nodeLabelExpression=GPU

# 执行器调度到高内存节点
spark.yarn.executor.nodeLabelExpression=HIGH_MEMORY

# 排除故障节点
spark.yarn.exclude.nodes=node1.example.com,node2.example.com
```

**效果**：精确控制应用组件的调度位置

### 5. **高可用性场景**

**场景描述**：需要提高应用的容错能力

**配置方案**：
```properties
# 增加最大尝试次数
spark.yarn.maxAppAttempts=5

# 配置 AM 故障有效性间隔
spark.yarn.am.attemptFailuresValidityInterval=1h

# 启用节点排除
spark.yarn.executor.launch.excludeOnFailure.enabled=true
```

**效果**：提高应用成功运行的概率

### 6. **日志聚合场景**

**场景描述**：需要聚合和查看应用日志

**配置方案**：
```properties
# 配置日志滚动聚合
spark.yarn.rolledLog.includePattern=spark*
spark.yarn.rolledLog.excludePattern=*.tmp

# 配置历史服务器地址
spark.yarn.historyServer.address=${hadoopconf-yarn.resourcemanager.hostname}:18080

# 包含驱动器日志链接
spark.yarn.includeDriverLogsLink=true
```

**效果**：方便查看和分析应用日志

### 7. **异构集群场景**

**场景描述**：集群中不同节点的路径配置不同

**配置方案**：
```properties
# 配置网关路径和替换路径
spark.yarn.config.gatewayPath=/disk1/hadoop
spark.yarn.config.replacementPath=$HADOOP_HOME
```

**效果**：支持异构配置的集群，确保远程进程正确启动

### 8. **安全增强场景**

**场景描述**：需要更高的安全要求

**配置方案**：
```properties
# 配置委托令牌
spark.yarn.am.tokenConfRegex=^dfs.nameservices$|^dfs.namenode.rpc-address.*$|^dfs.ha.namenodes.*$

# 禁用 shuffle 服务器恢复（不保存密钥）
spark.yarn.shuffle.server.recovery.disabled=true
```

**效果**：增强应用安全性

### 9. **快速启动场景**

**配置方案**：
```properties
# 减少心跳间隔（更快分配资源）
spark.yarn.scheduler.heartbeat.interval-ms=1000
spark.yarn.scheduler.initial-allocation.interval=100ms

# 不等待应用完成
spark.yarn.submit.waitAppCompletion=false
```

**效果**：加快资源分配和应用启动

### 10. **资源密集型 AM 场景**

**场景描述**：AM 需要更多资源

**配置方案**：
```properties
# 增加 AM 内存
spark.yarn.am.memory=2g
spark.yarn.am.memoryOverhead=512m

# 增加 AM CPU
spark.yarn.am.cores=2

# 增加启动线程数
spark.yarn.containerLauncherMaxThreads=50
```

**效果**：为 AM 提供更多资源，提高启动速度

### 11. **环境变量配置场景**

**场景描述**：需要为 AM 或执行器设置特定环境变量

**配置方案**：
```properties
# 设置环境变量
spark.yarn.appMasterEnv.HADOOP_CONF_DIR=/etc/hadoop/conf
spark.yarn.appMasterEnv.SPARK_HOME=/opt/spark
spark.yarn.appMasterEnv.PYTHONPATH=/opt/python/lib

# 设置 JVM 选项
spark.yarn.am.extraJavaOptions=-XX:+UseG1GC -XX:MaxGCPauseMillis=200
```

**效果**：为应用提供正确的运行环境

### 12. **Client 模式错误处理场景**

**场景描述**：需要更严格的错误处理

**配置方案**：
```properties
# 将异常断开视为失败
spark.yarn.am.clientModeTreatDisconnectAsFailed=true

# 错误时退出
spark.yarn.am.clientModeExitOnError=true
```

**效果**：更准确地反映应用状态，便于错误处理

## 三、总结

### 核心价值

1. **资源管理**：
   - 精确控制 AM、驱动器和执行器的资源分配
   - 支持特殊资源（GPU、FPGA）的请求
   - 通过节点标签表达式控制调度位置

2. **文件分发优化**：
   - 通过 HDFS 缓存 Spark JAR，减少分发时间
   - 支持多种文件分发方式
   - 强制下载不支持的协议资源

3. **调度控制**：
   - 队列、优先级、标签等调度策略
   - 节点标签表达式精确控制调度
   - 排除故障节点

4. **容错和可靠性**：
   - 应用重试机制
   - 节点排除功能
   - 故障有效性间隔

5. **监控和日志**：
   - 历史服务器集成
   - 日志滚动聚合
   - 指标报告

6. **安全增强**：
   - 委托令牌配置
   - Shuffle 服务器恢复控制
   - 环境变量和类路径管理

### 关键配置原则

1. **资源分配**：
   - Client 模式使用 `spark.yarn.am.*` 配置
   - Cluster 模式使用 `spark.driver.*` 配置
   - 特殊资源需要 YARN 3.0+

2. **文件分发**：
   - 将 Spark JAR 放在 HDFS 上可以显著提升启动速度
   - 使用归档文件可以减少文件数量
   - 强制下载不支持的协议资源

3. **心跳和通信**：
   - 心跳间隔不应超过 YARN 过期间隔的一半
   - 初始分配间隔用于快速分配资源
   - 报告间隔影响状态更新频率

4. **调度策略**：
   - 使用队列、优先级、标签控制调度
   - 节点标签表达式需要 YARN 2.6+
   - 排除节点可以避免故障节点

5. **容错配置**：
   - 最大尝试次数不应超过 YARN 全局配置
   - 故障有效性间隔可以重置故障计数
   - 节点排除可以自动处理故障节点

### 注意事项

- **模式差异**：Client 模式和 Cluster 模式使用不同的配置项
- **版本要求**：某些功能需要特定版本的 YARN（如节点标签需要 2.6+，特殊资源需要 3.0+）
- **性能影响**：某些配置可能影响性能（如日志链接需要轮询 RM API）
- **安全考虑**：禁用 shuffle 服务器恢复会丢失重启后的数据恢复能力
- **路径配置**：异构集群需要正确配置网关路径和替换路径
- **资源限制**：AM 资源不应设置过大，避免影响其他应用

### 配置建议

- **生产环境**：
  - 将 Spark JAR 放在 HDFS 上
  - 配置历史服务器地址
  - 启用日志聚合
  - 设置合理的队列和优先级

- **GPU 场景**：
  - 使用特殊资源配置请求 GPU
  - 使用节点标签表达式调度到 GPU 节点
  - 配置资源类型映射（如果使用自定义类型）

- **高可用场景**：
  - 增加最大尝试次数
  - 配置故障有效性间隔
  - 启用节点排除

- **性能优化**：
  - 优化心跳间隔
  - 使用 HDFS 缓存 Spark JAR
  - 增加容器启动线程数

- **监控和调试**：
  - 配置历史服务器
  - 启用日志聚合
  - 设置应用标签便于查询

这些配置选项共同控制 Spark 在 YARN 上的运行行为，影响资源分配、文件分发、调度策略、容错能力和监控能力。合理配置这些参数可以在 YARN 集群上优化 Spark 应用的性能、可靠性和可管理性。

