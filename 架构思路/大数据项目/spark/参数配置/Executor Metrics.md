详细配置：[Executor Metrics](https://spark.apache.org/docs/latest/configuration.html#executor-metrics)

这些配置项主要用于执行器指标（Executor Metrics）的收集、监控和事件日志记录。具体功能如下：

## 主要功能概述

这些配置项控制 Spark 如何收集、记录和报告执行器的运行时指标，用于性能监控、问题诊断和资源分析。

### 具体功能分类：

1. 指标收集与轮询

- spark.executor.metrics.pollingInterval：控制指标收集频率

- spark.executor.processTreeMetrics.enabled：控制是否收集进程树指标（从 /proc 文件系统）

1. 事件日志记录

- spark.eventLog.logStageExecutorMetrics：控制是否将每个阶段的执行器指标峰值写入事件日志

1. 垃圾回收（GC）指标

- spark.eventLog.gcMetrics.youngGenerationGarbageCollectors：配置支持的年轻代 GC 收集器

- spark.eventLog.gcMetrics.oldGenerationGarbageCollectors：配置支持的老年代 GC 收集器

1. 文件系统指标

- spark.executor.metrics.fileSystemSchemes：指定在指标中报告哪些文件系统（如 file、hdfs）

### 应用场景

- 性能监控：跟踪执行器的 CPU、内存、磁盘 I/O 等

- 问题诊断：通过事件日志分析性能瓶颈

- 资源优化：基于指标数据调整资源配置

- GC 分析：监控和记录垃圾回收行为

这些配置项共同构成了 Spark 的执行器监控体系，帮助用户了解应用运行时的资源使用情况。