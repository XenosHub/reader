控制 Spark 作业的执行方式、资源使用和数据处理行为。

### 主要功能分类：

1. 广播变量（Broadcast）配置

   - spark.broadcast.blockSize：控制广播数据的分块大小，影响广播性能

   - spark.broadcast.checksum：启用校验和，检测广播数据损坏

   - spark.broadcast.UDFCompressionThreshold：UDF 和 Python RDD 命令的压缩阈值

1. 资源分配配置

   - spark.executor.cores：每个执行器使用的核心数

   - spark.default.parallelism：默认并行度，影响 RDD 分区数量

1. 通信与心跳配置
   - spark.executor.heartbeatInterval：执行器向驱动器的心跳间隔，用于存活检测和指标更新

1. 文件处理配置

   - spark.files.*：控制文件获取、缓存、覆盖、错误处理等

   - spark.files.maxPartitionBytes：单个分区的最大字节数

   - spark.files.openCostInBytes：打开文件的估算成本

1. Hadoop 集成配置

   - spark.hadoop.cloneConf：为每个任务克隆 Hadoop 配置，解决线程安全问题

   - spark.hadoop.validateOutputSpecs：验证输出规范

   - spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version：文件输出提交器算法版本

1. 存储配置

   - spark.storage.memoryMapThreshold：内存映射阈值

   - spark.storage.decommission.*：执行器去委派时的块迁移和回退存储配置