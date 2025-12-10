## Spark Memory Management（内存管理）配置解析

### 一、主要功能分类

#### 1. 内存分配配置

堆内存分配：

- spark.memory.fraction：用于执行和存储的内存比例（默认 0.6，即堆空间减去 300MB 后的 60%）

- spark.memory.storageFraction：存储内存中不可驱逐的比例（默认 0.5，即存储区域的 50%）

堆外内存配置：

- spark.memory.offHeap.enabled：是否启用堆外内存（默认 false）

- spark.memory.offHeap.size：堆外内存的绝对大小（字节），启用堆外内存时必须设置为正值

#### 2. 存储内存管理配置

- spark.storage.unrollMemoryThreshold：展开块前请求的初始内存（默认 1MB）

- spark.storage.replication.proactive：是否启用 RDD 块的主动复制（默认 true），执行器故障时自动补充丢失的副本

- spark.storage.localDiskByExecutors.cacheSize：本地目录缓存的最大执行器数量（默认 1000），用于避免从同一主机获取磁盘持久化块时的网络开销

#### 3. 上下文清理配置

清理触发机制：

- spark.cleaner.periodicGC.interval：定期触发垃圾回收的间隔（默认 30 分钟）

清理功能控制：

- spark.cleaner.referenceTracking：是否启用上下文清理（默认 true）

- spark.cleaner.referenceTracking.blocking：清理线程是否阻塞（默认 true，shuffle 除外）

- spark.cleaner.referenceTracking.blocking.shuffle：shuffle 清理是否阻塞（默认 false）

- spark.cleaner.referenceTracking.cleanCheckpoints：引用超出作用域时是否清理检查点文件（默认 false）

------

### 二、应用场景

#### 1. 内存密集型作业场景

- 调整 spark.memory.fraction 增加执行和存储可用内存

- 注意：增加该值需要相应调整 JVM GC 参数，避免 GC 压力过大

- 使用堆外内存（spark.memory.offHeap.enabled=true）减少 GC 压力

#### 2. 缓存密集型应用场景

- 提高 spark.memory.storageFraction 增加不可驱逐的缓存空间

- 权衡：过高会减少执行内存，可能导致任务溢出到磁盘

- 启用 spark.storage.replication.proactive 提高缓存数据的容错性

#### 3. GC 压力大的场景

- 启用堆外内存（spark.memory.offHeap.enabled=true）并设置 spark.memory.offHeap.size

- 注意：堆外内存不计入 JVM 堆，需相应减少堆内存大小

- 适用于需要大量缓存但堆内存受限的场景

#### 4. 长时间运行应用场景

- 配置 spark.cleaner.periodicGC.interval 定期触发清理，防止磁盘空间耗尽

- 在大型驱动器 JVM 且内存压力小的场景中，弱引用可能很少被回收，需要定期清理

#### 5. 高可用性要求场景

- 启用 spark.storage.replication.proactive 自动补充丢失的 RDD 块副本

- 适用于执行器故障频繁或需要高数据可用性的场景

#### 6. 本地磁盘优化场景

- 调整 spark.storage.localDiskByExecutors.cacheSize 优化本地磁盘块获取

- 当 spark.shuffle.readHostLocalDisk 启用时，可避免从同一主机获取 shuffle 块时的网络开销

#### 7. Shuffle 密集型作业场景

- 设置 spark.cleaner.referenceTracking.blocking.shuffle=false 使 shuffle 清理不阻塞

- 适用于 shuffle 数据量大、需要快速清理的场景

#### 8. 检查点管理场景

- 配置 spark.cleaner.referenceTracking.cleanCheckpoints 控制是否自动清理检查点文件

- 设置为 true 可自动清理不再需要的检查点，节省存储空间

#### 9. 内存受限环境场景

- 降低 spark.memory.fraction 为元数据和用户数据结构预留更多空间

- 适用于稀疏记录或异常大记录的场景

- 注意：过低会导致更频繁的溢出和缓存驱逐

#### 10. 性能调优场景

- 调整 spark.storage.unrollMemoryThreshold 优化块展开时的内存分配

- 根据块大小特征调整初始内存请求，减少内存分配开销

------

### 三、总结

Memory Management 配置控制 Spark 的内存分配、存储管理和资源清理，直接影响性能、稳定性和资源利用。

核心价值：

1. 内存分配优化：通过 spark.memory.fraction 和 spark.memory.storageFraction 平衡执行与存储内存

1. GC 压力缓解：堆外内存可减少 GC 压力，适合缓存密集型应用

1. 容错性提升：主动块复制提高数据可用性

1. 资源清理：定期清理和引用跟踪防止资源泄漏和磁盘空间耗尽

1. 性能优化：本地磁盘缓存减少网络开销

关键配置原则：

- 内存分配平衡：spark.memory.fraction 和 spark.memory.storageFraction 的默认值通常已足够，除非有明确需求

- 堆外内存使用：启用堆外内存时，需相应减少 JVM 堆大小，确保总内存不超过容器限制

- GC 调优：增加 spark.memory.fraction 时，必须调整 JVM GC 参数以适应更大的堆内存使用

- 清理策略：长时间运行的应用应配置定期清理，防止资源累积

- 阻塞策略：shuffle 清理通常设为非阻塞，避免影响任务执行

内存模型理解：

- 总堆内存 = spark.executor.memory

- 可用内存 = (堆内存 - 300MB) × spark.memory.fraction

- 存储内存 = 可用内存 × spark.memory.storageFraction

- 执行内存 = 可用内存 × (1 - spark.memory.storageFraction)

- 堆外内存 = spark.memory.offHeap.size（独立于堆内存）

注意事项：

- spark.memory.fraction 的默认值（0.6）通常是最优的，不建议随意修改

- 堆外内存启用后，总内存消耗 = 堆内存 + 堆外内存 + 内存开销

- 定期清理间隔应根据应用运行时间和资源使用情况调整

- 主动块复制会增加网络和存储开销，需权衡容错性和性能

- 本地磁盘缓存大小限制可防止无界存储，但过小可能影响性能

这些配置共同构成 Spark 的内存管理体系，帮助在不同场景下优化内存使用、提升性能并确保系统稳定性。