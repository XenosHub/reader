# Spark Scheduling（调度）配置选项解析

## 一、主要功能分类

### 1. **资源调度配置**

#### 核心资源限制

- `spark.cores.max`：应用从整个集群请求的最大 CPU 核心数（Standalone 模式）
  - 不设置时使用 `spark.deploy.defaultCores` 的默认值
  - 控制应用可使用的总 CPU 资源

#### 任务资源分配

- `spark.task.cpus`：为每个任务分配的核心数（默认 1）
  
  - 控制任务的 CPU 资源需求
  - 影响任务的并发度和资源利用率

- `spark.task.resource.{resourceName}.amount`：为每个任务分配的特定资源数量
  
  - 支持 GPU 等特殊资源
  - 支持分数分配（如 0.25，表示 4 个任务共享 1 个资源）
  - 分数必须 ≤ 0.5（即至少 2 个任务共享 1 个资源）

#### 资源注册等待

- `spark.scheduler.maxRegisteredResourcesWaitingTime`：调度开始前等待资源注册的最大时间（默认 30 秒）
  
  - 控制调度器等待资源就绪的时间

- `spark.scheduler.minRegisteredResourcesRatio`：调度开始前需要注册的最小资源比例
  
  - Standalone 模式：0.0（不等待）
  - YARN 模式：0.8（等待 80% 的资源）
  - Kubernetes 模式：0.8（等待 80% 的资源）
  - 与 `maxRegisteredResourcesWaitingTime` 配合使用

### 2. **数据本地性配置**

#### 本地性等待时间

- `spark.locality.wait`：启动数据本地任务前的等待时间（默认 3 秒）
  - 用于多个本地性级别：process-local → node-local → rack-local → any
  - 任务较长且本地性差时，可增加此值

#### 各层级本地性配置

- `spark.locality.wait.process`：进程本地性等待时间（默认等于 `spark.locality.wait`）
  
  - 影响访问特定执行器进程中缓存数据的任务

- `spark.locality.wait.node`：节点本地性等待时间（默认等于 `spark.locality.wait`）
  
  - 可设置为 0 跳过节点本地性，直接搜索机架本地性

- `spark.locality.wait.rack`：机架本地性等待时间（默认等于 `spark.locality.wait`）
  
  - 控制机架级别的数据本地性等待

### 3. **调度模式配置**

- `spark.scheduler.mode`：作业调度模式（默认 FIFO）
  
  - **FIFO**：先进先出，按提交顺序执行
  - **FAIR**：公平调度，多用户服务场景适用
  - 适用于提交到同一 SparkContext 的多个作业

- `spark.scheduler.revive.interval`：调度器重新激活工作节点资源提供的间隔（默认 1 秒）
  
  - 控制调度器检查可用资源的频率

### 4. **事件队列配置**

#### 事件队列容量

- `spark.scheduler.listenerbus.eventqueue.capacity`：事件队列的默认容量（默认 10000）
  - 如果监听器事件被丢弃，考虑增加此值（如 20000）
  - 增加此值可能导致驱动器使用更多内存

#### 各类型事件队列

- `spark.scheduler.listenerbus.eventqueue.shared.capacity`：共享事件队列容量
  
  - 用于外部监听器注册到监听器总线的事件

- `spark.scheduler.listenerbus.eventqueue.appStatus.capacity`：应用状态事件队列容量
  
  - 用于内部应用状态监听器的事件

- `spark.scheduler.listenerbus.eventqueue.executorManagement.capacity`：执行器管理事件队列容量
  
  - 用于内部执行器管理监听器的事件

- `spark.scheduler.listenerbus.eventqueue.eventLog.capacity`：事件日志队列容量
  
  - 用于将事件写入事件日志的监听器

- `spark.scheduler.listenerbus.eventqueue.streams.capacity`：流事件队列容量
  
  - 用于内部流监听器的事件

### 5. **资源配置文件合并**

- `spark.scheduler.resource.profileMergeConflicts`：是否合并资源配置文件冲突（默认 false）
  
  - 当合并到同一阶段的 RDD 指定了不同的 ResourceProfile 时
  - true：合并配置，选择每个资源的最大值
  - false：抛出异常（默认）

- `spark.standalone.submit.waitAppCompletion`：Standalone 模式下是否等待应用完成（默认 false）
  
  - 注意：描述似乎有误，实际功能可能不同

### 6. **故障排除配置**

#### 故障排除启用

- `spark.excludeOnFailure.enabled`：是否启用故障排除（默认 false）
  
  - 防止在因任务失败过多而被排除的执行器上调度任务
  - 可被应用级和任务/阶段级配置覆盖

- `spark.excludeOnFailure.application.enabled`：应用级故障排除（默认 false）
  
  - 覆盖 `spark.excludeOnFailure.enabled`
  - 在整个应用级别排除执行器

- `spark.excludeOnFailure.taskAndStage.enabled`：任务/阶段级故障排除（默认 false）
  
  - 覆盖 `spark.excludeOnFailure.enabled`
  - 在任务集级别排除执行器

#### 故障排除超时

- `spark.excludeOnFailure.timeout`：节点或执行器被排除的时间（默认 1 小时）
  - 实验性功能
  - 超时后无条件从排除列表中移除，尝试运行新任务

#### 任务级排除

- `spark.excludeOnFailure.task.maxTaskAttemptsPerExecutor`：任务在单个执行器上的最大重试次数（默认 1）
  
  - 实验性功能
  - 超过此次数后，该执行器被排除用于该任务

- `spark.excludeOnFailure.task.maxTaskAttemptsPerNode`：任务在单个节点上的最大重试次数（默认 2）
  
  - 实验性功能
  - 超过此次数后，整个节点被排除用于该任务

#### 阶段级排除

- `spark.excludeOnFailure.stage.maxFailedTasksPerExecutor`：阶段中单个执行器上的最大失败任务数（默认 2）
  
  - 实验性功能
  - 超过此数量后，该执行器被排除用于该阶段

- `spark.excludeOnFailure.stage.maxFailedExecutorsPerNode`：阶段中单个节点上的最大失败执行器数（默认 2）
  
  - 实验性功能
  - 超过此数量后，整个节点被标记为失败用于该阶段

#### 应用级排除

- `spark.excludeOnFailure.application.maxFailedTasksPerExecutor`：应用级别单个执行器上的最大失败任务数（默认 2）
  
  - 实验性功能
  - 在成功的任务集中，超过此数量后，该执行器被排除用于整个应用

- `spark.excludeOnFailure.application.maxFailedExecutorsPerNode`：应用级别单个节点上的最大失败执行器数（默认 2）
  
  - 实验性功能
  - 超过此数量后，该节点被排除用于整个应用

#### 其他故障排除配置

- `spark.excludeOnFailure.killExcludedExecutors`：是否自动杀死被排除的执行器（默认 false）
  
  - 实验性功能
  - 当整个节点被排除时，该节点上的所有执行器将被杀死

- `spark.excludeOnFailure.application.fetchFailure.enabled`：获取失败时是否立即排除执行器（默认 false）
  
  - 实验性功能
  - 如果启用了外部 shuffle 服务，整个节点将被排除

- `spark.scheduler.excludeOnFailure.unschedulableTaskSetTimeout`：无法调度的任务集超时（默认 120 秒）
  
  - 由于所有执行器被排除而无法调度时，等待获取新执行器和调度任务的时间

### 7. **推测执行配置**

#### 推测执行启用

- `spark.speculation`：是否启用推测执行（默认 false）
  - 如果阶段中有一个或多个任务运行缓慢，将重新启动它们

#### 推测执行参数

- `spark.speculation.interval`：检查推测任务的频率（默认 100ms）

- `spark.speculation.multiplier`：任务比中位数慢多少倍才考虑推测（默认 3）
  
  - 任务运行时间 > 中位数 × 3 时，可能被推测执行

- `spark.speculation.quantile`：启用推测前必须完成的任务比例（默认 0.9）
  
  - 90% 的任务完成后，才开始推测执行

- `spark.speculation.minTaskRuntime`：任务被考虑推测前的最小运行时间（默认 100ms）
  
  - 避免对非常短的任务启动推测副本

- `spark.speculation.task.duration.threshold`：任务持续时间阈值
  
  - 如果提供，当阶段包含的任务数 ≤ 单个执行器的槽数，且任务运行时间超过阈值时，将推测执行
  - 有助于推测任务数很少的阶段

#### 效率评估配置

- `spark.speculation.efficiency.enabled`：是否启用效率评估（默认 true）
  
  - 通过阶段任务指标或持续时间评估任务处理效率
  - 只推测低效率任务

- `spark.speculation.efficiency.processRateMultiplier`：评估低效率任务的乘数（默认 0.75）
  
  - 乘数越高，更多任务可能被视为低效率

- `spark.speculation.efficiency.longRunTaskFactor`：长时间运行任务因子（默认 2）
  
  - 如果任务持续时间超过因子 × 时间阈值，无论如何都会推测
  - 避免遗漏与数据处理速率无关的慢任务

### 8. **任务故障处理配置**

- `spark.task.maxFailures`：放弃作业前特定任务的连续失败次数（默认 4）
  - 允许的重试次数 = 此值 - 1
  - 如果任何尝试成功，任务的失败计数将重置

### 9. **任务监控配置**

- `spark.task.reaper.enabled`：是否启用任务监控（默认 false）
  
  - 监控被杀死/中断的任务
  - 执行器将监控任务直到实际完成执行

- `spark.task.reaper.pollingInterval`：轮询被杀死任务状态的频率（默认 10 秒）

- `spark.task.reaper.threadDump`：是否在轮询期间记录任务线程转储（默认 true）

- `spark.task.reaper.killTimeout`：执行器 JVM 自毁超时（默认 -1，禁用）
  
  - 如果被杀死的任务在超时后仍未停止运行，执行器 JVM 将杀死自己
  - 作为安全网，防止无法取消的任务使执行器无法使用

### 10. **阶段故障处理配置**

- `spark.stage.maxConsecutiveAttempts`：阶段被中止前允许的连续尝试次数（默认 4）

- `spark.stage.ignoreDecommissionFetchFailure`：是否忽略由执行器去委派引起的阶段获取失败（默认 true）
  
  - 在计算 `spark.stage.maxConsecutiveAttempts` 时忽略此类失败

## 二、应用场景

### 1. **多用户共享集群场景**

**场景描述**：多个用户或应用共享同一个 Spark 集群

**配置方案**：

```properties
# 启用公平调度
spark.scheduler.mode=FAIR

# 限制每个应用的核心数
spark.cores.max=100

# 配置资源注册等待
spark.scheduler.minRegisteredResourcesRatio=0.8
spark.scheduler.maxRegisteredResourcesWaitingTime=60s
```

**效果**：公平分配资源，避免单个应用占用过多资源

### 2. **GPU 加速场景**

**场景描述**：使用 GPU 进行机器学习训练或计算

**配置方案**：

```properties
# 为每个任务分配 GPU 资源
spark.task.resource.gpu.amount=0.25  # 4 个任务共享 1 个 GPU

# 执行器配置（需要在执行器端配置）
spark.executor.resource.gpu.amount=2
spark.executor.resource.gpu.discoveryScript=/path/to/gpu-discovery.sh
```

**效果**：合理分配 GPU 资源，提高利用率

### 3. **数据本地性优化场景**

**场景描述**：需要最大化数据本地性以减少网络传输

**配置方案**：

```properties
# 增加本地性等待时间
spark.locality.wait=10s
spark.locality.wait.node=5s
spark.locality.wait.process=3s
spark.locality.wait.rack=10s
```

**效果**：更长时间等待本地任务，减少网络 I/O

### 4. **快速启动场景**

**场景描述**：需要快速启动调度，不等待所有资源

**配置方案**：

```properties
# 降低资源注册等待比例
spark.scheduler.minRegisteredResourcesRatio=0.5

# 减少资源注册等待时间
spark.scheduler.maxRegisteredResourcesWaitingTime=10s

# 跳过某些本地性级别
spark.locality.wait.node=0  # 跳过节点本地性
```

**效果**：快速开始执行，适合对延迟敏感的应用

### 5. **不稳定节点场景**

**场景描述**：集群中有不稳定的节点或执行器

**配置方案**：

```properties
# 启用故障排除
spark.excludeOnFailure.enabled=true
spark.excludeOnFailure.application.enabled=true

# 配置排除阈值
spark.excludeOnFailure.application.maxFailedTasksPerExecutor=3
spark.excludeOnFailure.application.maxFailedExecutorsPerNode=2

# 配置排除超时
spark.excludeOnFailure.timeout=30min

# 自动杀死被排除的执行器
spark.excludeOnFailure.killExcludedExecutors=true
```

**效果**：自动排除故障节点，提高作业成功率

### 6. **慢任务处理场景**

**场景描述**：阶段中有慢任务影响整体性能

**配置方案**：

```properties
# 启用推测执行
spark.speculation=true

# 调整推测参数
spark.speculation.interval=500ms
spark.speculation.multiplier=2  # 更敏感
spark.speculation.quantile=0.75  # 更早开始推测

# 启用效率评估
spark.speculation.efficiency.enabled=true
spark.speculation.efficiency.processRateMultiplier=0.8
```

**效果**：自动检测并重新执行慢任务，提升整体性能

### 7. **高并发事件场景**

**场景描述**：大量监听器事件，可能导致事件丢失

**配置方案**：

```properties
# 增加事件队列容量
spark.scheduler.listenerbus.eventqueue.capacity=50000
spark.scheduler.listenerbus.eventqueue.shared.capacity=20000
spark.scheduler.listenerbus.eventqueue.appStatus.capacity=20000
spark.scheduler.listenerbus.eventqueue.eventLog.capacity=20000
```

**效果**：避免事件丢失，但会增加驱动器内存使用

### 8. **长时间运行任务场景**

**场景描述**：任务运行时间很长，需要更好的监控

**配置方案**：

```properties
# 启用任务监控
spark.task.reaper.enabled=true
spark.task.reaper.pollingInterval=30s
spark.task.reaper.threadDump=true
spark.task.reaper.killTimeout=3600s  # 1 小时
```

**效果**：更好地监控被杀死但仍在运行的任务

### 9. **资源密集型任务场景**

**场景描述**：任务需要更多 CPU 资源

**配置方案**：

```properties
# 为每个任务分配更多 CPU
spark.task.cpus=2  # 每个任务使用 2 个核心

# 相应地调整执行器核心数
spark.executor.cores=8  # 每个执行器 8 个核心，可运行 4 个任务
```

**效果**：任务获得更多 CPU 资源，但并发度降低

### 10. **混合资源需求场景**

**场景描述**：不同 RDD 需要不同的资源配置文件

**配置方案**：

```properties
# 启用资源配置文件合并
spark.scheduler.resource.profileMergeConflicts=true
```

**效果**：自动合并冲突的资源配置，选择最大值

### 11. **网络不稳定场景**

**场景描述**：网络不稳定，经常出现 fetch failure

**配置方案**：

```properties
# 启用获取失败排除
spark.excludeOnFailure.application.fetchFailure.enabled=true

# 增加任务最大失败次数
spark.task.maxFailures=6

# 增加阶段最大连续尝试次数
spark.stage.maxConsecutiveAttempts=6
```

**效果**：快速排除有网络问题的节点

### 12. **去委派场景**

**场景描述**：执行器去委派时出现获取失败

**配置方案**：

```properties
# 忽略去委派引起的获取失败
spark.stage.ignoreDecommissionFetchFailure=true
```

**效果**：避免因去委派导致的阶段失败

## 三、总结

### 核心价值

1. **资源管理**：
   
   - 通过核心限制和任务资源分配控制资源使用
   - 支持特殊资源（如 GPU）的分配
   - 通过资源注册等待平衡启动速度和资源可用性

2. **数据本地性优化**：
   
   - 通过多层级本地性等待最大化数据本地性
   - 减少网络传输，提升性能
   - 可针对不同层级独立配置

3. **调度策略**：
   
   - FIFO 模式适合单用户场景
   - FAIR 模式适合多用户共享场景
   - 通过调度器激活间隔控制资源检查频率

4. **故障容错**：
   
   - 多层次的故障排除机制（任务级、阶段级、应用级）
   - 自动排除故障节点和执行器
   - 可配置的排除超时和自动恢复

5. **性能优化**：
   
   - 推测执行自动处理慢任务
   - 效率评估只推测低效率任务
   - 任务监控确保资源正确释放

6. **事件管理**：
   
   - 可配置的事件队列容量
   - 不同类型事件独立队列
   - 防止事件丢失

### 关键配置原则

1. **资源分配**：
   
   - `spark.task.cpus` × 任务数 ≤ `spark.executor.cores`
   - 特殊资源支持分数分配，但必须 ≤ 0.5
   - 资源注册等待在启动速度和资源可用性之间平衡

2. **数据本地性**：
   
   - 任务越长，本地性等待时间可以越长
   - 可跳过某些本地性级别以快速启动
   - 各层级等待时间可独立配置

3. **推测执行**：
   
   - 需要足够多的任务完成才能准确判断慢任务
   - 效率评估可以更精确地识别需要推测的任务
   - 避免对短任务进行推测

4. **故障排除**：
   
   - 多层次配置，应用级覆盖全局，任务/阶段级覆盖应用级
   - 排除超时后自动恢复
   - 可配置自动杀死被排除的执行器

5. **事件队列**：
   
   - 如果事件被丢弃，增加队列容量
   - 增加容量会增加驱动器内存使用
   - 不同类型事件使用独立队列

### 注意事项

- **实验性功能**：许多故障排除和效率评估配置标记为实验性，需谨慎使用
- **内存影响**：增加事件队列容量会增加驱动器内存使用
- **资源平衡**：任务资源分配需要与执行器资源匹配
- **推测执行开销**：推测执行会增加资源使用，需权衡性能提升和资源消耗
- **故障排除影响**：排除节点可能影响资源可用性，需合理配置超时
- **动态分配**：使用动态分配时，被排除的执行器可能被标记为空闲并被回收

### 配置建议

- **生产环境默认**：
  
  - 保持大多数默认值
  - 根据实际工作负载调整本地性等待时间
  - 启用推测执行（如果资源充足）

- **多用户环境**：
  
  - 使用 FAIR 调度模式
  - 设置核心限制
  - 配置合理的资源注册等待

- **不稳定环境**：
  
  - 启用故障排除
  - 配置合理的排除阈值和超时
  - 增加任务和阶段的最大失败次数

- **性能调优**：
  
  - 根据数据分布调整本地性等待
  - 启用推测执行和效率评估
  - 监控事件队列，必要时增加容量

- **监控指标**：
  
  - 任务本地性统计
  - 推测执行效果
  - 故障排除统计
  - 事件队列使用情况

这些配置选项共同控制 Spark 的调度行为，影响资源利用率、数据本地性、故障容错和整体性能。合理配置这些参数可以在不同场景下优化 Spark 应用的执行效率和稳定性。
