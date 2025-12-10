# Push-based Shuffle 客户端配置选项

## Client Side Configuration Options

以下配置选项用于控制 Spark 客户端端的 Push-based Shuffle 行为。

### 配置属性

| 属性名称 | 默认值 | 含义 | 起始版本 |
| --- | --- | --- | --- |
| `spark.shuffle.push.enabled` | `false` | 设置为 true 以在客户端启用基于推送的 shuffle，与服务器端标志 `spark.shuffle.push.server.mergedShuffleFileManagerImpl` 配合使用。 | 3.2.0 |
| `spark.shuffle.push.finalize.timeout` | `10s` | 在给定 shuffle map 阶段的所有 mapper 完成后，驱动器在向远程外部 shuffle 服务发送合并完成请求之前等待的时间（秒）。这为外部 shuffle 服务提供了额外的合并块时间。设置过长可能导致性能下降。 | 3.2.0 |
| `spark.shuffle.push.maxRetainedMergerLocations` | `500` | 基于推送的 shuffle 缓存的最大合并位置数。目前，合并位置是负责处理推送块、合并它们并为后续 shuffle 获取提供合并块的外部 shuffle 服务的主机。 | 3.2.0 |
| `spark.shuffle.push.mergersMinThresholdRatio` | `0.05` | 用于根据 reducer 阶段的分区数计算阶段所需的最小 shuffle 合并位置数的比例。例如，一个有 100 个分区的 reduce 阶段，使用默认值 0.05 需要至少 5 个唯一的合并位置才能启用基于推送的 shuffle。 | 3.2.0 |
| `spark.shuffle.push.mergersMinStaticThreshold` | `5` | 为阶段启用基于推送的 shuffle 应可用的 shuffle 推送合并位置数的静态阈值。注意此配置与 `spark.shuffle.push.mergersMinThresholdRatio` 配合使用。启用基于推送的 shuffle 所需的最大合并位置数为 `spark.shuffle.push.mergersMinStaticThreshold` 和 `spark.shuffle.push.mergersMinThresholdRatio` 比例计算出的合并位置数中的较大值。例如：子阶段有 1000 个分区，`spark.shuffle.push.mergersMinStaticThreshold` 为 5，`spark.shuffle.push.mergersMinThresholdRatio` 设置为 0.05，我们需要至少 50 个合并器才能为该阶段启用基于推送的 shuffle。 | 3.2.0 |
| `spark.shuffle.push.numPushThreads` | `(none)` | 指定块推送器池中的线程数。这些线程协助创建连接并将块推送到远程外部 shuffle 服务。默认情况下，线程池大小等于 `spark.executor.cores` 的数量。 | 3.2.0 |
| `spark.shuffle.push.maxBlockSizeToPush` | `1m` | 推送到远程外部 shuffle 服务的单个块的最大大小。超过此阈值的块不会被推送到远程合并。这些 shuffle 块将以原始方式获取。设置过高会导致更多块被推送到远程外部 shuffle 服务，但这些块已经可以通过现有机制高效获取，从而产生将大块推送到远程外部 shuffle 服务的额外开销。建议将 `spark.shuffle.push.maxBlockSizeToPush` 设置为小于 `spark.shuffle.push.maxBlockBatchSize` 配置的值。设置过低会导致合并的块数量减少，直接从 mapper 外部 shuffle 服务获取会导致更多小的随机读取，影响整体磁盘 I/O 性能。 | 3.2.0 |
| `spark.shuffle.push.maxBlockBatchSize` | `3m` | 要分组到单个推送请求中的 shuffle 块批次的最大大小。默认设置为 3m，以保持略高于 `spark.storage.memoryMapThreshold` 默认值 2m，因为每个块批次很可能被内存映射，这会产生更高的开销。 | 3.2.0 |
| `spark.shuffle.push.merge.finalizeThreads` | `8` | 驱动器用于完成 shuffle 合并的线程数。由于大型 shuffle 的完成可能需要几秒钟，当启用基于推送的 shuffle 时，多个线程有助于驱动器处理并发的 shuffle 合并完成请求。 | 3.3.0 |
| `spark.shuffle.push.minShuffleSizeToWait` | `500m` | 仅当总 shuffle 数据大小超过此阈值时，驱动器才会等待合并完成。如果总 shuffle 大小较小，驱动器将立即完成 shuffle 输出。 | 3.3.0 |
| `spark.shuffle.push.minCompletedPushRatio` | `1.0` | 在基于推送的 shuffle 期间，驱动器开始 shuffle 合并完成之前应完成推送的最小 map 分区比例。 | 3.3.0 |

## 一、主要功能分类

### 1. **Push-based Shuffle 启用控制**
- `spark.shuffle.push.enabled`：客户端启用/禁用基于推送的 shuffle
  - 需要与服务器端配置 `spark.shuffle.push.server.mergedShuffleFileManagerImpl` 配合使用
  - 默认禁用，需要显式启用

### 2. **合并位置管理配置**
- `spark.shuffle.push.maxRetainedMergerLocations`：缓存的最大合并位置数（默认 500）
  - 合并位置是外部 shuffle 服务的主机地址
  - 用于跟踪可用的合并服务位置

- `spark.shuffle.push.mergersMinThresholdRatio`：基于分区数的动态阈值比例（默认 0.05）
  - 根据 reducer 阶段的分区数计算所需的最小合并位置数
  - 公式：最小合并位置数 = max(静态阈值, 分区数 × 比例)

- `spark.shuffle.push.mergersMinStaticThreshold`：静态最小阈值（默认 5）
  - 无论分区数多少，都需要满足的最小合并位置数
  - 与比例阈值取最大值作为实际要求

### 3. **块推送配置**
- `spark.shuffle.push.numPushThreads`：推送线程数
  - 默认等于执行器核心数
  - 控制并发推送块到远程服务的线程数

- `spark.shuffle.push.maxBlockSizeToPush`：单个块的最大推送大小（默认 1MB）
  - 超过此大小的块不会推送，使用传统方式获取
  - 需要在推送开销和随机读取之间平衡

- `spark.shuffle.push.maxBlockBatchSize`：批次最大大小（默认 3MB）
  - 单个推送请求中块批次的最大大小
  - 应大于 `spark.storage.memoryMapThreshold`（默认 2MB）

### 4. **合并完成配置**
- `spark.shuffle.push.finalize.timeout`：合并完成等待超时（默认 10 秒）
  - 所有 mapper 完成后，等待外部 shuffle 服务合并的时间
  - 给服务额外时间完成块合并

- `spark.shuffle.push.merge.finalizeThreads`：合并完成线程数（默认 8）
  - 驱动器处理并发 shuffle 合并完成的线程数
  - 大型 shuffle 可能需要多线程处理

- `spark.shuffle.push.minShuffleSizeToWait`：最小等待的 shuffle 大小（默认 500MB）
  - 仅当 shuffle 数据超过此阈值时才等待合并完成
  - 小 shuffle 立即完成，不等待

- `spark.shuffle.push.minCompletedPushRatio`：最小完成推送比例（默认 1.0）
  - 开始合并完成前必须完成推送的最小 map 分区比例
  - 1.0 表示所有分区必须完成推送

## 二、应用场景

### 1. **启用 Push-based Shuffle 场景**
- **场景描述**：需要启用基于推送的 shuffle 以提升性能
- **配置方法**：
  - 客户端：设置 `spark.shuffle.push.enabled=true`
  - 服务器端：设置 `spark.shuffle.push.server.mergedShuffleFileManagerImpl=org.apache.spark.network.shuffle.RemoteBlockPushResolver`
- **适用情况**：
  - 大规模 shuffle 操作
  - 需要减少网络 I/O 和磁盘 I/O
  - 提升 shuffle 性能的场景

### 2. **大规模分区场景**
- **场景描述**：reducer 阶段有大量分区（如 1000+）
- **配置调整**：
  - 根据分区数动态计算所需合并位置数
  - 例如：1000 个分区 × 0.05 = 50 个合并位置（需要至少 50 个）
  - 如果静态阈值为 5，则实际需要 50 个合并位置
- **注意事项**：确保有足够的外部 shuffle 服务节点

### 3. **小规模分区场景**
- **场景描述**：reducer 阶段分区数较少（如 50）
- **配置调整**：
  - 50 个分区 × 0.05 = 2.5，但静态阈值为 5
  - 实际需要至少 5 个合并位置
- **效果**：静态阈值确保小规模作业也有足够的合并位置

### 4. **高并发推送场景**
- **场景描述**：需要同时推送大量块到远程服务
- **配置调整**：
  - 增加 `spark.shuffle.push.numPushThreads` 以提升并发推送能力
  - 根据网络带宽和执行器核心数调整
- **权衡**：过多线程可能导致网络拥塞

### 5. **大块处理场景**
- **场景描述**：shuffle 块较大（超过 1MB）
- **配置调整**：
  - 增加 `spark.shuffle.push.maxBlockSizeToPush` 允许推送更大的块
  - 但需注意：大块已可通过现有机制高效获取，推送可能产生额外开销
- **建议**：保持默认值或根据实际块大小分布调整

### 6. **小块优化场景**
- **场景描述**：大量小 shuffle 块（小于 1MB）
- **配置调整**：
  - 保持或降低 `spark.shuffle.push.maxBlockSizeToPush`
  - 增加 `spark.shuffle.push.maxBlockBatchSize` 以批量推送更多小块
- **效果**：减少推送请求数量，提升效率

### 7. **大型 Shuffle 合并场景**
- **场景描述**：shuffle 数据量很大（超过 500MB）
- **配置调整**：
  - 增加 `spark.shuffle.push.finalize.timeout` 给服务更多合并时间
  - 增加 `spark.shuffle.push.merge.finalizeThreads` 处理并发合并
- **效果**：确保大型 shuffle 有足够时间和资源完成合并

### 8. **小型 Shuffle 快速完成场景**
- **场景描述**：shuffle 数据量较小（小于 500MB）
- **配置行为**：
  - 驱动器会立即完成 shuffle 输出，不等待合并
  - 通过 `spark.shuffle.push.minShuffleSizeToWait` 控制
- **效果**：小 shuffle 快速完成，减少延迟

### 9. **部分推送完成场景**
- **场景描述**：允许部分 map 分区完成推送后开始合并
- **配置调整**：
  - 降低 `spark.shuffle.push.minCompletedPushRatio`（如 0.8）
  - 允许 80% 的分区完成推送后开始合并
- **权衡**：可能提升性能，但需要确保数据完整性

### 10. **合并位置缓存管理场景**
- **场景描述**：大量外部 shuffle 服务节点
- **配置调整**：
  - 增加 `spark.shuffle.push.maxRetainedMergerLocations` 缓存更多位置
  - 适用于大规模集群
- **注意**：过多缓存可能占用内存

## 三、总结

### 核心价值

1. **Push-based Shuffle 控制**：通过客户端和服务器端配置协同工作，灵活启用基于推送的 shuffle 功能

2. **智能合并位置管理**：
   - 通过动态比例和静态阈值确保有足够的合并位置
   - 适应不同规模的分区数量
   - 静态阈值保护小规模作业，动态比例适应大规模作业

3. **块推送优化**：
   - 通过块大小和批次大小控制推送策略
   - 平衡推送开销和随机读取性能
   - 避免推送过大的块（已有高效获取机制）

4. **合并完成策略**：
   - 根据 shuffle 大小决定是否等待合并完成
   - 小 shuffle 快速完成，大 shuffle 等待合并
   - 多线程处理并发合并请求

5. **性能与延迟平衡**：
   - 通过超时和完成比例控制合并行为
   - 在数据完整性和性能之间找到平衡点

### 关键配置原则

1. **启用 Push-based Shuffle**：
   - 必须客户端和服务器端同时配置
   - 确保有足够的外部 shuffle 服务节点
   - 版本要求：Spark 3.2.0+

2. **合并位置要求**：
   - 实际要求 = max(静态阈值, 分区数 × 比例)
   - 确保集群中有足够的外部 shuffle 服务
   - 缓存位置数应大于等于实际要求

3. **块大小配置**：
   - `maxBlockSizeToPush` < `maxBlockBatchSize`
   - `maxBlockBatchSize` > `spark.storage.memoryMapThreshold`
   - 根据实际块大小分布调整

4. **合并完成策略**：
   - 大 shuffle（>500MB）等待合并完成
   - 小 shuffle（<500MB）立即完成
   - 根据实际数据量调整阈值

5. **线程配置**：
   - 推送线程数默认等于执行器核心数
   - 合并完成线程数根据并发需求调整
   - 避免过多线程导致资源竞争

### 注意事项

- **客户端配置**：这些配置在 Spark 应用中设置，与服务器端配置配合使用
- **版本要求**：Push-based shuffle 功能需要 Spark 3.2.0 或更高版本
- **外部 Shuffle 服务**：必须启用外部 shuffle 服务，且版本至少为 2.3.0
- **合并位置要求**：确保有足够的外部 shuffle 服务节点满足合并位置要求
- **块大小权衡**：大块推送可能产生额外开销，小块推送可能增加随机读取
- **超时设置**：合并完成超时过长可能导致性能下降，过短可能导致合并未完成
- **完成比例**：降低完成比例可能提升性能，但需确保数据完整性

### 配置建议

- **生产环境启用**：
  - 客户端：`spark.shuffle.push.enabled=true`
  - 服务器端：`spark.shuffle.push.server.mergedShuffleFileManagerImpl=org.apache.spark.network.shuffle.RemoteBlockPushResolver`
  - 确保有足够的外部 shuffle 服务节点

- **性能调优**：
  - 根据实际工作负载调整块大小和批次大小
  - 根据 shuffle 数据量调整等待阈值
  - 根据并发需求调整线程数

- **监控指标**：
  - 关注推送成功率
  - 监控合并完成时间
  - 观察网络和磁盘 I/O 性能

- **测试验证**：
  - 在生产环境应用前，在测试环境验证配置效果
  - 对比启用前后的性能指标
  - 确保数据完整性和正确性

这些配置选项共同控制 Spark 客户端端的 Push-based Shuffle 行为，与服务器端配置配合使用，可以显著提升大规模 shuffle 操作的性能和效率。合理配置这些参数可以在推送开销、网络效率、磁盘 I/O 和合并性能之间找到最佳平衡点。

