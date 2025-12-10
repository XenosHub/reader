# 外部 Shuffle 服务（服务器端）配置选项

## External Shuffle Service Server-side Configuration Options

以下配置选项用于外部 Shuffle 服务的服务器端配置。

### 配置属性

| 属性名称 | 默认值 | 含义 | 起始版本 |
| --- | --- | --- | --- |
| `spark.shuffle.push.server.mergedShuffleFileManagerImpl` | `org.apache.spark.network.shuffle.NoOpMergedShuffleFileManager` | 实现 `MergedShuffleFileManager` 的类名，用于管理基于推送的 shuffle。作为服务器端配置项，用于禁用或启用基于推送的 shuffle。默认情况下，服务器端的基于推送的 shuffle 是禁用的。要在服务器端启用基于推送的 shuffle，请将此配置设置为 `org.apache.spark.network.shuffle.RemoteBlockPushResolver`。 | 3.2.0 |
| `spark.shuffle.push.server.minChunkSizeInMergedShuffleFile` | `2m` | 在基于推送的 shuffle 过程中，将合并的 shuffle 文件分割成多个块时，每个块的最小大小。合并的 shuffle 文件由多个小的 shuffle 块组成。在单个磁盘 I/O 中获取完整的合并 shuffle 文件会增加客户端和外部 shuffle 服务的内存需求。相反，外部 shuffle 服务以 MB 大小的块提供合并文件。此配置控制块的最大大小。每个合并的 shuffle 文件将生成一个相应的索引文件，指示块边界。设置过高会增加客户端和外部 shuffle 服务的内存需求。设置过低会不必要地增加对外部 shuffle 服务的 RPC 请求总数。 | 3.2.0 |
| `spark.shuffle.push.server.mergedIndexCacheSize` | `100m` | 内存中可用于存储合并索引文件的基于推送的 shuffle 缓存的最大大小。此缓存是 `spark.shuffle.service.index.cache.size` 配置的缓存之外的附加缓存。 | 3.2.0 |

## 一、主要功能分类

### 1. **Push-based Shuffle 启用/禁用配置**
- `spark.shuffle.push.server.mergedShuffleFileManagerImpl`：控制基于推送的 shuffle 的启用或禁用
  - 默认值：`org.apache.spark.network.shuffle.NoOpMergedShuffleFileManager`（禁用）
  - 启用值：`org.apache.spark.network.shuffle.RemoteBlockPushResolver`（启用）

### 2. **Shuffle 文件分块配置**
- `spark.shuffle.push.server.minChunkSizeInMergedShuffleFile`：合并 shuffle 文件分块的最小大小（默认 2MB）
  - 控制如何将合并的 shuffle 文件分割成多个块
  - 影响内存使用和 RPC 请求数量

### 3. **索引缓存配置**
- `spark.shuffle.push.server.mergedIndexCacheSize`：合并索引文件的内存缓存最大大小（默认 100MB）
  - 用于存储合并索引文件，提升查询性能
  - 是 `spark.shuffle.service.index.cache.size` 之外的额外缓存

## 二、应用场景

### 1. **启用 Push-based Shuffle 场景**
- **场景描述**：需要启用基于推送的 shuffle 以提升性能
- **配置方法**：设置 `spark.shuffle.push.server.mergedShuffleFileManagerImpl=org.apache.spark.network.shuffle.RemoteBlockPushResolver`
- **适用情况**：
  - 大规模 shuffle 操作
  - 需要减少网络 I/O 和磁盘 I/O
  - 提升 shuffle 性能的场景

### 2. **内存受限环境场景**
- **场景描述**：执行器或外部 shuffle 服务内存有限
- **配置调整**：
  - 降低 `spark.shuffle.push.server.minChunkSizeInMergedShuffleFile`（如 1MB）以减少内存占用
  - 降低 `spark.shuffle.push.server.mergedIndexCacheSize` 以节省内存
- **权衡**：较小的块大小会增加 RPC 请求数量

### 3. **高吞吐量场景**
- **场景描述**：需要最大化 shuffle 吞吐量
- **配置调整**：
  - 增加 `spark.shuffle.push.server.minChunkSizeInMergedShuffleFile`（如 4MB 或 8MB）以减少 RPC 请求
  - 增加 `spark.shuffle.push.server.mergedIndexCacheSize` 以提升索引查询性能
- **前提条件**：确保有足够的内存支持更大的块和缓存

### 4. **网络带宽受限场景**
- **场景描述**：网络带宽是瓶颈
- **配置调整**：
  - 增加 `spark.shuffle.push.server.minChunkSizeInMergedShuffleFile` 以减少 RPC 请求次数
  - 通过减少网络往返次数来提升效率
- **注意**：需要平衡内存使用和网络效率

### 5. **大规模索引查询场景**
- **场景描述**：频繁查询合并 shuffle 文件的索引
- **配置调整**：增加 `spark.shuffle.push.server.mergedIndexCacheSize` 以缓存更多索引文件
- **效果**：减少磁盘 I/O，提升查询性能

### 6. **混合工作负载场景**
- **场景描述**：同时运行多个 Spark 应用，共享外部 shuffle 服务
- **配置考虑**：
  - 合理设置缓存大小，避免单个应用占用过多资源
  - 根据整体内存情况调整 `spark.shuffle.push.server.mergedIndexCacheSize`

## 三、总结

### 核心价值

1. **Push-based Shuffle 控制**：通过 `mergedShuffleFileManagerImpl` 配置灵活启用或禁用基于推送的 shuffle，适应不同的性能需求

2. **内存与性能平衡**：通过 `minChunkSizeInMergedShuffleFile` 在内存使用和 RPC 请求数量之间找到平衡点
   - 较大块：减少 RPC 请求，但增加内存需求
   - 较小块：降低内存占用，但增加 RPC 请求

3. **索引缓存优化**：通过 `mergedIndexCacheSize` 优化索引文件的访问性能，减少磁盘 I/O

### 关键配置原则

1. **块大小选择**：
   - 默认 2MB 通常是最佳平衡点
   - 内存充足时，可适当增大以提升性能
   - 内存受限时，可适当减小以节省内存

2. **缓存大小设置**：
   - 默认 100MB 适用于大多数场景
   - 索引查询频繁时，可适当增大
   - 多应用共享时，需考虑整体资源分配

3. **启用 Push-based Shuffle**：
   - 需要客户端和服务器端同时配置
   - 确保外部 shuffle 服务版本支持（至少 2.3.0）

### 注意事项

- **服务器端配置**：这些配置需要在运行外部 Shuffle 服务的节点上设置，而不是在 Spark 应用中
- **版本要求**：Push-based shuffle 功能需要 Spark 3.2.0 或更高版本
- **内存影响**：增加块大小和缓存大小会增加内存使用，需要确保有足够的可用内存
- **性能权衡**：块大小和缓存大小的调整需要在内存使用、网络效率和查询性能之间找到平衡
- **索引文件**：每个合并的 shuffle 文件都会生成对应的索引文件，索引缓存可以显著提升查询性能

### 配置建议

- **生产环境默认**：保持默认值通常是最安全的选择
- **性能调优**：根据实际工作负载和资源情况逐步调整
- **监控指标**：关注内存使用、RPC 请求数量和 shuffle 性能指标
- **测试验证**：在生产环境应用前，在测试环境验证配置效果

这些配置选项共同控制外部 Shuffle 服务在服务器端的行为，特别是与基于推送的 shuffle 相关的功能。合理配置这些参数可以显著提升 Spark 应用的 shuffle 性能和资源利用效率。

