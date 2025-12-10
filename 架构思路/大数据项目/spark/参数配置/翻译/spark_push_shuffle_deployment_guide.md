# Push-based Shuffle 服务端与客户端配置关系及部署指南

## 一、服务端与客户端的关系

### 1.1 基本概念

**服务端（Server-side）配置：**
- 配置位置：运行**外部 Shuffle 服务（External Shuffle Service）**的节点
- 配置文件：通常在 `spark-defaults.conf` 或通过启动参数配置
- 作用范围：影响所有连接到该外部 Shuffle 服务的 Spark 应用
- 配置项：以 `spark.shuffle.push.server.*` 开头

**客户端（Client-side）配置：**
- 配置位置：**Spark 应用**中（通过 SparkConf、命令行参数或配置文件）
- 配置文件：`spark-defaults.conf`、`spark-submit` 参数或应用代码
- 作用范围：仅影响配置了该选项的 Spark 应用
- 配置项：以 `spark.shuffle.push.*` 开头（不含 `server`）

### 1.2 协同工作关系

```
┌─────────────────────────────────────────────────────────────┐
│                    Spark 应用（客户端）                        │
│  ┌──────────────────────────────────────────────────────┐    │
│  │  Executor 1                                         │    │
│  │  - 生成 shuffle 块                                  │    │
│  │  - 推送块到外部 Shuffle 服务                        │    │
│  │  - 配置：spark.shuffle.push.enabled=true           │    │
│  └──────────────────────────────────────────────────────┘    │
│  ┌──────────────────────────────────────────────────────┐    │
│  │  Executor 2                                         │    │
│  │  - 生成 shuffle 块                                  │    │
│  │  - 推送块到外部 Shuffle 服务                        │    │
│  └──────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                        │
                        │ 推送 shuffle 块
                        ▼
┌─────────────────────────────────────────────────────────────┐
│           外部 Shuffle 服务（服务端）                        │
│  ┌──────────────────────────────────────────────────────┐    │
│  │  Node 1: External Shuffle Service                   │    │
│  │  - 接收推送的 shuffle 块                             │    │
│  │  - 合并 shuffle 块                                  │    │
│  │  - 提供合并后的块给 Executor                         │    │
│  │  - 配置：spark.shuffle.push.server.                 │    │
│  │         mergedShuffleFileManagerImpl=               │    │
│  │         RemoteBlockPushResolver                      │    │
│  └──────────────────────────────────────────────────────┘    │
│  ┌──────────────────────────────────────────────────────┐    │
│  │  Node 2: External Shuffle Service                   │    │
│  │  - 接收推送的 shuffle 块                             │    │
│  │  - 合并 shuffle 块                                  │    │
│  └──────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### 1.3 工作流程

1. **客户端（Executor）**：
   - 生成 shuffle 块
   - 根据 `spark.shuffle.push.enabled` 决定是否推送
   - 如果启用，将块推送到外部 Shuffle 服务

2. **服务端（External Shuffle Service）**：
   - 根据 `spark.shuffle.push.server.mergedShuffleFileManagerImpl` 决定是否接收
   - 如果启用，接收推送的块并合并
   - 将合并后的块提供给后续的 shuffle fetch

3. **协同要求**：
   - **必须两端都启用**才能正常工作
   - 如果只有客户端启用，服务端会拒绝推送请求
   - 如果只有服务端启用，客户端不会推送，功能无效

## 二、实际场景操作步骤

### 2.1 场景一：Standalone 模式部署

#### 步骤 1：配置服务端（外部 Shuffle 服务）

在每个 Worker 节点上，编辑外部 Shuffle 服务的配置文件：

**方法 1：通过 spark-defaults.conf**

```bash
# 编辑 spark-defaults.conf
vi $SPARK_HOME/conf/spark-defaults.conf
```

添加以下配置：

```properties
# 启用 Push-based Shuffle 服务端
spark.shuffle.push.server.mergedShuffleFileManagerImpl=org.apache.spark.network.shuffle.RemoteBlockPushResolver

# 可选：调整分块大小（默认 2m）
spark.shuffle.push.server.minChunkSizeInMergedShuffleFile=2m

# 可选：调整索引缓存大小（默认 100m）
spark.shuffle.push.server.mergedIndexCacheSize=100m
```

**方法 2：通过启动参数**

```bash
# 启动外部 Shuffle 服务时添加参数
$SPARK_HOME/sbin/start-shuffle-service.sh \
  --conf spark.shuffle.push.server.mergedShuffleFileManagerImpl=org.apache.spark.network.shuffle.RemoteBlockPushResolver \
  --conf spark.shuffle.push.server.minChunkSizeInMergedShuffleFile=2m \
  --conf spark.shuffle.push.server.mergedIndexCacheSize=100m
```

#### 步骤 2：重启外部 Shuffle 服务

```bash
# 停止外部 Shuffle 服务
$SPARK_HOME/sbin/stop-shuffle-service.sh

# 启动外部 Shuffle 服务
$SPARK_HOME/sbin/start-shuffle-service.sh
```

#### 步骤 3：配置客户端（Spark 应用）

**方法 1：通过 spark-defaults.conf（全局配置）**

```bash
# 编辑 spark-defaults.conf
vi $SPARK_HOME/conf/spark-defaults.conf
```

添加以下配置：

```properties
# 启用 Push-based Shuffle 客户端
spark.shuffle.push.enabled=true

# 可选：调整推送线程数（默认等于 executor cores）
spark.shuffle.push.numPushThreads=8

# 可选：调整最大推送块大小（默认 1m）
spark.shuffle.push.maxBlockSizeToPush=1m

# 可选：调整批次大小（默认 3m）
spark.shuffle.push.maxBlockBatchSize=3m
```

**方法 2：通过 spark-submit 参数（应用级配置）**

```bash
spark-submit \
  --conf spark.shuffle.push.enabled=true \
  --conf spark.shuffle.push.numPushThreads=8 \
  --conf spark.shuffle.push.maxBlockSizeToPush=1m \
  --conf spark.shuffle.push.maxBlockBatchSize=3m \
  --class com.example.MyApp \
  my-app.jar
```

**方法 3：在应用代码中配置**

```scala
val spark = SparkSession.builder()
  .appName("MyApp")
  .config("spark.shuffle.push.enabled", "true")
  .config("spark.shuffle.push.numPushThreads", "8")
  .config("spark.shuffle.push.maxBlockSizeToPush", "1m")
  .getOrCreate()
```

### 2.2 场景二：YARN 模式部署

#### 步骤 1：配置服务端（NodeManager 上的外部 Shuffle 服务）

在每个 NodeManager 节点上，编辑配置文件：

```bash
# 编辑 yarn-site.xml 或 spark-defaults.conf
vi $SPARK_HOME/conf/spark-defaults.conf
```

添加服务端配置：

```properties
# 启用 Push-based Shuffle 服务端
spark.shuffle.push.server.mergedShuffleFileManagerImpl=org.apache.spark.network.shuffle.RemoteBlockPushResolver
spark.shuffle.push.server.minChunkSizeInMergedShuffleFile=2m
spark.shuffle.push.server.mergedIndexCacheSize=100m
```

#### 步骤 2：重启 NodeManager（如果需要）

```bash
# 在 YARN ResourceManager 节点执行
yarn --daemon stop nodemanager
yarn --daemon start nodemanager
```

#### 步骤 3：配置客户端（提交 Spark 应用时）

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.shuffle.push.enabled=true \
  --conf spark.shuffle.push.numPushThreads=8 \
  --conf spark.shuffle.push.maxBlockSizeToPush=1m \
  --class com.example.MyApp \
  my-app.jar
```

### 2.3 场景三：Kubernetes 模式部署

#### 步骤 1：配置服务端（通过 ConfigMap）

创建 ConfigMap：

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: spark-shuffle-service-config
  namespace: spark
data:
  spark-defaults.conf: |
    spark.shuffle.push.server.mergedShuffleFileManagerImpl=org.apache.spark.network.shuffle.RemoteBlockPushResolver
    spark.shuffle.push.server.minChunkSizeInMergedShuffleFile=2m
    spark.shuffle.push.server.mergedIndexCacheSize=100m
```

#### 步骤 2：在 External Shuffle Service Deployment 中引用

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: spark-shuffle-service
spec:
  template:
    spec:
      containers:
      - name: shuffle-service
        volumeMounts:
        - name: config
          mountPath: /opt/spark/conf
      volumes:
      - name: config
        configMap:
          name: spark-shuffle-service-config
```

#### 步骤 3：配置客户端（SparkApplication 或 SparkSubmit）

```bash
spark-submit \
  --master k8s://https://kubernetes-api-server:443 \
  --conf spark.shuffle.push.enabled=true \
  --conf spark.shuffle.push.numPushThreads=8 \
  --class com.example.MyApp \
  my-app.jar
```

## 三、配置验证

### 3.1 检查服务端配置

**方法 1：检查配置文件**

```bash
# 检查 spark-defaults.conf
grep "spark.shuffle.push.server" $SPARK_HOME/conf/spark-defaults.conf
```

**方法 2：检查运行中的服务**

```bash
# 查看外部 Shuffle 服务日志
tail -f $SPARK_HOME/logs/spark-*-org.apache.spark.deploy.worker.Worker-*.out

# 查找相关配置信息
grep "mergedShuffleFileManagerImpl" $SPARK_HOME/logs/*.log
```

### 3.2 检查客户端配置

**方法 1：在 Spark UI 中查看**

1. 打开 Spark UI（通常是 `http://driver-host:4040`）
2. 进入 "Environment" 标签页
3. 搜索 "shuffle.push" 相关配置

**方法 2：通过 Spark 应用日志**

```bash
# 查看应用日志
grep "shuffle.push" spark-application.log
```

**方法 3：在应用代码中打印配置**

```scala
val conf = spark.sparkContext.getConf
println("Push shuffle enabled: " + conf.get("spark.shuffle.push.enabled", "false"))
println("Push threads: " + conf.get("spark.shuffle.push.numPushThreads", "default"))
```

### 3.3 功能验证

**验证 Push-based Shuffle 是否生效：**

1. **查看 Spark UI 的 Executors 标签页**
   - 检查是否有 "Shuffle Push" 相关的指标

2. **查看应用日志**
   - 查找 "Push-based shuffle" 相关的日志信息
   - 确认没有 "Push shuffle disabled" 的警告

3. **性能对比测试**
   - 运行相同的作业，对比启用前后的性能
   - 观察 shuffle 阶段的执行时间

## 四、常见问题与解决方案

### 4.1 问题：Push-based Shuffle 未生效

**症状：**
- 应用日志显示 "Push shuffle disabled"
- Spark UI 中没有 push shuffle 相关指标

**可能原因：**
1. 服务端未启用
2. 客户端未启用
3. 合并位置不足

**解决方案：**

```bash
# 1. 检查服务端配置
grep "mergedShuffleFileManagerImpl" $SPARK_HOME/conf/spark-defaults.conf

# 2. 检查客户端配置
grep "spark.shuffle.push.enabled" $SPARK_HOME/conf/spark-defaults.conf

# 3. 检查外部 Shuffle 服务是否运行
jps | grep ExternalShuffleService

# 4. 检查合并位置数量
# 确保有足够的外部 Shuffle 服务节点
# 要求：max(5, 分区数 × 0.05)
```

### 4.2 问题：推送失败

**症状：**
- 日志中出现 "Failed to push block" 错误
- 网络连接错误

**可能原因：**
1. 网络连接问题
2. 外部 Shuffle 服务未运行
3. 端口被占用

**解决方案：**

```bash
# 1. 检查外部 Shuffle 服务状态
jps | grep ExternalShuffleService

# 2. 检查网络连接
telnet shuffle-service-host 7337

# 3. 检查防火墙规则
iptables -L | grep 7337

# 4. 查看外部 Shuffle 服务日志
tail -f $SPARK_HOME/logs/spark-*-ExternalShuffleService-*.out
```

### 4.3 问题：内存不足

**症状：**
- OOM 错误
- 外部 Shuffle 服务崩溃

**可能原因：**
1. 索引缓存过大
2. 分块大小过大
3. 并发推送过多

**解决方案：**

```properties
# 减少索引缓存大小
spark.shuffle.push.server.mergedIndexCacheSize=50m

# 减少分块大小
spark.shuffle.push.server.minChunkSizeInMergedShuffleFile=1m

# 减少推送线程数
spark.shuffle.push.numPushThreads=4
```

### 4.4 问题：性能未提升

**症状：**
- 启用后性能没有明显改善
- 甚至性能下降

**可能原因：**
1. Shuffle 数据量太小
2. 块大小配置不当
3. 网络带宽不足

**解决方案：**

```properties
# 1. 检查 shuffle 数据大小
# 如果 shuffle 数据 < 500MB，可能不会等待合并

# 2. 调整块大小
spark.shuffle.push.maxBlockSizeToPush=512k  # 对于小块
spark.shuffle.push.maxBlockSizeToPush=2m    # 对于大块

# 3. 增加合并完成超时
spark.shuffle.push.finalize.timeout=20s
```

## 五、最佳实践

### 5.1 配置建议

**服务端配置：**
```properties
# 生产环境推荐配置
spark.shuffle.push.server.mergedShuffleFileManagerImpl=org.apache.spark.network.shuffle.RemoteBlockPushResolver
spark.shuffle.push.server.minChunkSizeInMergedShuffleFile=2m
spark.shuffle.push.server.mergedIndexCacheSize=100m
```

**客户端配置：**
```properties
# 生产环境推荐配置
spark.shuffle.push.enabled=true
spark.shuffle.push.numPushThreads=8  # 根据 executor cores 调整
spark.shuffle.push.maxBlockSizeToPush=1m
spark.shuffle.push.maxBlockBatchSize=3m
spark.shuffle.push.finalize.timeout=10s
spark.shuffle.push.minShuffleSizeToWait=500m
```

### 5.2 部署检查清单

- [ ] 确认 Spark 版本 >= 3.2.0
- [ ] 确认外部 Shuffle 服务版本 >= 2.3.0
- [ ] 服务端配置已添加到所有外部 Shuffle 服务节点
- [ ] 客户端配置已添加到 Spark 应用
- [ ] 外部 Shuffle 服务已重启
- [ ] 有足够的外部 Shuffle 服务节点（满足合并位置要求）
- [ ] 网络连接正常
- [ ] 已进行功能验证
- [ ] 已进行性能测试

### 5.3 监控指标

**关键指标：**
- Shuffle Push 成功率
- Shuffle Push 吞吐量
- 合并完成时间
- 网络 I/O 使用率
- 内存使用情况

**监控命令：**
```bash
# 查看 Spark UI
http://driver-host:4040

# 查看外部 Shuffle 服务指标（如果启用了指标收集）
curl http://shuffle-service-host:metrics-port/metrics
```

## 六、总结

### 6.1 关键要点

1. **必须两端都启用**：服务端和客户端必须同时配置才能工作
2. **配置位置不同**：
   - 服务端：外部 Shuffle 服务节点
   - 客户端：Spark 应用
3. **版本要求**：Spark 3.2.0+，外部 Shuffle 服务 2.3.0+
4. **合并位置要求**：确保有足够的外部 Shuffle 服务节点

### 6.2 配置关系总结

| 配置类型 | 配置位置 | 作用范围 | 关键配置项 |
|---------|---------|---------|-----------|
| 服务端 | 外部 Shuffle 服务节点 | 所有连接的应用 | `spark.shuffle.push.server.mergedShuffleFileManagerImpl` |
| 客户端 | Spark 应用 | 单个应用 | `spark.shuffle.push.enabled` |

### 6.3 工作流程总结

1. **客户端（Executor）** 生成 shuffle 块
2. **客户端** 检查 `spark.shuffle.push.enabled`，如果启用则推送
3. **服务端（External Shuffle Service）** 检查 `mergedShuffleFileManagerImpl`，如果启用则接收
4. **服务端** 合并接收到的块
5. **客户端（Reducer）** 从服务端获取合并后的块

通过正确配置服务端和客户端，Push-based Shuffle 可以显著提升大规模 shuffle 操作的性能。

