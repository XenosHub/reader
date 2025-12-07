# RDD 详解：第一个 RDD 的创建、分区控制与并行度配置

## 一、RDD 核心概念

### 1.1 RDD 定义

```56:82:core/src/main/scala/org/apache/spark/rdd/RDD.scala
/**
 * A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable,
 * partitioned collection of elements that can be operated on in parallel. This class contains the
 * basic operations available on all RDDs, such as `map`, `filter`, and `persist`. In addition,
 * [[org.apache.spark.rdd.PairRDDFunctions]] contains operations available only on RDDs of key-value
 * pairs, such as `groupByKey` and `join`;
 * [[org.apache.spark.rdd.DoubleRDDFunctions]] contains operations available only on RDDs of
 * Doubles; and
 * [[org.apache.spark.rdd.SequenceFileRDDFunctions]] contains operations available on RDDs that
 * can be saved as SequenceFiles.
 * All operations are automatically available on any RDD of the right type (e.g. RDD[(Int, Int)])
 * through implicit.
 *
 * Internally, each RDD is characterized by five main properties:
 *
 *  - A list of partitions
 *  - A function for computing each split
 *  - A list of dependencies on other RDDs
 *  - Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
 *  - Optionally, a list of preferred locations to compute each split on (e.g. block locations for
 *    an HDFS file)
 *
 * All of the scheduling and execution in Spark is done based on these methods, allowing each RDD
 * to implement its own way of computing itself. Indeed, users can implement custom RDDs (e.g. for
 * reading data from a new storage system) by overriding these functions. Please refer to the
 * <a href="http://people.csail.mit.edu/matei/papers/2012/nsdi_spark.pdf">Spark paper</a>
 * for more details on RDD internals.
 */
abstract class RDD[T: ClassTag](
    @transient private var _sc: SparkContext,
    @transient private var deps: Seq[Dependency[_]]
  ) extends Serializable with Logging {
```

**RDD 的五个核心属性：**

1. **分区列表（Partitions）**：RDD 被分成多个分区，每个分区是数据的子集
2. **计算函数（Compute Function）**：每个分区如何计算
3. **依赖关系（Dependencies）**：RDD 之间的依赖（窄依赖/宽依赖）
4. **分区器（Partitioner）**：可选，用于 key-value RDD 的分区策略
5. **首选位置（Preferred Locations）**：可选，数据本地性信息

### 1.2 RDD 抽象方法

```111:136:core/src/main/scala/org/apache/spark/rdd/RDD.scala
  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  @DeveloperApi
  def compute(split: Partition, context: TaskContext): Iterator[T]

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   *
   * The partitions in this array must satisfy the following property:
   *   `rdd.partitions.zipWithIndex.forall { case (partition, index) => partition.index == index }`
   */
  protected def getPartitions: Array[Partition]

  /**
   * Implemented by subclasses to return how this RDD depends on parent RDDs. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  protected def getDependencies: Seq[Dependency[_]] = deps

  /**
   * Optionally overridden by subclasses to specify placement preferences.
   */
  protected def getPreferredLocations(split: Partition): Seq[String] = Nil

  /** Optionally overridden by subclasses to specify how they are partitioned. */
  @transient val partitioner: Option[Partitioner] = None
```

---

## 二、第一个 RDD 的创建方式

### 2.1 从本地集合创建：`parallelize`

```1016:1021:core/src/main/scala/org/apache/spark/SparkContext.scala
  def parallelize[T: ClassTag](
      seq: Seq[T],
      numSlices: Int = defaultParallelism): RDD[T] = withScope {
    assertNotStopped()
    new ParallelCollectionRDD[T](this, seq, numSlices, Map[Int, Seq[String]]())
  }
```

**特点：**
- 将本地集合（List、Array 等）转换为分布式 RDD
- `numSlices` 参数控制分区数，默认是 `defaultParallelism`
- 内部使用 `ParallelCollectionRDD` 实现

**分区划分逻辑：**

```96:99:core/src/main/scala/org/apache/spark/rdd/ParallelCollectionRDD.scala
  override def getPartitions: Array[Partition] = {
    val slices = ParallelCollectionRDD.slice(data, numSlices).toArray
    slices.indices.map(i => new ParallelCollectionPartition(id, i, slices(i))).toArray
  }
```

```117:156:core/src/main/scala/org/apache/spark/rdd/ParallelCollectionRDD.scala
  def slice[T: ClassTag](seq: Seq[T], numSlices: Int): Seq[Seq[T]] = {
    if (numSlices < 1) {
      throw new IllegalArgumentException("Positive number of partitions required")
    }
    // Sequences need to be sliced at the same set of index positions for operations
    // like RDD.zip() to behave as expected
    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }
    }
    seq match {
      case r: Range =>
        positions(r.length, numSlices).zipWithIndex.map { case ((start, end), index) =>
          // If the range is inclusive, use inclusive range for the last slice
          if (r.isInclusive && index == numSlices - 1) {
            new Range.Inclusive(r.start + start * r.step, r.end, r.step)
          } else {
            new Range.Inclusive(r.start + start * r.step, r.start + (end - 1) * r.step, r.step)
          }
        }.toSeq.asInstanceOf[Seq[Seq[T]]]
      case nr: NumericRange[T] =>
        // For ranges of Long, Double, BigInteger, etc
        val slices = new ArrayBuffer[Seq[T]](numSlices)
        var r = nr
        for ((start, end) <- positions(nr.length, numSlices)) {
          val sliceSize = end - start
          slices += r.take(sliceSize).asInstanceOf[Seq[T]]
          r = r.drop(sliceSize)
        }
        slices.toSeq
      case _ =>
        val array = seq.toArray // To prevent O(n^2) operations for List etc
        positions(array.length, numSlices).map { case (start, end) =>
            array.slice(start, end).toImmutableArraySeq
        }.toSeq
    }
  }
```

**分区算法：**
- 将集合平均分成 `numSlices` 个分区
- 每个分区的范围：`[i * length / numSlices, (i+1) * length / numSlices)`
- 对于 Range 类型，使用 Range 对象优化内存

### 2.2 从文件创建：`textFile`

```1130:1136:core/src/main/scala/org/apache/spark/SparkContext.scala
  def textFile(
      path: String,
      minPartitions: Int = defaultMinPartitions): RDD[String] = withScope {
    assertNotStopped()
    hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      minPartitions).map(pair => pair._2.toString).setName(path)
  }
```

**特点：**
- 从文件系统（HDFS、本地文件系统等）读取文本文件
- `minPartitions` 参数建议最小分区数，默认是 `defaultMinPartitions`
- 内部使用 `HadoopRDD` 或 `NewHadoopRDD`，依赖 Hadoop 的 `InputFormat`

**分区划分逻辑（HadoopRDD）：**

```226:257:core/src/main/scala/org/apache/spark/rdd/HadoopRDD.scala
  override def getPartitions: Array[Partition] = {
    val jobConf = getJobConf()
    // add the credentials here as this can be called before SparkContext initialized
    SparkHadoopUtil.get.addCredentials(jobConf)
    try {
      val allInputSplits = getInputFormat(jobConf).getSplits(jobConf, minPartitions)
      val inputSplits = if (ignoreEmptySplits) {
        allInputSplits.filter(_.getLength > 0)
      } else {
        allInputSplits
      }
      if (inputSplits.length == 1 && inputSplits(0).isInstanceOf[FileSplit]) {
        val fileSplit = inputSplits(0).asInstanceOf[FileSplit]
        val path = fileSplit.getPath
        if (fileSplit.getLength > conf.get(IO_WARNING_LARGEFILETHRESHOLD)) {
          val codecFactory = new CompressionCodecFactory(jobConf)
          if (Utils.isFileSplittable(path, codecFactory)) {
            logWarning(log"Loading one large file ${MDC(PATH, path.toString)} " +
              log"with only one partition, " +
              log"we can increase partition numbers for improving performance.")
          } else {
            logWarning(log"Loading one large unsplittable file ${MDC(PATH, path.toString)} " +
              log"with only one " +
              log"partition, because the file is compressed by unsplittable compression codec.")
          }
        }
      }
      val array = new Array[Partition](inputSplits.size)
      for (i <- 0 until inputSplits.size) {
        array(i) = new HadoopPartition(id, i, inputSplits(i))
      }
      array
    } catch {
```

**分区规则：**
- 调用 Hadoop `InputFormat.getSplits()` 方法
- 每个 `InputSplit` 对应一个 RDD 分区
- 分区数受以下因素影响：
  - 文件大小（HDFS 块大小，默认 128MB）
  - `minPartitions` 参数
  - 文件是否可切分（压缩格式）

### 2.3 其他创建方式

- **`range`**：创建数字序列 RDD
- **`hadoopFile`**：从 Hadoop 文件系统读取任意格式
- **`newAPIHadoopFile`**：使用新的 Hadoop API
- **`sequenceFile`**：读取 SequenceFile
- **`objectFile`**：读取序列化对象文件

---

## 三、RDD 分区控制机制

### 3.1 分区数的决定因素

#### 3.1.1 `parallelize` 的分区控制

```scala
sc.parallelize(data, numSlices)
```

- **显式指定**：`numSlices` 参数直接指定分区数
- **默认值**：如果不指定，使用 `defaultParallelism`

#### 3.1.2 `textFile` 的分区控制

```scala
sc.textFile(path, minPartitions)
```

- **Hadoop InputFormat**：实际分区数由 `InputFormat.getSplits()` 决定
- **`minPartitions`**：只是建议值，实际可能更多
- **文件大小**：大文件会被切分成多个分区（按 HDFS 块大小）

**实际分区数计算：**
```
实际分区数 = max(
    Hadoop InputFormat 计算的分区数,
    minPartitions
)
```

### 3.2 `defaultParallelism` 的确定

```2883:2886:core/src/main/scala/org/apache/spark/SparkContext.scala
  def defaultParallelism: Int = {
    assertNotStopped()
    taskScheduler.defaultParallelism()
  }
```

**Local 模式：**

```156:156:core/src/main/scala/org/apache/spark/scheduler/local/LocalSchedulerBackend.scala
  override def defaultParallelism(): Int =
```

- `local`：1 个线程
- `local[N]`：N 个线程
- `local[*]`：CPU 核心数

**集群模式（YARN/Kubernetes）：**

```709:714:core/src/main/scala/org/apache/spark/scheduler/cluster/CoarseGrainedSchedulerBackend.scala
  override def defaultParallelism(): Int = {
    conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
  }
```

- 如果设置了 `spark.default.parallelism`，使用配置值
- 否则，使用 `max(总核心数, 2)`

### 3.3 `defaultMinPartitions` 的确定

```2888:2896:core/src/main/scala/org/apache/spark/SparkContext.scala
  /**
   * Default min number of partitions for Hadoop RDDs when not given by user
   * Notice that we use math.min so the "defaultMinPartitions" cannot be higher than 2.
   * For large files, the Hadoop InputFormat library always creates more partitions even though
   * defaultMinPartitions is 2. For small files, it can be good to process small files quickly.
   * However, usually when Spark joins a small table with a big one, we'll still spend most of
   * time on the map part of the big one anyway.
   */
  def defaultMinPartitions: Int = math.min(defaultParallelism, 2)
```

**特点：**
- 最小值为 2（即使 `defaultParallelism` 更大）
- 对于大文件，Hadoop InputFormat 会创建更多分区
- 对于小文件，使用 2 个分区可以快速处理

---

## 四、分区与并行度的关系

### 4.1 并行度 = 分区数

**核心原理：**
- **一个分区 = 一个 Task**
- **Task 数量 = 并行度**
- **分区数决定并行度上限**

### 4.2 并行度配置参数

#### 4.2.1 `spark.default.parallelism`

```45:55:core/src/main/scala/org/apache/spark/internal/config/package.scala
  private[spark] val DEFAULT_PARALLELISM =
    ConfigBuilder("spark.default.parallelism")
      .doc("Default number of partitions in RDDs returned by transformations like " +
        "join, reduceByKey, and parallelize when not set by user. " +
        "For distributed shuffle operations like reduceByKey and join, the largest number of " +
        "partitions in a parent RDD. For operations like parallelize with no parent RDDs, " +
        "it depends on the cluster manager. For example in Local mode, it defaults to the " +
        "number of cores on the local machine")
      .version("0.5.0")
      .intConf
      .createOptional
```

**作用：**
- 控制 `parallelize` 的默认分区数
- 控制 Shuffle 操作的默认分区数（如 `reduceByKey`、`join`）
- 不直接影响文件读取的分区数（文件读取由 Hadoop InputFormat 决定）

#### 4.2.2 分区数对性能的影响

**分区数过少：**
- 并行度低，资源利用不充分
- 单个分区数据量大，可能导致 OOM
- 任务执行时间长

**分区数过多：**
- 调度开销大（Task 创建、序列化、网络传输）
- 小文件问题（写操作时）
- 资源浪费（Executor 数量有限）

**最佳实践：**
- 每个分区 100-200MB 数据
- 分区数 = 2-4 × CPU 核心数
- 对于 Shuffle 操作，使用 `spark.default.parallelism` 控制

### 4.3 分区数调整方法

#### 4.3.1 创建时指定

```scala
// parallelize 指定分区数
val rdd1 = sc.parallelize(data, 10)

// textFile 指定最小分区数
val rdd2 = sc.textFile(path, 20)
```

#### 4.3.2 转换操作调整

```scala
// repartition：增加或减少分区数（会触发 Shuffle）
val rdd3 = rdd.repartition(100)

// coalesce：减少分区数（不触发 Shuffle，如果减少）
val rdd4 = rdd.coalesce(50)
```

#### 4.3.3 Shuffle 操作指定

```scala
// reduceByKey 指定分区数
val rdd5 = rdd.reduceByKey(_ + _, 100)

// join 指定分区数
val rdd6 = rdd1.join(rdd2, 100)
```

---

## 五、实际示例

### 5.1 示例 1：parallelize 分区

```scala
val sc = new SparkContext(...)

// 不指定分区数，使用 defaultParallelism
val rdd1 = sc.parallelize(1 to 1000)
println(rdd1.partitions.length)  // 取决于 defaultParallelism

// 指定分区数
val rdd2 = sc.parallelize(1 to 1000, 10)
println(rdd2.partitions.length)  // 10
```

### 5.2 示例 2：textFile 分区

```scala
// 读取 1GB 文件（HDFS 块大小 128MB）
val rdd = sc.textFile("hdfs://path/to/1gb-file.txt")

// 实际分区数 ≈ 1GB / 128MB = 8 个分区
// 即使 minPartitions=2，实际也会是 8 个分区
```

### 5.3 示例 3：配置并行度

```scala
val conf = new SparkConf()
  .set("spark.default.parallelism", "200")

val sc = new SparkContext(conf)

// parallelize 默认使用 200 个分区
val rdd1 = sc.parallelize(1 to 10000)
println(rdd1.partitions.length)  // 200

// reduceByKey 默认使用 200 个分区
val rdd2 = rdd1.map(x => (x % 10, x))
  .reduceByKey(_ + _)
println(rdd2.partitions.length)  // 200
```

---

## 六、总结

### 6.1 第一个 RDD 的创建

1. **`parallelize`**：从本地集合创建，分区数由 `numSlices` 或 `defaultParallelism` 决定
2. **`textFile`**：从文件创建，分区数由 Hadoop InputFormat 和 `minPartitions` 决定
3. **其他方式**：`hadoopFile`、`range` 等

### 6.2 分区控制机制

1. **创建时指定**：`parallelize(data, numSlices)`、`textFile(path, minPartitions)`
2. **配置参数**：`spark.default.parallelism` 影响默认分区数
3. **转换操作**：`repartition`、`coalesce` 可以调整分区数

### 6.3 并行度与分区关系

1. **并行度 = 分区数**：一个分区对应一个 Task
2. **配置影响**：`spark.default.parallelism` 影响 Shuffle 操作的默认分区数
3. **性能优化**：合理设置分区数，平衡并行度和调度开销

### 6.4 最佳实践

1. **设置 `spark.default.parallelism`**：通常为 2-4 × CPU 核心数
2. **文件读取**：让 Hadoop InputFormat 自动决定，必要时指定 `minPartitions`
3. **Shuffle 操作**：使用 `spark.default.parallelism` 或显式指定分区数
4. **监控调整**：根据实际执行情况调整分区数

