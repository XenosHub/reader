## Spark Runtime Environment（运行时环境）配置解析

### 一、主要功能分类

#### 1. JVM 运行时配置

驱动器 JVM 配置：

- spark.driver.extraClassPath：驱动器额外类路径

- spark.driver.defaultJavaOptions：默认 JVM 选项（管理员设置，如 GC 设置）

- spark.driver.extraJavaOptions：额外 JVM 选项（用户设置，如 GC 设置）

- spark.driver.extraLibraryPath：驱动器 JVM 启动时的特殊库路径

- spark.driver.userClassPathFirst：用户 jar 是否优先于 Spark 自身 jar（实验性）

执行器 JVM 配置：

- spark.executor.extraClassPath：执行器额外类路径（主要用于向后兼容）

- spark.executor.defaultJavaOptions：默认 JVM 选项（管理员设置，支持 ${appId} 和 ${executorId} 占位符）

- spark.executor.extraJavaOptions：额外 JVM 选项（用户设置，支持占位符）

- spark.executor.extraLibraryPath：执行器 JVM 启动时的特殊库路径

- spark.executor.userClassPathFirst：执行器端用户类路径优先（实验性）

#### 2. 执行器日志管理配置

- spark.executor.logs.rolling.strategy：日志滚动策略（time/size/disabled）

- spark.executor.logs.rolling.maxSize：日志文件最大大小（字节）

- spark.executor.logs.rolling.time.interval：基于时间的滚动间隔（daily/hourly/minutely/秒数）

- spark.executor.logs.rolling.maxRetainedFiles：保留的最新滚动日志文件数量

- spark.executor.logs.rolling.enableCompression：是否启用日志压缩

#### 3. 依赖管理配置

JAR 文件管理：

- spark.jars：逗号分隔的 JAR 文件列表（支持 glob）

- spark.jars.packages：Maven 坐标列表（groupId:artifactId:version）

- spark.jars.excludes：排除的依赖（groupId:artifactId）

- spark.jars.ivy：Ivy 用户目录路径

- spark.jars.ivySettings：Ivy 设置文件路径（自定义依赖解析）

- spark.jars.repositories：额外的远程 Maven 仓库列表

文件与归档管理：

- spark.files：放置在执行器工作目录的文件列表（支持 glob）

- spark.archives：提取到执行器工作目录的归档文件列表（支持 .jar, .tar.gz, .tgz, .zip）

Python 文件管理：

- spark.submit.pyFiles：Python 应用的 .zip, .egg, .py 文件列表（放置到 PYTHONPATH）

#### 4. Python 运行时配置

- spark.pyspark.python：驱动器和执行器使用的 Python 可执行文件

- spark.pyspark.driver.python：驱动器专用的 Python 可执行文件（默认使用 spark.pyspark.python）

- spark.python.worker.memory：每个 Python worker 进程的内存大小（聚合时使用）

- spark.python.worker.reuse：是否重用 Python worker（避免为每个任务 fork 进程）

- spark.python.profile：是否启用 Python worker 性能分析

- spark.python.profile.dump：性能分析结果转储目录

#### 5. 环境变量与安全配置

- spark.executorEnv.[EnvironmentVariableName]：添加到执行器进程的环境变量

- spark.redaction.regex：用于识别敏感信息的正则表达式（配置属性和环境变量）

- spark.redaction.string.regex：用于识别字符串中敏感信息的正则表达式（如 SQL explain 输出）

------

### 二、应用场景

#### 1. JVM 调优场景

- 使用 spark.driver.extraJavaOptions 和 spark.executor.extraJavaOptions 设置 GC 参数

- 通过 spark.executor.defaultJavaOptions 统一设置集群级 JVM 选项

- 使用占位符（${appId}, ${executorId}）为每个执行器生成独立的 GC 日志文件

示例：

spark.executor.extraJavaOptions=-verbose:gc -Xloggc:/tmp/${executorId}.gc

#### 2. 依赖冲突解决场景

- 使用 spark.driver.userClassPathFirst 或 spark.executor.userClassPathFirst 让用户 jar 优先

- 通过 spark.jars.excludes 排除冲突依赖

- 使用 spark.jars.ivySettings 配置自定义依赖解析策略

#### 3. 企业内网部署场景

- 配置 spark.jars.ivySettings 指向内网 Maven 仓库（如 Artifactory）

- 使用 spark.jars.repositories 添加内部仓库地址

- 通过 spark.jars.ivy 指定 Ivy 缓存目录

#### 4. Python 应用场景

- 配置 spark.pyspark.python 指定 Python 版本（如 Python 3.9）

- 使用 spark.python.worker.reuse=true 重用 worker，减少进程创建开销

- 设置 spark.python.worker.memory 控制聚合时的内存使用

- 通过 spark.submit.pyFiles 分发 Python 依赖包

- 启用 spark.python.profile 进行性能分析

#### 5. 日志管理场景

- 配置日志滚动策略（时间或大小）

- 设置 spark.executor.logs.rolling.maxRetainedFiles 自动清理旧日志

- 启用 spark.executor.logs.rolling.enableCompression 压缩日志节省空间

#### 6. 敏感信息保护场景

- 配置 spark.redaction.regex 自动脱敏配置中的敏感信息（如密码、token）

- 使用 spark.redaction.string.regex 脱敏 SQL explain 输出中的敏感数据

#### 7. 资源文件分发场景

- 使用 spark.files 分发配置文件、数据文件等到执行器

- 通过 spark.archives 分发压缩包并自动解压到执行器工作目录

- 使用 spark.executorEnv.* 设置执行器环境变量

#### 8. 多版本 Python 环境场景

- 使用 spark.pyspark.driver.python 和 spark.pyspark.python 分别指定驱动器和执行器的 Python 版本

- 适用于需要不同 Python 版本的场景

#### 9. 性能诊断场景

- 启用 spark.python.profile 分析 Python 代码性能

- 使用 spark.python.profile.dump 将分析结果保存到磁盘

- 通过 sc.show_profiles() 查看性能分析结果

------

### 三、总结

Runtime Environment 配置控制 Spark 应用的运行时环境，包括 JVM 参数、依赖管理、Python 环境、日志和安全设置。

核心价值：

1. JVM 调优：灵活配置 GC 和其他 JVM 参数，优化性能

1. 依赖管理：支持多种方式管理 JAR 和 Python 依赖，解决冲突

1. 环境隔离：支持自定义类路径、库路径和环境变量

1. 日志管理：自动滚动、压缩和清理，节省存储空间

1. 安全保护：自动脱敏敏感信息，保护数据安全

1. Python 支持：完整的 Python 运行时配置和性能分析能力

关键配置原则：

- Client 模式限制：驱动器相关配置（spark.driver.*）在 client 模式下不能通过 SparkConf 设置，需使用命令行参数或配置文件

- 内存设置限制：不能在 JVM 选项中设置 -Xmx，应使用 spark.driver.memory 或 spark.executor.memory

- 配置优先级：spark.*.defaultJavaOptions 会前置到 spark.*.extraJavaOptions

- 占位符使用：执行器 JVM 选项支持 ${appId} 和 ${executorId} 占位符，便于生成独立日志

- 依赖解析：Maven 坐标解析顺序：本地仓库 → Maven Central → 额外仓库

注意事项：

- spark.driver.userClassPathFirst 和 spark.executor.userClassPathFirst 是实验性功能

- spark.executor.extraClassPath 主要用于向后兼容，通常不需要设置

- Python worker 重用可以显著提升性能，特别是使用大广播变量时

- 日志滚动和压缩可以防止日志文件过大，节省磁盘空间

- 敏感信息脱敏是生产环境的重要安全措施

这些配置共同确保 Spark 应用在不同环境下的稳定运行、性能优化和安全合规。