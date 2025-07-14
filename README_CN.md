# Milvus Spark Connector 示例

本仓库包含了完整的示例，展示如何在 Scala 和 Python 应用程序中使用 Milvus Spark Connector。

## 概述

Milvus Spark Connector 提供了 Apache Spark 与 Milvus 向量数据库之间的无缝集成，支持大规模高效的数据处理和向量操作。本示例仓库展示了各种使用模式和最佳实践。

## 前置条件

- Apache Spark 环境搭建
- Milvus 实例运行并可访问
- 关于详细的 Spark 环境搭建和 Spark connector 的两种新格式参数，请参考主仓库：**[milvus-spark-connector](https://github.com/SimFG/milvus-spark-connector)**

## 项目结构

```
├── src/main/scala/example/
│   ├── HelloDemo.scala           # 基础使用示例
│   ├── read/                     # 数据读取示例
│   │   ├── MilvusDemo.scala      # 集合和分段读取
│   │   ├── LocalBinlogDemo.scala # 本地 binlog 文件读取
│   │   └── RemoteBinlogDemo.scala # 远程 binlog 文件读取
│   └── write/                    # 数据写入示例
│       ├── FloatVectorDemo.scala  # 浮点向量数据写入
│       └── DoubleVectorDemo.scala # 双精度向量数据写入
└── python/
    ├── pyspark_milvus_demo.py    # Python PySpark 示例
    ├── config.py                 # 配置文件
    └── .env.example              # 环境配置模板
```

## 示例演示

### 基础使用

#### [HelloDemo.scala](src/main/scala/example/HelloDemo.scala)
最基础的示例，展示如何连接到 Milvus 并使用 Spark connector 读取数据。

**主要特性：**
- 简单的 Milvus 连接设置
- 从集合中基本数据读取

#### DataFrame 操作
一旦从 Milvus 集合获得 DataFrame，您就可以利用 Spark DataFrame API 进行各种数据操作：

**1. 选择特定列**
```scala
// Scala
val selectedDF = df.select("id", "vector", "metadata")

// Python
selected_df = df.select("id", "vector", "metadata")
```

**2. 数据过滤**
```scala
// Scala
val filteredDF = df.filter($"id" > 100)
val complexFilterDF = df.filter($"metadata" === "important" && $"score" > 0.8)

// Python
filtered_df = df.filter(df.id > 100)
complex_filter_df = df.filter((df.metadata == "important") & (df.score > 0.8))
```

**3. 记录计数**
```scala
// Scala
val totalCount = df.count()
val filteredCount = df.filter($"score" > 0.5).count()

// Python
total_count = df.count()
filtered_count = df.filter(df.score > 0.5).count()
```

**4. 分组和聚合**
```scala
// Scala
val groupedDF = df.groupBy("category").agg(
  count("*").as("count"),
  avg("score").as("avg_score"),
  max("timestamp").as("latest_timestamp")
)

// Python
grouped_df = df.groupBy("category").agg(
    count("*").alias("count"),
    avg("score").alias("avg_score"),
    max("timestamp").alias("latest_timestamp")
)
```

**5. 数据排序**
```scala
// Scala
val sortedDF = df.orderBy($"score".desc, $"timestamp".asc)

// Python
sorted_df = df.orderBy(df.score.desc(), df.timestamp.asc())
```

**6. 其他操作**
```scala
// Scala
// 显示前 20 行
df.show(20)

// 获取模式信息
df.printSchema()

// 收集到本地数组（大数据集请谨慎使用）
val localData = df.collect()

// Python
# 显示前 20 行
df.show(20)

# 获取模式信息
df.printSchema()

# 收集到本地数组（大数据集请谨慎使用）
local_data = df.collect()
```

更多全面的 DataFrame 操作，请参考官方 Spark DataFrame 文档：https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html

### 数据读取示例

#### [MilvusDemo.scala](src/main/scala/example/read/MilvusDemo.scala)
全面的示例，展示从 Milvus 集合读取数据的各种方式。

**主要特性：**
- 读取完整集合数据
- 读取集合中的特定字段
- 应用时间戳过滤器
- 读取特定分段数据
- 使用 S3 路径读取分段数据

#### [LocalBinlogDemo.scala](src/main/scala/example/read/LocalBinlogDemo.scala)
演示读取本地存储的 Milvus binlog 文件。

**主要特性：**
- 读取本地插入 binlog 文件
- 读取本地删除 binlog 文件
- 支持各种数据类型（varchar、short、float vector）

#### [RemoteBinlogDemo.scala](src/main/scala/example/read/RemoteBinlogDemo.scala)
展示如何从远程存储（S3）读取 Milvus binlog 文件。

**主要特性：**
- 读取远程插入 binlog 文件
- 读取远程删除 binlog 文件
- S3 文件系统集成

### 数据写入示例

#### [FloatVectorDemo.scala](src/main/scala/example/write/FloatVectorDemo.scala)
演示向 Milvus 写入包含浮点向量字段的数据。

**主要特性：**
- 创建包含浮点向量字段的集合
- 浮点向量的数据模式定义
- 批量数据插入

#### [DoubleVectorDemo.scala](src/main/scala/example/write/DoubleVectorDemo.scala)
展示向 Milvus 写入包含双精度向量字段的数据。

**主要特性：**
- 创建包含双精度向量字段的集合
- 双精度向量的数据模式定义
- 批量数据插入

> **重要提示：** 写入数据时，务必确保您的 Spark DataFrame 模式与 Milvus 集合中的向量数据类型匹配。对于浮点向量使用 `FloatType`，对于双精度向量使用 `DoubleType`。

### Python 示例

#### [pyspark_milvus_demo.py](python/pyspark_milvus_demo.py)
使用 PySpark 与 Milvus connector 的全面 Python 示例。

**主要特性：**
- PySpark 会话配置
- 使用 Python 读取 Milvus 数据
- S3 集成设置
- 数据分析和统计
- 错误处理和验证

## 配置

### Scala 示例
大多数 Scala 示例为了简单起见使用硬编码配置：

```scala
val uri = "http://localhost:19530"
val token = "root:Milvus"
val collectionName = "your_collection_name"
```

### Python 示例
Python 示例使用配置文件方式。请在 `python/config.py` 中更新您的实际值：

```python
MILVUS_CONFIG = {
    "uri": "your_milvus_uri",
    "token": "your_milvus_token",
    "collection_name": "your_collection_name",
    # ... 其他配置
}
```

## 运行示例

### Scala 示例

1. 确保您的 Spark 环境已正确配置。如果您不确定如何搭建 Spark 环境，请参考主仓库：[milvus-spark-connector](https://github.com/SimFG/milvus-spark-connector)
2. 在每个示例文件中更新连接参数
3. 编译和打包项目：

```bash
sbt clean compile package
```

4. 使用 spark-submit 运行特定示例：

```bash
# HelloDemo
spark-submit-wrapper --jars /xxx/spark-connector-assembly-x.x.x-SNAPSHOT.jar --class "example.HelloDemo" /xxx/milvus-spark-connector-example_2.13-0.1.0-SNAPSHOT.jar

# 特定读取/写入示例
spark-submit-wrapper --jars /xxx/spark-connector-assembly-x.x.x-SNAPSHOT.jar --class "example.read.MilvusDemo" /xxx/milvus-spark-connector-example_2.13-0.1.0-SNAPSHOT.jar
spark-submit-wrapper --jars /xxx/spark-connector-assembly-x.x.x-SNAPSHOT.jar --class "example.write.FloatVectorDemo" /xxx/milvus-spark-connector-example_2.13-0.1.0-SNAPSHOT.jar
```

### Python 示例

我们推荐使用 [uv](https://docs.astral.sh/uv/getting-started/installation/) 进行 Python 环境管理：

1. 如果尚未安装 uv，请先安装：
   - 访问 [uv 安装指南](https://docs.astral.sh/uv/getting-started/installation/) 获取详细说明

2. 进入 python 目录：
```bash
cd python
```

3. 复制示例环境文件并进行配置：
```bash
cp .env.example .env
# 编辑 .env 文件，填入您的实际 Milvus 和 S3 配置
```

4. 使用 uv 运行示例：
```bash
uv run pyspark_milvus_demo.py
```

## 其他资源

- **主仓库**：[milvus-spark-connector](https://github.com/SimFG/milvus-spark-connector)
- **Milvus 文档**：[https://milvus.io/docs](https://milvus.io/docs)
- **Apache Spark 文档**：[https://spark.apache.org/docs/latest/](https://spark.apache.org/docs/latest/)

## 支持

如有问题、问题或贡献，请参考主仓库 [milvus-spark-connector](https://github.com/SimFG/milvus-spark-connector)。 