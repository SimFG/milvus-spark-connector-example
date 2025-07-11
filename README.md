# Milvus Spark Connector Examples

This repository contains comprehensive examples demonstrating how to use the Milvus Spark Connector for both Scala and Python applications.

## Overview

The Milvus Spark Connector provides seamless integration between Apache Spark and Milvus vector database, enabling efficient data processing and vector operations at scale. This example repository showcases various usage patterns and best practices.

## Prerequisites

- Apache Spark environment setup
- Milvus instance running and accessible
- For detailed Spark environment setup and the two new format parameters of the Spark connector, please refer to the main repository: **[milvus-spark-connector](https://github.com/SimFG/milvus-spark-connector)**

## Project Structure

```
├── src/main/scala/example/
│   ├── HelloDemo.scala           # Basic usage example
│   ├── read/                     # Data reading examples
│   │   ├── MilvusDemo.scala      # Collection and segment reading
│   │   ├── LocalBinlogDemo.scala # Local binlog file reading
│   │   └── RemoteBinlogDemo.scala # Remote binlog file reading
│   └── write/                    # Data writing examples
│       ├── FloatVectorDemo.scala  # Float vector data writing
│       └── DoubleVectorDemo.scala # Double vector data writing
└── python/
    ├── pyspark_milvus_demo.py    # Python PySpark demo
    ├── config.py                 # Configuration file
    └── .env.example              # Environment configuration template
```

## Demo Examples

### Basic Usage

#### [HelloDemo.scala](src/main/scala/example/HelloDemo.scala)
The most basic example demonstrating how to connect to Milvus and read data using the Spark connector.

**Key Features:**
- Simple Milvus connection setup
- Basic data reading from a collection

### Data Reading Examples

#### [MilvusDemo.scala](src/main/scala/example/read/MilvusDemo.scala)
Comprehensive example showing various ways to read data from Milvus collections.

**Key Features:**
- Read entire collection data
- Read specific fields from collection
- Apply timestamp filters
- Read specific segment data
- Read segment data with S3 path

#### [LocalBinlogDemo.scala](src/main/scala/example/read/LocalBinlogDemo.scala)
Demonstrates reading Milvus binlog files stored locally.

**Key Features:**
- Read local insert binlog files
- Read local delete binlog files
- Support for various data types (varchar, short, float vector)

#### [RemoteBinlogDemo.scala](src/main/scala/example/read/RemoteBinlogDemo.scala)
Shows how to read Milvus binlog files from remote storage (S3).

**Key Features:**
- Read remote insert binlog files
- Read remote delete binlog files
- S3 filesystem integration

### Data Writing Examples

#### [FloatVectorDemo.scala](src/main/scala/example/write/FloatVectorDemo.scala)
Demonstrates writing data with float vector fields to Milvus.

**Key Features:**
- Collection creation with float vector fields
- Data schema definition for float vectors
- Batch data insertion

#### [DoubleVectorDemo.scala](src/main/scala/example/write/DoubleVectorDemo.scala)
Shows writing data with double vector fields to Milvus.

**Key Features:**
- Collection creation with double vector fields
- Data schema definition for double vectors
- Batch data insertion

> **Important Note:** When writing data, ensure that your Spark DataFrame schema matches the vector data type in your Milvus collection. Use `FloatType` for float vectors and `DoubleType` for double vectors in your schema definition.

### Python Examples

#### [pyspark_milvus_demo.py](python/pyspark_milvus_demo.py)
Comprehensive Python example using PySpark with Milvus connector.

**Key Features:**
- PySpark session configuration
- Milvus data reading with Python
- S3 integration setup
- Data analysis and statistics
- Error handling and validation

## Configuration

### Scala Examples
Most Scala examples use hardcoded configuration for simplicity:

```scala
val uri = "http://localhost:19530"
val token = "root:Milvus"
val collectionName = "your_collection_name"
```

### Python Examples
Python examples use a configuration file approach. Update `python/config.py` with your actual values:

```python
MILVUS_CONFIG = {
    "uri": "your_milvus_uri",
    "token": "your_milvus_token",
    "collection_name": "your_collection_name",
    # ... other configurations
}
```

## Running the Examples

### Scala Examples

1. Ensure your Spark environment is properly configured. If you're not sure how to set up Spark, please refer to the main repository: [milvus-spark-connector](https://github.com/SimFG/milvus-spark-connector)
2. Update the connection parameters in each demo file
3. Compile and package the project:

```bash
sbt clean compile package
```

4. Run the specific demo using spark-submit:

```bash
# For HelloDemo
spark-submit-wrapper --jars /xxx/spark-connector-assembly-x.x.x-SNAPSHOT.jar --class "example.HelloDemo" /xxx/milvus-spark-connector-example_2.13-0.1.0-SNAPSHOT.jar

# For specific read/write demos
spark-submit-wrapper --jars /xxx/spark-connector-assembly-x.x.x-SNAPSHOT.jar --class "example.read.MilvusDemo" /xxx/milvus-spark-connector-example_2.13-0.1.0-SNAPSHOT.jar
spark-submit-wrapper --jars /xxx/spark-connector-assembly-x.x.x-SNAPSHOT.jar --class "example.write.FloatVectorDemo" /xxx/milvus-spark-connector-example_2.13-0.1.0-SNAPSHOT.jar
```

### Python Examples

We recommend using [uv](https://docs.astral.sh/uv/getting-started/installation/) for Python environment management:

1. Install uv if you haven't already:
   - Visit [uv installation guide](https://docs.astral.sh/uv/getting-started/installation/) for detailed instructions

2. Navigate to the python directory:
```bash
cd python
```

3. Copy the example environment file and configure it:
```bash
cp .env.example .env
# Edit .env with your actual Milvus and S3 configuration
```

4. Run the demo using uv:
```bash
uv run pyspark_milvus_demo.py
```

## Additional Resources

- **Main Repository**: [milvus-spark-connector](https://github.com/SimFG/milvus-spark-connector)
- **Milvus Documentation**: [https://milvus.io/docs](https://milvus.io/docs)
- **Apache Spark Documentation**: [https://spark.apache.org/docs/latest/](https://spark.apache.org/docs/latest/)

## Support

For questions, issues, or contributions, please refer to the main [milvus-spark-connector](https://github.com/SimFG/milvus-spark-connector) repository. 