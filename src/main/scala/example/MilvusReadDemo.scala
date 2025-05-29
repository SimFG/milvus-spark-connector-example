package example

import org.apache.spark.sql.SparkSession
import com.zilliz.spark.connector.MilvusOption

object MilvusReadDemo extends App {
  val uri = "http://localhost:19530"
  val token = "root:Milvus"
  val collectionName = "hello_mix"
  val milvusSegmentPath = "insert_log/458155846610556542/458155846610556543/458155846610556627"
  val collection = "458338271272109051"
  val partition = "458155846610556543"
  val segment = "458155846610556627"

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("MilvusReadDemo")
    .getOrCreate()

  val df = spark.read
    .format("milvus")
    .option(MilvusOption.ReaderPath, milvusSegmentPath)
    .option(MilvusOption.ReaderType, "insert")
    .option(MilvusOption.S3FileSystemTypeName, "s3a://")
    .option(MilvusOption.MilvusUri, uri)
    .option(MilvusOption.MilvusToken, token)
    .option(MilvusOption.MilvusCollectionName, collectionName)
    .load()
  df.show()

  val df2 = spark.read
    .format("milvus")
    .option(MilvusOption.MilvusCollectionID, collection)
    .option(MilvusOption.MilvusPartitionID, partition)
    .option(MilvusOption.MilvusSegmentID, segment)
    .option(MilvusOption.ReaderType, "insert")
    .option(MilvusOption.S3FileSystemTypeName, "s3a://")
    .option(MilvusOption.MilvusUri, uri)
    .option(MilvusOption.MilvusToken, token)
    .option(MilvusOption.MilvusCollectionName, collectionName)
    .load()
  df2.show()

  val df3 = spark.read
    .format("milvus")
    .option(MilvusOption.MilvusCollectionID, collection)
    .option(MilvusOption.ReaderType, "insert")
    .option(MilvusOption.S3FileSystemTypeName, "s3a://")
    // .option(MilvusOption.ReaderBeginTimestamp, "458338327854317571")
    // .option(MilvusOption.ReaderEndTimestamp, "458338327854317571")
    .option(MilvusOption.MilvusUri, uri)
    .option(MilvusOption.MilvusToken, token)
    .option(MilvusOption.MilvusCollectionName, collectionName)
    .load()
  df3.show()

  val df4 = spark.read
    .format("milvus")
    .option(MilvusOption.ReaderType, "insert")
    .option(MilvusOption.S3FileSystemTypeName, "s3a://")
    .option(MilvusOption.MilvusUri, uri)
    .option(MilvusOption.MilvusToken, token)
    .option(MilvusOption.MilvusCollectionName, collectionName)
    .load()
  df4.show()

  spark.stop()
}