package example.read

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import com.zilliz.spark.connector.filter.VectorBruteForceSearch
import com.zilliz.spark.connector.MilvusOption

object MilvusDemo extends App {
  val uri = "http://localhost:19530"
  val token = "root:Milvus"
  val collectionName = "hello_array"
  val milvusSegmentPath =
    "insert_log/458155846610556542/458155846610556543/458155846610556627"
  val collection = "458338271272109051"
  val partition = "458155846610556543"
  val segment = "458155846610556627"

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("MilvusDemo")
    .getOrCreate()

  import spark.implicits._

  readCollection()
  readCollectionWithFields()
  readCollectionWithTimestampFilter()
  readSegment()
  readSegmentWithS3Path()

  spark.stop()

  // read collection data
  def readCollection(
  ) = {
    val df = spark.read
      .format("milvus")
      .option(MilvusOption.MilvusUri, uri)
      .option(MilvusOption.MilvusToken, token)
      .option(MilvusOption.MilvusCollectionName, collectionName)
      .option(MilvusOption.S3FileSystemTypeName, "s3a://")
      .load()
    df.show()
  }

  // read collection data with some fields
  def readCollectionWithFields(
  ) = {
    val df = spark.read
      .format("milvus")
      .option(MilvusOption.MilvusUri, uri)
      .option(MilvusOption.MilvusToken, token)
      .option(MilvusOption.MilvusCollectionName, collectionName)
      .option(MilvusOption.S3FileSystemTypeName, "s3a://")
      .option(MilvusOption.ReaderFieldIDs, "100,102")
      .load()

    df.show()
  }

  // read collection data with timestamp filter
  def readCollectionWithTimestampFilter(
  ) = {
    val df = spark.read
      .format("milvus")
      .option(MilvusOption.MilvusUri, uri)
      .option(MilvusOption.MilvusToken, token)
      .option(MilvusOption.MilvusCollectionName, collectionName)
      .option(MilvusOption.S3FileSystemTypeName, "s3a://")
      .load()
      .filter(col("timestamp") > 458635994081525763L)
    df.show()
  }

  // read segment data
  def readSegment() = {
    val df = spark.read
      .format("milvus")
      .option(MilvusOption.MilvusUri, uri)
      .option(MilvusOption.MilvusToken, token)
      .option(MilvusOption.MilvusCollectionName, collectionName)
      .option(MilvusOption.S3FileSystemTypeName, "s3a://")
      .option(MilvusOption.MilvusCollectionID, collection)
      .option(MilvusOption.MilvusPartitionID, partition)
      .option(MilvusOption.MilvusSegmentID, segment)
      .load()
    df.show()
  }

  // read segment data with s3 path
  def readSegmentWithS3Path() = {
    val df = spark.read
      .format("milvus")
      .option(MilvusOption.MilvusUri, uri)
      .option(MilvusOption.MilvusToken, token)
      .option(MilvusOption.MilvusCollectionName, collectionName)
      .option(MilvusOption.S3FileSystemTypeName, "s3a://")
      .option(MilvusOption.ReaderPath, milvusSegmentPath)
      .load()
    df.show()
  }
}
