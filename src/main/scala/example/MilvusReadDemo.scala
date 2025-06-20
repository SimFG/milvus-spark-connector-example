package example

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import com.zilliz.spark.connector.filter.VectorBruteForceSearch
import com.zilliz.spark.connector.MilvusOption

object MilvusReadDemo extends App {
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
    .appName("MilvusReadDemo")
    .getOrCreate()

  import spark.implicits._

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
    .option(MilvusOption.MilvusUri, uri)
    .option(MilvusOption.MilvusToken, token)
    .option(MilvusOption.MilvusCollectionName, collectionName)
    .load()
    // .filter(col("timestamp") > 458635994081525763L)
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


  // generate a random vector with 16 elements
  val randomVector = Array.fill(16)(scala.util.Random.nextFloat())
  println(s"randomVector: ${randomVector.mkString("[", ", ", "]")}")
  val topKResults = VectorBruteForceSearch.filterSimilarVectors(
      df = df4,
      queryVector = randomVector,
      k = 10,
      vectorCol = "vector",
      idCol = Some("id")
    )

  println(s"\nTop 10 brute force search results:")
  topKResults.show()

  val df5 = spark.read
    .format("milvus")
    .option(MilvusOption.ReaderType, "insert")
    .option(MilvusOption.ReaderFieldIDs, "100,102")
    .option(MilvusOption.S3FileSystemTypeName, "s3a://")
    .option(MilvusOption.MilvusUri, uri)
    .option(MilvusOption.MilvusToken, token)
    .option(MilvusOption.MilvusCollectionName, collectionName)
    .load()

  df5.show()

  spark.stop()
}
