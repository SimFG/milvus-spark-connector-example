package example

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

import com.zilliz.spark.connector.filter.VectorBruteForceSearch
import com.zilliz.spark.connector.MilvusDataReader
import com.zilliz.spark.connector.MilvusDataReaderConfig
import com.zilliz.spark.connector.MilvusOption
import org.apache.spark.sql.DataFrame

object HelloDemo extends App {
  val uri = "http://localhost:19530"
  val token = "root:Milvus"
  val collectionName = "hello_spark_milvus"

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("HelloDemo")
    .getOrCreate()

  import spark.implicits._

  val milvusDF = MilvusDataReader.read(
    spark,
    MilvusDataReaderConfig(
      uri,
      token,
      collectionName,
      options = Map(
        MilvusOption.S3FileSystemTypeName -> "s3a://"
      )
    )
  )
  milvusDF.show()
  println(s"milvusDF.count: ${milvusDF.count()}")

  spark.stop()
}
