package example.other

import org.apache.spark.sql.{functions => F, SparkSession}

import com.zilliz.spark.connector.filter.VectorBruteForceSearch
import com.zilliz.spark.connector.MilvusOption

object UserTestDemo extends App {
  val uri = sys.env.getOrElse("SPARK_MILVUS_URI", "http://localhost:19530")
  val token = sys.env.getOrElse("SPARK_MILVUS_TOKEN", "root:Milvus")
  val collectionName = sys.env.getOrElse("SPARK_MILVUS_COLLECTION", "hello_dep")
  val collection =
    sys.env.getOrElse("SPARK_MILVUS_COLLECTION_ID", "458338271272109051")
  val partition =
    sys.env.getOrElse("SPARK_MILVUS_PARTITION_ID", "458155846610556543")
  val segment =
    sys.env.getOrElse("SPARK_MILVUS_SEGMENT_ID", "458155846610556627")

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("MilvusReadDemo")
    .getOrCreate()

  import spark.implicits._

  val df = spark.read
    .format("milvus")
    .option(MilvusOption.MilvusCollectionID, collection)
    .option(MilvusOption.MilvusPartitionID, partition)
    .option(MilvusOption.MilvusSegmentID, segment)
    .option(MilvusOption.ReaderType, "insert")
    .option(MilvusOption.S3FileSystemTypeName, "s3a://")
    .option(MilvusOption.S3Endpoint, "s3.amazonaws.com")
    .option(MilvusOption.S3AccessKey, "")
    .option(MilvusOption.S3SecretKey, "")
    .option(MilvusOption.S3BucketName, "zilliz-aws-yvprdefuyefnsky")
    .option(MilvusOption.S3RootPath, "421b7033af5b7e0")
    .option(MilvusOption.S3UseSSL, "true")
    .option(MilvusOption.MilvusUri, uri)
    .option(MilvusOption.MilvusToken, token)
    .option(MilvusOption.MilvusCollectionName, collectionName)
    .option(MilvusOption.ReaderFieldIDs, "100")
    .load()
  println("df2.count: " + df.count())
  df.show()

  val pkField = "Auto_id"

  val duplicateAutoIDs = df
    .groupBy(pkField)
    .agg(F.count("*").alias("count"))
    .filter(F.col("count") > 1)
    .select(pkField)

  val dfDuplicates = df.join(duplicateAutoIDs, Seq(pkField), "inner")

  println("Rows with duplicate Auto_id:")
  dfDuplicates.show()

  val numDuplicateRows = dfDuplicates.count()
  println(s"Number of rows with duplicate Auto_id: $numDuplicateRows")

  val uniqueAutoIDs = df
    .groupBy(pkField)
    .agg(F.count("*").alias("count"))
    .filter(F.col("count") === 1)
    .select(pkField)

  val dfUniques = df.join(uniqueAutoIDs, Seq("Auto_id"), "inner")

  val numUniqueRows = dfUniques.count()
  println(s"Number of rows with unique Auto_id: $numUniqueRows")

  val outputPath =
    "/milvus/duplicate_id"

  dfDuplicates
    .repartition(1)
    .write
    .option("header", "true")
    .mode("overwrite")
    .csv(outputPath)

  println(s"duplicated id saved to '$outputPath'")

  val outputPath2 =
    "/milvus/unique_id"

  dfUniques
    .repartition(1)
    .write
    .option("header", "true")
    .mode("overwrite")
    .csv(outputPath2)

  spark.stop()
}
