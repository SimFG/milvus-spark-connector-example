package example

import org.apache.spark.sql.SparkSession
import com.zilliz.spark.connector.MilvusOption

object BinlogReadDemo extends App {
  val deleteFilePath = "data/read_binlog/delta_str_pk"
  val insertVarcharFilePath = "data/read_binlog/insert_varchar"
  val insertShortFilePath = "data/read_binlog/insert_short"
  val insertVecFilePath = "data/read_binlog/insert_float_vec"
  val minioPath = "insert_log/458338271272109051/458338271272109052/458338271273509174/0/458338271273509177"
  val ossPath = "insert_short"

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("BinlogReadDemo")
    .getOrCreate()

  val df = spark.read
    .format("milvusbinlog")
    .option(MilvusOption.ReaderPath, deleteFilePath)
    .option(MilvusOption.ReaderType, "delete")
    .load()
  df.show()

  val df2 = spark.read
    .format("milvusbinlog")
    .option(MilvusOption.ReaderPath, insertVarcharFilePath)
    .option(MilvusOption.ReaderType, "insert")
    .load()
  df2.show()

  val df3 = spark.read
    .format("milvusbinlog")
    .option(MilvusOption.ReaderPath, insertShortFilePath)
    .option(MilvusOption.ReaderType, "insert")
    .load()
  df3.show()

  val df4 = spark.read
    .format("milvusbinlog")
    .option(MilvusOption.ReaderPath, insertVecFilePath)
    .option(MilvusOption.ReaderType, "insert")
    .load()
  df4.show()

  // val df5 = spark.read
  //   .format("milvusbinlog")
  //   .option(MilvusOption.ReaderPath, ossPath)
  //   .option(MilvusOption.S3FileSystemTypeName, "s3a://")
  //   .option(MilvusOption.S3Endpoint, "oss-cn-beijing.aliyuncs.com")
  //   .option(MilvusOption.S3AccessKey, "xxx")
  //   .option(MilvusOption.S3SecretKey, "xxx")
  //   .option(MilvusOption.S3BucketName, "xxx")
  //   .option(MilvusOption.S3RootPath, "simfg-files")
  //   .option(MilvusOption.S3UseSSL, "true")
  //   .option(MilvusOption.S3PathStyleAccess, "false") // when use the oss, set to false
  //   .option(MilvusOption.ReaderType, "insert")
  //   .load()
  // df5.show()

  // val df6 = spark.read
  //   .format("milvusbinlog")
  //   .option(MilvusOption.ReaderPath, minioPath)
  //   .option(MilvusOption.S3FileSystemTypeName, "s3a://")
  //   .option(MilvusOption.ReaderType, "insert")
  //   .load()
  // df6.show()

  spark.stop()
}