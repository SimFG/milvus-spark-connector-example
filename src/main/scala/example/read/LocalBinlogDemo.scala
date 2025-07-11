package example.read

import org.apache.spark.sql.SparkSession

import com.zilliz.spark.connector.MilvusOption

object LocalBinlogDemo extends App {
  val deleteFilePath = "data/read_binlog/delta_str_pk"
  val insertVarcharFilePath = "data/read_binlog/insert_varchar"
  val insertShortFilePath = "data/read_binlog/insert_short"
  val insertVecFilePath = "data/read_binlog/insert_float_vec"
  val minioPath =
    "insert_log/458338271272109051/458338271272109052/458338271273509174/0/458338271273509177"
  val ossPath = "insert_short"

  // More local binlog files can be viewed in the data/read_binlog directory of the project root directory
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("LocalBinlogDemo")
    .getOrCreate()

  readLocalInsertBinlog()
  readLocalDeleteBinlog()

  spark.stop()

  def readLocalInsertBinlog() {
    val df = spark.read
      .format("milvusbinlog")
      .option(MilvusOption.ReaderPath, insertVarcharFilePath)
      .option(MilvusOption.ReaderType, "insert")
      .load()
    df.show()
  }

  def readLocalDeleteBinlog() {
    val df = spark.read
      .format("milvusbinlog")
      .option(MilvusOption.ReaderPath, deleteFilePath)
      .option(MilvusOption.ReaderType, "delete")
      .load()
    df.show()
  }
}
