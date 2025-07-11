package example.read

import org.apache.spark.sql.SparkSession

import com.zilliz.spark.connector.MilvusOption

object RemoteBinlogDemo extends App {
  val collection = "458155846610556542"
  val partition = "458155846610556543"
  val segment = "458155846610556627"
  val field = "101"

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("RemoteBinlogDemo")
    .getOrCreate()

  readRemoteInsertBinlog()
  readRemoteDeleteBinlog()

  spark.stop()

  def readRemoteInsertBinlog() {
    val df = spark.read
      .format("milvusbinlog")
      .option(MilvusOption.S3FileSystemTypeName, "s3a://")
      .option(MilvusOption.MilvusCollectionID, collection)
      .option(MilvusOption.MilvusPartitionID, partition)
      .option(MilvusOption.MilvusSegmentID, segment)
      .option(MilvusOption.MilvusFieldID, field)
      .option(MilvusOption.ReaderType, "insert")
      .load()
    df.show()
  }

  def readRemoteDeleteBinlog() {
    val df = spark.read
      .format("milvusbinlog")
      .option(MilvusOption.S3FileSystemTypeName, "s3a://")
      .option(MilvusOption.MilvusCollectionID, collection)
  }
}
