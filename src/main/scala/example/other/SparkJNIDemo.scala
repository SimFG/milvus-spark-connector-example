package example.other

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.zilliz.spark.connector.jni.MilvusStorageJNI
import scala.util.{Try, Success, Failure}

// spark-submit-wrapper --jars /home/zilliz/Repo/milvus-spark-connector/target/scala-2.13/spark-connector-assembly-0.1.6-SNAPSHOT.jar --class "example.SparkJNIDemo" /home/zilliz/Repo/milvus-spark-connector-example/target/scala-2.13/milvus-spark-connector-example_2.13-0.1.0-SNAPSHOT.jar
object SparkJNIDemo {
  
  def main(args: Array[String]): Unit = {
    
    // // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Spark JNI Demo")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    println("=== Spark JNI Demo Started ===")
    
    try {
      MilvusStorageJNI.testMilvusStorage()
      
      println("=== All tests completed successfully! ===")
      
    } catch {
      case e: Exception =>
        println(s"Error during execution: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}