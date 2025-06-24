package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.zilliz.spark.connector.jni.NativeLib
import scala.util.{Try, Success, Failure}

// spark-submit-wrapper --jars /home/zilliz/Repo/milvus-spark-connector/target/scala-2.13/spark-connector-assembly-0.1.5-SNAPSHOT.jar --class "example.SparkJNIDemo" /home/zilliz/Repo/milvus-spark-connector-example/target/scala-2.13/milvus-spark-connector-example_2.13-0.1.0-SNAPSHOT.jar
object SparkJNIDemo {
  
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Spark JNI Demo")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    println("=== Spark JNI Demo Started ===")
    
    try {
      // 1. Test basic JNI library functions
      testBasicJNIFunctions()
      
      // 2. Use JNI to process data in Spark
      testJNIWithSpark(spark)
      
      // 3. Create custom UDF using JNI
      testJNIUDF(spark)
      
      println("=== All tests completed successfully! ===")
      
    } catch {
      case e: Exception =>
        println(s"Error during execution: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
  
  def testBasicJNIFunctions(): Unit = {
    println("\n--- Testing Basic JNI Functions ---")
    
    Try {
      NativeLib.testNativeLibrary()
    } match {
      case Success(_) => println("✓ Basic JNI functions test passed")
      case Failure(e) => 
        println(s"✗ Basic JNI functions test failed: ${e.getMessage}")
        throw e
    }
  }
  
  def testJNIWithSpark(spark: SparkSession): Unit = {
    println("\n--- Testing JNI with Spark DataFrames ---")
    
    import spark.implicits._
    
    // Create test data
    val testData = Seq(
      ("Alice", Array(1, 2, 3, 4, 5), Array(1.1, 2.2, 3.3)),
      ("Bob", Array(10, 20, 30, 40, 50), Array(10.1, 20.2, 30.3)),
      ("Charlie", Array(100, 200, 300), Array(100.1, 200.2, 300.3))
    ).toDF("name", "int_array", "double_array")
    
    println("Original data:")
    testData.show()
    
    // Process data using JNI
    val processedData = testData.map { row =>
      val name = row.getAs[String]("name")
      val intArray = row.getAs[Seq[Int]]("int_array").toArray
      val doubleArray = row.getAs[Seq[Double]]("double_array").toArray
      
      // Process arrays using JNI
      val processedIntArray = NativeLib.processArray(intArray)
      val sum = NativeLib.calculateSum(doubleArray)
      val processedName = NativeLib.processString(name)
      
      (processedName, processedIntArray.toSeq, sum)
    }.toDF("processed_name", "processed_int_array", "double_sum")
    
    println("Processed data using JNI:")
    processedData.show(truncate = false)
  }
  
  def testJNIUDF(spark: SparkSession): Unit = {
    println("\n--- Testing JNI UDF ---")
    
    import spark.implicits._
    
    // Register UDF
    val processStringUDF = udf((input: String) => {
      Try(NativeLib.processString(input)).getOrElse(s"ERROR: $input")
    })
    
    val processArrayUDF = udf((input: Seq[Int]) => {
      Try(NativeLib.processArray(input.toArray).toSeq).getOrElse(Seq.empty[Int])
    })
    
    val calculateSumUDF = udf((input: Seq[Double]) => {
      Try(NativeLib.calculateSum(input.toArray)).getOrElse(0.0)
    })
    
    // Create larger test dataset
    val largeTestData = spark.range(1, 1001)
      .select(
        col("id"),
        concat(lit("user_"), col("id")).as("username"),
        array((1 to 5).map(i => (col("id") * i).cast(IntegerType)): _*).as("numbers"),
        array((1 to 3).map(i => (col("id") * i * 0.1).cast(DoubleType)): _*).as("decimals")
      )
    
    println("Sample of large test data:")
    largeTestData.show(5)
    
    // Apply JNI UDF
    val processedLargeData = largeTestData
      .withColumn("processed_username", processStringUDF(col("username")))
      .withColumn("processed_numbers", processArrayUDF(col("numbers")))
      .withColumn("decimal_sum", calculateSumUDF(col("decimals")))
    
    println("Sample of processed large data using JNI UDF:")
    processedLargeData.select("id", "processed_username", "processed_numbers", "decimal_sum")
      .show(5, truncate = false)
    
    // Statistics
    val count = processedLargeData.count()
    val avgSum = processedLargeData.agg(avg("decimal_sum")).collect()(0).getDouble(0)
    
    println(f"Processed $count records using JNI")
    println(f"Average decimal sum: $avgSum%.2f")
    
    // Performance test
    println("\n--- Performance Test ---")
    val startTime = System.currentTimeMillis()
    
    processedLargeData
      .filter(col("decimal_sum") > 100)
      .groupBy((col("id") / 100).cast(IntegerType).as("group"))
      .agg(
        org.apache.spark.sql.functions.count(lit(1)).as("record_count"),
        avg("decimal_sum").as("avg_sum"),
        max("decimal_sum").as("max_sum")
      )
      .orderBy("group")
      .show()
    
    val endTime = System.currentTimeMillis()
    println(f"Performance test completed in ${endTime - startTime}ms")
  }
} 