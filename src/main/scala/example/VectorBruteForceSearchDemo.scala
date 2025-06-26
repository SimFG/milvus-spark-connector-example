package example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Random

// Import the VectorBruteForceSearch class (assuming it's available in the classpath)
// import com.zilliz.spark.connector.filter.VectorBruteForceSearch
// import com.zilliz.spark.connector.filter.VectorBruteForceSearch.{DistanceType, SearchType}

/**
 * Demo showcasing three types of vector brute force search:
 * 1. Dense Float Vector Search (using cosine similarity, L2 distance, inner product)
 * 2. Binary Vector Search (using Hamming distance, Jaccard distance) 
 * 3. Sparse Vector Search (using cosine similarity, L2 distance, inner product)
 * 
 * This demo is based on the filterSimilarVectors method from VectorBruteForceSearch.scala
 */
object VectorBruteForceSearchDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Vector Brute Force Search Demo")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .getOrCreate()

    import spark.implicits._

    println("=== Vector Brute Force Search Demo ===")
    println("Demonstrating three types of vector search:")
    println("1. Dense Float Vector Search")
    println("2. Binary Vector Search") 
    println("3. Sparse Vector Search")
    println()

    try {
      // Demo 1: Dense Float Vector Search
      println("🔍 Demo 1: Dense Float Vector Search")
      println("=" * 50)
      denseFloatVectorSearchDemo(spark)
      println()

      // Demo 2: Binary Vector Search
      println("🔍 Demo 2: Binary Vector Search")
      println("=" * 50)
      binaryVectorSearchDemo(spark)
      println()

      // Demo 3: Sparse Vector Search
      println("🔍 Demo 3: Sparse Vector Search")
      println("=" * 50)
      sparseVectorSearchDemo(spark)
      println()

    } finally {
      spark.stop()
    }
  }

  /**
   * Demo 1: Dense Float Vector Search
   * Demonstrates cosine similarity, L2 distance, and inner product searches
   */
  def denseFloatVectorSearchDemo(spark: SparkSession): Unit = {
    import spark.implicits._

    println("Creating sample dense float vectors...")
    
    // Create sample data with dense float vectors (128-dimensional)
    val vectorDim = 128
    val numVectors = 1000
    val random = new Random(42)

    val denseVectorData = (1 to numVectors).map { id =>
      val vector = Array.fill(vectorDim)(random.nextFloat() * 2.0f - 1.0f) // Random values between -1 and 1
      (id, vector.toSeq, s"item_$id", random.nextInt(5) + 1) // id, vector, name, category
    }.toDF("id", "vector", "name", "category")

    println(s"Created ${numVectors} dense float vectors with ${vectorDim} dimensions")
    println("Sample data:")
    denseVectorData.select("id", "name", "category").show(5)

    // Query vector - similar to first vector but with some noise
    val baseVector = denseVectorData.select("vector").first().getAs[scala.collection.Seq[Float]](0)
    val queryVector = baseVector.toSeq.map(v => v + random.nextFloat() * 0.1f - 0.05f) // Add small noise

    println("Query vector (first 10 dimensions):")
    println(queryVector.take(10).mkString("[", ", ", ", ...]"))
    println()

    // Example 1: Cosine Similarity Search (KNN)
    println("📊 Example 1a: Cosine Similarity Search (Top 5 similar)")
    val cosineResults = performDenseVectorSearch(
      denseVectorData, queryVector, k = 5, 
      distanceType = "COSINE", searchType = "KNN"
    )
    cosineResults.select("id", "name", "similarity").show()

    // Example 2: L2 Distance Search (KNN)
    println("📊 Example 1b: L2 Distance Search (Top 5 closest)")
    val l2Results = performDenseVectorSearch(
      denseVectorData, queryVector, k = 5,
      distanceType = "L2", searchType = "KNN"
    )
    l2Results.select("id", "name", "distance").show()

    // Example 3: Inner Product Search (KNN)
    println("📊 Example 1c: Inner Product Search (Top 5 highest)")
    val ipResults = performDenseVectorSearch(
      denseVectorData, queryVector, k = 5,
      distanceType = "IP", searchType = "KNN"
    )
    ipResults.select("id", "name", "inner_product").show()

    // Example 4: Range Search with Cosine Similarity
    println("📊 Example 1d: Range Search (Cosine similarity > 0.8)")
    val rangeResults = performDenseVectorSearch(
      denseVectorData, queryVector, k = 100,
      distanceType = "COSINE", searchType = "RANGE", radius = Some(0.8)
    )
    println(s"Found ${rangeResults.count()} vectors with cosine similarity > 0.8")
    rangeResults.select("id", "name", "similarity").show()
  }

  /**
   * Demo 2: Binary Vector Search
   * Demonstrates Hamming distance and Jaccard distance searches
   */
  def binaryVectorSearchDemo(spark: SparkSession): Unit = {
    import spark.implicits._

    println("Creating sample binary vectors...")
    
    // Create sample data with binary vectors (64 bytes = 512 bits)
    val vectorBytes = 64
    val numVectors = 500
    val random = new Random(42)

    val binaryVectorData = (1 to numVectors).map { id =>
      val vector = Array.fill(vectorBytes)(random.nextInt(256).toByte)
      (id, vector.toSeq, s"binary_item_$id", random.nextBoolean())
    }.toDF("id", "vector", "name", "is_active")

    println(s"Created ${numVectors} binary vectors with ${vectorBytes} bytes (${vectorBytes * 8} bits)")
    println("Sample data:")
    binaryVectorData.select("id", "name", "is_active").show(5)

    // Query vector - random binary vector
    val queryVector = Array.fill(vectorBytes)(random.nextInt(256).toByte)
    
    println("Query vector (first 8 bytes in hex):")
    println(queryVector.take(8).map(b => f"${b & 0xFF}%02X").mkString("[", " ", " ...]"))
    println()

    // Example 1: Hamming Distance Search (KNN)
    println("📊 Example 2a: Hamming Distance Search (Top 5 closest)")
    val hammingResults = performBinaryVectorSearch(
      binaryVectorData, queryVector, k = 5,
      distanceType = "HAMMING", searchType = "KNN"
    )
    hammingResults.select("id", "name", "hamming_distance").show()

    // Example 2: Jaccard Distance Search (KNN)
    println("📊 Example 2b: Jaccard Distance Search (Top 5 closest)")
    val jaccardResults = performBinaryVectorSearch(
      binaryVectorData, queryVector, k = 5,
      distanceType = "JACCARD", searchType = "KNN"
    )
    jaccardResults.select("id", "name", "jaccard_distance").show()

    // Example 3: Range Search with Hamming Distance
    println("📊 Example 2c: Range Search (Hamming distance <= 50)")
    val rangeResults = performBinaryVectorSearch(
      binaryVectorData, queryVector, k = 100,
      distanceType = "HAMMING", searchType = "RANGE", radius = Some(50.0)
    )
    println(s"Found ${rangeResults.count()} vectors with Hamming distance <= 50")
    rangeResults.select("id", "name", "hamming_distance").show()
  }

  /**
   * Demo 3: Sparse Vector Search
   * Demonstrates cosine similarity, L2 distance, and inner product searches on sparse vectors
   */
  def sparseVectorSearchDemo(spark: SparkSession): Unit = {
    import spark.implicits._

    println("Creating sample sparse vectors...")
    
    // Create sample data with sparse vectors (10000-dimensional, but only ~50 non-zero values)
    val vectorSize = 10000
    val numVectors = 300
    val sparsity = 50 // Number of non-zero elements
    val random = new Random(42)

    val sparseVectorData = (1 to numVectors).map { id =>
      // Generate sparse vector as Map[Long, Float]
      val indices = random.shuffle((0L until vectorSize).toList).take(sparsity)
      val sparseMap = indices.map { idx =>
        idx -> (random.nextFloat() * 2.0f - 1.0f) // Random values between -1 and 1
      }.toMap
      
      (id, sparseMap, s"sparse_item_$id", random.nextDouble())
    }.toDF("id", "vector", "name", "score")

    println(s"Created ${numVectors} sparse vectors with ${vectorSize} dimensions (~${sparsity} non-zero elements each)")
    println("Sample data:")
    sparseVectorData.select("id", "name", "score").show(5)

    // Query vector - sparse vector with some random non-zero elements
    val queryIndices = random.shuffle((0L until vectorSize).toList).take(sparsity)
    val queryVector = queryIndices.map { idx =>
      idx -> (random.nextFloat() * 2.0f - 1.0f)
    }.toMap

    println(s"Query vector has ${queryVector.size} non-zero elements")
    println("Sample non-zero elements:")
    queryVector.take(5).foreach { case (idx, value) => 
      println(f"  Index $idx: $value%.4f")
    }
    println()

    // Example 1: Cosine Similarity Search (KNN)
    println("📊 Example 3a: Sparse Cosine Similarity Search (Top 5 similar)")
    val cosineResults = performSparseVectorSearch(
      sparseVectorData, queryVector, k = 5,
      distanceType = "COSINE", searchType = "KNN", vectorSize = vectorSize
    )
    cosineResults.select("id", "name", "similarity").show()

    // Example 2: L2 Distance Search (KNN)
    println("📊 Example 3b: Sparse L2 Distance Search (Top 5 closest)")
    val l2Results = performSparseVectorSearch(
      sparseVectorData, queryVector, k = 5,
      distanceType = "L2", searchType = "KNN", vectorSize = vectorSize
    )
    l2Results.select("id", "name", "distance").show()

    // Example 3: Inner Product Search (KNN)
    println("📊 Example 3c: Sparse Inner Product Search (Top 5 highest)")
    val ipResults = performSparseVectorSearch(
      sparseVectorData, queryVector, k = 5,
      distanceType = "IP", searchType = "KNN", vectorSize = vectorSize
    )
    ipResults.select("id", "name", "inner_product").show()

    // Example 4: Range Search with Cosine Similarity
    println("📊 Example 3d: Sparse Range Search (Cosine similarity > 0.1)")
    val rangeResults = performSparseVectorSearch(
      sparseVectorData, queryVector, k = 100,
      distanceType = "COSINE", searchType = "RANGE", radius = Some(0.1), vectorSize = vectorSize
    )
    println(s"Found ${rangeResults.count()} vectors with cosine similarity > 0.1")
    rangeResults.select("id", "name", "similarity").show()
  }

  // Helper methods for demonstration (these would call the actual VectorBruteForceSearch methods)

  /**
   * Simulated dense vector search - in real implementation, this would call:
   * VectorBruteForceSearch.filterSimilarVectors(df, queryVector, k, threshold, "vector", Some("id"), distanceType, searchType, radius)
   */
  def performDenseVectorSearch(
      df: DataFrame, 
      queryVector: Seq[Float], 
      k: Int = 10,
      distanceType: String = "COSINE",
      searchType: String = "KNN",
      radius: Option[Double] = None
  ): DataFrame = {
    println(s"🔧 Performing ${distanceType} ${searchType} search on dense float vectors...")
    println(s"   Query vector dimensions: ${queryVector.length}")
    println(s"   K: $k, Radius: ${radius.getOrElse("N/A")}")
    
    // In a real implementation, you would call:
    // VectorBruteForceSearch.filterSimilarVectors(
    //   df = df,
    //   queryVector = queryVector,
    //   k = k,
    //   vectorCol = "vector",
    //   idCol = Some("id"),
    //   distanceType = DistanceType.valueOf(distanceType),
    //   searchType = SearchType.valueOf(searchType),
    //   radius = radius
    // )
    
    // For demo purposes, return a mock result
    createMockDenseResult(df, distanceType, k)
  }

  /**
   * Simulated binary vector search
   */
  def performBinaryVectorSearch(
      df: DataFrame,
      queryVector: Array[Byte],
      k: Int = 10,
      distanceType: String = "HAMMING",
      searchType: String = "KNN",
      radius: Option[Double] = None
  ): DataFrame = {
    println(s"🔧 Performing ${distanceType} ${searchType} search on binary vectors...")
    println(s"   Query vector size: ${queryVector.length} bytes")
    println(s"   K: $k, Radius: ${radius.getOrElse("N/A")}")
    
    // In a real implementation, you would call:
    // VectorBruteForceSearch.filterSimilarVectors(
    //   df = df,
    //   queryVector = queryVector,
    //   k = k,
    //   vectorCol = "vector",
    //   idCol = Some("id"),
    //   distanceType = DistanceType.valueOf(distanceType),
    //   searchType = SearchType.valueOf(searchType),
    //   radius = radius
    // )
    
    // For demo purposes, return a mock result
    createMockBinaryResult(df, distanceType, k)
  }

  /**
   * Simulated sparse vector search
   */
  def performSparseVectorSearch(
      df: DataFrame,
      queryVector: Map[Long, Float],
      k: Int = 10,
      distanceType: String = "COSINE",
      searchType: String = "KNN",
      radius: Option[Double] = None,
      vectorSize: Int = 10000
  ): DataFrame = {
    println(s"🔧 Performing ${distanceType} ${searchType} search on sparse vectors...")
    println(s"   Query vector non-zero elements: ${queryVector.size}")
    println(s"   Vector dimension: $vectorSize")
    println(s"   K: $k, Radius: ${radius.getOrElse("N/A")}")
    
    // In a real implementation, you would call:
    // VectorBruteForceSearch.filterSimilarVectors(
    //   df = df,
    //   queryVector = queryVector,
    //   k = k,
    //   vectorCol = "vector",
    //   idCol = Some("id"),
    //   distanceType = DistanceType.valueOf(distanceType),
    //   searchType = SearchType.valueOf(searchType),
    //   radius = radius,
    //   vectorSize = vectorSize
    // )
    
    // For demo purposes, return a mock result
    createMockSparseResult(df, distanceType, k)
  }

  // Mock result generators for demonstration
  private def createMockDenseResult(df: DataFrame, distanceType: String, k: Int): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._
    
    val random = new Random(42)
    val sampleData = df.limit(k).collect()
    
    val resultsWithDistance = sampleData.map { row =>
      val distance = distanceType match {
        case "COSINE" => 0.5 + random.nextDouble() * 0.5 // 0.5 to 1.0
        case "L2" => random.nextDouble() * 2.0 // 0.0 to 2.0
        case "IP" => random.nextDouble() * 10.0 // 0.0 to 10.0
      }
      (row.getAs[Int]("id"), row.getAs[String]("name"), row.getAs[Int]("category"), distance)
    }
    
    val distanceCol = distanceType match {
      case "COSINE" => "similarity"
      case "L2" => "distance"
      case "IP" => "inner_product"
    }
    
    spark.createDataFrame(resultsWithDistance.toSeq).toDF("id", "name", "category", distanceCol)
  }

  private def createMockBinaryResult(df: DataFrame, distanceType: String, k: Int): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._
    
    val random = new Random(42)
    val sampleData = df.limit(k).collect()
    
    val resultsWithDistance = sampleData.map { row =>
      val distance = distanceType match {
        case "HAMMING" => random.nextInt(100).toDouble // 0 to 100
        case "JACCARD" => random.nextDouble() // 0.0 to 1.0
      }
      (row.getAs[Int]("id"), row.getAs[String]("name"), row.getAs[Boolean]("is_active"), distance)
    }
    
    val distanceCol = distanceType match {
      case "HAMMING" => "hamming_distance"
      case "JACCARD" => "jaccard_distance"
    }
    
    spark.createDataFrame(resultsWithDistance.toSeq).toDF("id", "name", "is_active", distanceCol)
  }

  private def createMockSparseResult(df: DataFrame, distanceType: String, k: Int): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._
    
    val random = new Random(42)
    val sampleData = df.limit(k).collect()
    
    val resultsWithDistance = sampleData.map { row =>
      val distance = distanceType match {
        case "COSINE" => random.nextDouble() * 0.8 // 0.0 to 0.8
        case "L2" => random.nextDouble() * 5.0 // 0.0 to 5.0
        case "IP" => random.nextDouble() * 20.0 // 0.0 to 20.0
      }
      (row.getAs[Int]("id"), row.getAs[String]("name"), row.getAs[Double]("score"), distance)
    }
    
    val distanceCol = distanceType match {
      case "COSINE" => "similarity"
      case "L2" => "distance"
      case "IP" => "inner_product"
    }
    
    spark.createDataFrame(resultsWithDistance.toSeq).toDF("id", "name", "score", distanceCol)
  }
}