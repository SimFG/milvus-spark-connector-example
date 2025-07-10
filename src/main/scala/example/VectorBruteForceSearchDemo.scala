package example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Random

// Import the VectorBruteForceSearch class and its enums
import com.zilliz.spark.connector.filter.VectorBruteForceSearch
import com.zilliz.spark.connector.filter.VectorBruteForceSearch.{DistanceType, SearchType}

/**
 * To run this demo with spark-submit, first compile both projects, then use:
 * 
 * spark-submit-wrapper --jars /home/zilliz/Repo/milvus-spark-connector/target/scala-2.13/spark-connector-assembly-0.1.6-SNAPSHOT.jar \
 *   --class "example.VectorBruteForceSearchDemo" \
 *   /home/zilliz/Repo/milvus-spark-connector-example/target/scala-2.13/milvus-spark-connector-example_2.13-0.1.0-SNAPSHOT.jar
 * 
 * Important Notes about Range Search:
 * - For COSINE similarity: radius parameter works as (similarity >= 1.0 - radius)
 *   e.g., radius=0.5 means similarity >= 0.5
 * - For L2 distance: radius parameter works as (distance <= radius)
 * - For IP (inner product): radius parameter works as (inner_product >= radius)
 * - For HAMMING/JACCARD distance: radius parameter works as (distance <= radius)
 * 
 * Important Notes about KNN Search Thresholds:
 * - For distance-based metrics (L2, HAMMING, JACCARD): set threshold high enough to allow results
 * - For similarity-based metrics (COSINE, IP): default threshold=0.0 usually works
 * - The demo creates some similar vectors to ensure meaningful results
 */
/**
 * Demo showcasing three types of vector brute force search:
 * 1. Dense Float Vector Search (using cosine similarity, L2 distance, inner product)
 * 2. Binary Vector Search (using Hamming distance, Jaccard distance) 
 * 3. Sparse Vector Search (using cosine similarity, L2 distance, inner product)
 * 
 * This demo uses the real VectorBruteForceSearch.filterSimilarVectors method
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

    // Create a base vector for generating similar vectors
    val baseVector = Array.fill(vectorDim)(random.nextFloat() * 2.0f - 1.0f)
    
    val denseVectorData = (1 to numVectors).map { id =>
      val vector = if (id <= 100) {
        // Create first 100 vectors similar to base vector
        baseVector.map(v => v + (random.nextFloat() * 0.4f - 0.2f)) // Add moderate noise
      } else {
        // Create remaining vectors completely random
        Array.fill(vectorDim)(random.nextFloat() * 2.0f - 1.0f)
      }
      (id, vector.toSeq, s"item_$id", random.nextInt(5) + 1) // id, vector, name, category
    }.toDF("id", "vector", "name", "category")

    println(s"Created ${numVectors} dense float vectors with ${vectorDim} dimensions")
    println("(First 100 vectors are similar to the query vector for better demo results)")
    println("Sample data:")
    denseVectorData.select("id", "name", "category").show(5)

    // Query vector - use the base vector so we have similar vectors in the dataset
    val queryVector = baseVector.toSeq

    println("Query vector (first 10 dimensions):")
    println(queryVector.take(10).mkString("[", ", ", ", ...]"))
    println()

    // Example 1: Cosine Similarity Search (KNN)
    println("📊 Example 1a: Cosine Similarity Search (Top 5 similar)")
    val cosineResults = VectorBruteForceSearch.filterSimilarVectors(
      df = denseVectorData,
      queryVector = queryVector,
      k = 5,
      vectorCol = "vector",
      idCol = Some("id"),
      distanceType = DistanceType.COSINE,
      searchType = SearchType.KNN
    )
    cosineResults.select("id", "name", "similarity").show()

    // Example 2: L2 Distance Search (KNN)
    println("📊 Example 1b: L2 Distance Search (Top 5 closest)")
    val l2Results = VectorBruteForceSearch.filterSimilarVectors(
      df = denseVectorData,
      queryVector = queryVector,
      k = 5,
      threshold = 100.0, // Set a reasonable threshold for L2 distance (allow distances up to 100)
      vectorCol = "vector",
      idCol = Some("id"),
      distanceType = DistanceType.L2,
      searchType = SearchType.KNN
    )
    l2Results.select("id", "name", "distance").show()

    // Example 3: Inner Product Search (KNN)
    println("📊 Example 1c: Inner Product Search (Top 5 highest)")
    val ipResults = VectorBruteForceSearch.filterSimilarVectors(
      df = denseVectorData,
      queryVector = queryVector,
      k = 5,
      vectorCol = "vector",
      idCol = Some("id"),
      distanceType = DistanceType.IP,
      searchType = SearchType.KNN
    )
    ipResults.select("id", "name", "inner_product").show()

    // Example 4: Range Search with Cosine Similarity
    println("📊 Example 1d: Range Search (Cosine similarity >= 0.5)")
    val rangeResults = VectorBruteForceSearch.filterSimilarVectors(
      df = denseVectorData,
      queryVector = queryVector,
      k = 100,
      vectorCol = "vector",
      idCol = Some("id"),
      distanceType = DistanceType.COSINE,
      searchType = SearchType.RANGE,
      radius = Some(0.5) // This means similarity >= (1.0 - 0.5) = 0.5
    )
    println(s"Found ${rangeResults.count()} vectors with cosine similarity >= 0.5")
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

    // Create a base vector for generating similar vectors
    val baseVector = Array.fill(vectorBytes)(random.nextInt(256).toByte)
    
    val binaryVectorData = (1 to numVectors).map { id =>
      val vector = if (id <= 50) {
        // Create first 50 vectors similar to base vector (modify only a few bytes)
        val similarVector = baseVector.clone()
        val numChanges = random.nextInt(5) + 1 // Change 1-5 bytes
        (0 until numChanges).foreach { _ =>
          val pos = random.nextInt(vectorBytes)
          similarVector(pos) = random.nextInt(256).toByte
        }
        similarVector
      } else {
        // Create remaining vectors completely random
        Array.fill(vectorBytes)(random.nextInt(256).toByte)
      }
      (id, vector.toSeq, s"binary_item_$id", random.nextBoolean())
    }.toDF("id", "vector", "name", "is_active")

    println(s"Created ${numVectors} binary vectors with ${vectorBytes} bytes (${vectorBytes * 8} bits)")
    println("(First 50 vectors are similar to the query vector for better demo results)")
    println("Sample data:")
    binaryVectorData.select("id", "name", "is_active").show(5)

    // Query vector - use the base vector so we have similar vectors in the dataset
    val queryVector = baseVector
    
    println("Query vector (first 8 bytes in hex):")
    println(queryVector.take(8).map(b => f"${b & 0xFF}%02X").mkString("[", " ", " ...]"))
    println()

    // Example 1: Hamming Distance Search (KNN)
    println("📊 Example 2a: Hamming Distance Search (Top 5 closest)")
    val hammingResults = VectorBruteForceSearch.filterSimilarVectors(
      df = binaryVectorData,
      queryVector = queryVector,
      k = 5,
      threshold = 512.0, // Set threshold to max possible Hamming distance (64 bytes * 8 bits = 512)
      vectorCol = "vector",
      idCol = Some("id"),
      distanceType = DistanceType.HAMMING,
      searchType = SearchType.KNN
    )
    hammingResults.select("id", "name", "hamming_distance").show()

    // Example 2: Jaccard Distance Search (KNN)
    println("📊 Example 2b: Jaccard Distance Search (Top 5 closest)")
    val jaccardResults = VectorBruteForceSearch.filterSimilarVectors(
      df = binaryVectorData,
      queryVector = queryVector,
      k = 5,
      threshold = 1.0, // Set threshold to max possible Jaccard distance (1.0)
      vectorCol = "vector",
      idCol = Some("id"),
      distanceType = DistanceType.JACCARD,
      searchType = SearchType.KNN
    )
    jaccardResults.select("id", "name", "jaccard_distance").show()

    // Example 3: Range Search with Hamming Distance
    println("📊 Example 2c: Range Search (Hamming distance <= 100)")
    val rangeResults = VectorBruteForceSearch.filterSimilarVectors(
      df = binaryVectorData,
      queryVector = queryVector,
      k = 100,
      vectorCol = "vector",
      idCol = Some("id"),
      distanceType = DistanceType.HAMMING,
      searchType = SearchType.RANGE,
      radius = Some(100.0) // Increased radius to find more similar vectors
    )
    println(s"Found ${rangeResults.count()} vectors with Hamming distance <= 100")
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

    // Create base indices for generating similar sparse vectors
    val baseIndices = random.shuffle((0L until vectorSize).toList).take(sparsity)
    val baseValues = baseIndices.map(_ -> (random.nextFloat() * 2.0f - 1.0f)).toMap
    
    val sparseVectorData = (1 to numVectors).map { id =>
      val sparseMap = if (id <= 50) {
        // Create first 50 vectors with some overlapping indices
        val commonIndices = baseIndices.take(sparsity / 2) // Use half of base indices
        val newIndices = random.shuffle((0L until vectorSize).toList.filterNot(baseIndices.contains)).take(sparsity / 2)
        val allIndices = commonIndices ++ newIndices
        allIndices.map { idx =>
          idx -> (random.nextFloat() * 2.0f - 1.0f)
        }.toMap
      } else {
        // Create remaining vectors completely random
        val indices = random.shuffle((0L until vectorSize).toList).take(sparsity)
        indices.map { idx =>
          idx -> (random.nextFloat() * 2.0f - 1.0f)
        }.toMap
      }
      
      (id, sparseMap, s"sparse_item_$id", random.nextDouble())
    }.toDF("id", "vector", "name", "score")

    println(s"Created ${numVectors} sparse vectors with ${vectorSize} dimensions (~${sparsity} non-zero elements each)")
    println("(First 50 vectors have some overlapping indices with the query vector for better demo results)")
    println("Sample data:")
    sparseVectorData.select("id", "name", "score").show(5)

    // Query vector - use the base vector so we have similar vectors in the dataset
    val queryVector = baseValues

    println(s"Query vector has ${queryVector.size} non-zero elements")
    println("Sample non-zero elements:")
    queryVector.take(5).foreach { case (idx, value) => 
      println(f"  Index $idx: $value%.4f")
    }
    println()

    // Example 1: Cosine Similarity Search (KNN)
    println("📊 Example 3a: Sparse Cosine Similarity Search (Top 5 similar)")
    val cosineResults = VectorBruteForceSearch.filterSimilarVectors(
      df = sparseVectorData,
      queryVector = queryVector,
      k = 5,
      vectorCol = "vector",
      idCol = Some("id"),
      distanceType = DistanceType.COSINE,
      searchType = SearchType.KNN,
      vectorSize = vectorSize
    )
    cosineResults.select("id", "name", "similarity").show()

    // Example 2: L2 Distance Search (KNN)
    println("📊 Example 3b: Sparse L2 Distance Search (Top 5 closest)")
    val l2Results = VectorBruteForceSearch.filterSimilarVectors(
      df = sparseVectorData,
      queryVector = queryVector,
      k = 5,
      threshold = 1000.0, // Set a reasonable threshold for sparse L2 distance
      vectorCol = "vector",
      idCol = Some("id"),
      distanceType = DistanceType.L2,
      searchType = SearchType.KNN,
      vectorSize = vectorSize
    )
    l2Results.select("id", "name", "distance").show()

    // Example 3: Inner Product Search (KNN)
    println("📊 Example 3c: Sparse Inner Product Search (Top 5 highest)")
    val ipResults = VectorBruteForceSearch.filterSimilarVectors(
      df = sparseVectorData,
      queryVector = queryVector,
      k = 5,
      vectorCol = "vector",
      idCol = Some("id"),
      distanceType = DistanceType.IP,
      searchType = SearchType.KNN,
      vectorSize = vectorSize
    )
    ipResults.select("id", "name", "inner_product").show()

    // Example 4: Range Search with Cosine Similarity
    println("📊 Example 3d: Sparse Range Search (Cosine similarity >= 0.01)")
    val rangeResults = VectorBruteForceSearch.filterSimilarVectors(
      df = sparseVectorData,
      queryVector = queryVector,
      k = 100,
      vectorCol = "vector",
      idCol = Some("id"),
      distanceType = DistanceType.COSINE,
      searchType = SearchType.RANGE,
      radius = Some(0.99), // This means similarity >= (1.0 - 0.99) = 0.01
      vectorSize = vectorSize
    )
    println(s"Found ${rangeResults.count()} vectors with cosine similarity >= 0.01")
    rangeResults.select("id", "name", "similarity").show()
  }
}