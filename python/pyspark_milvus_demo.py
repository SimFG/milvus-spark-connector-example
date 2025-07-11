#!/usr/bin/env python3
"""
PySpark Milvus Demo with Configuration
=====================================

This demo shows how to use Milvus Spark Connector to read data from Milvus.
Uses external configuration file for easy setup.

Usage:
    python pyspark_milvus_demo_with_config.py

Requirements:
    - Update config.py with your actual Milvus and S3 credentials
    - Ensure PySpark is installed in your environment
"""

from pyspark.sql import SparkSession
import sys

try:
    from config import MILVUS_CONFIG, SPARK_CONFIG, ivy_cache
except ImportError:
    print("Error: config.py not found or invalid.")
    print("Please make sure config.py exists and contains MILVUS_CONFIG and SPARK_CONFIG.")
    sys.exit(1)

def create_spark_session():
    """
    Create SparkSession with Milvus connector JAR and optimized settings
    """
    print("Initializing Spark session with Milvus connector...")
    
    builder = SparkSession.builder \
        .appName(SPARK_CONFIG["app_name"]) \
        .config("spark.jars.repositories", SPARK_CONFIG["repositories"]) \
        .config("spark.jars.packages", 
                           f"{SPARK_CONFIG['maven_coords']}") \
                    .config(
                        "spark.jars.excludes",
                        "org.slf4j:slf4j-api,org.slf4j:slf4j-log4j12,org.slf4j:slf4j-simple"
                    ) \
                    .config("spark.jars.ivy", str(ivy_cache)) \
                    .config("spark.driver.userClassPathFirst", "true") \
                    .config("spark.executor.userClassPathFirst", "true")
    
    # Add additional Spark configurations
    for key, value in SPARK_CONFIG["additional_configs"].items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"Spark version: {spark.version}")
    print(f"Spark application ID: {spark.sparkContext.applicationId}")
    
    return spark

def validate_config():
    """
    Validate configuration before running the demo
    """
    required_milvus_keys = ["uri", "token", "collection_name"]
    
    for key in required_milvus_keys:
        if key not in MILVUS_CONFIG or MILVUS_CONFIG[key] in [None, "", "your_milvus_uri_here", "your_milvus_token_here", "your_collection_name"]:
            print(f"Error: Please update '{key}' in config.py with your actual value.")
            return False
    
    return True

def read_milvus_data(spark, config):
    """
    Read data from Milvus using the Spark Connector
    
    Args:
        spark: SparkSession
        config: Dictionary containing Milvus configuration
    """
    
    print("Configuring Milvus data reader...")
    
    # Build the reader with all options
    reader = spark.read.format("milvus")
    
    # Milvus connection options
    reader = reader.option("milvus.uri", config["uri"]) \
                   .option("milvus.token", config["token"]) \
                   .option("milvus.collection.name", config["collection_name"]) \
                   .option("milvus.database.name", config.get("database_name", "default"))
    
    # S3 configuration options
    s3_options = {
        "s3.fs": config.get("s3_filesystem_type", "s3a://"),
        "s3.endpoint": config["s3_endpoint"],
        "s3.user": config["s3_access_key"],
        "s3.password": config["s3_secret_key"],
        "s3.bucket": config["s3_bucket_name"],
        "s3.rootPath": config["s3_root_path"],
        "s3.useSSL": config.get("s3_use_ssl", "true"),
        "s3.pathStyleAccess": config.get("s3_path_style_access", "false")
    }
    
    for key, value in s3_options.items():
        reader = reader.option(key, value)
    
    # Load the data
    print("Loading data from Milvus...")
    milvus_df = reader.load()
    
    return milvus_df

def analyze_data(milvus_df):
    """
    Perform basic analysis on the Milvus data
    """
    print("\n" + "="*50)
    print("DATA ANALYSIS")
    print("="*50)
    
    try:
        milvus_df.cache()
        print("   ✅ Data cached successfully")
    except Exception as e:
        print(f"   ⚠️  Data caching failed: {e}")
        print("   ℹ️  Continuing with other operations...")
    
    # Show basic info
    print("\n📊 Dataset Overview:")
    print(f"   Columns: {len(milvus_df.columns)}")
    print(f"   Column names: {milvus_df.columns}")
    
    # Show schema
    print("\n📋 Schema:")
    milvus_df.printSchema()
    
    # Show sample data
    print("\n🔍 Sample Data (first 5 rows):")
    try:
        milvus_df.show(5)
        print("   ✅ Data query successful")
    except Exception as e:
        print(f"   ❌ Data query failed: {str(e)[:100]}...")
        print("   ℹ️  Trying alternative data query methods...")
        
        # Try simpler query method
        try:
            print("   📊 Attempting to get data column information:")
            for col in milvus_df.columns:
                print(f"      - {col}")
            
            # Try to show only first few columns with simple data
            simple_cols = [col for col in milvus_df.columns if col not in ['title_vector', '$meta']][:3]
            print(f"\n   📋 Showing first 3 simple fields {simple_cols}:")
            milvus_df.select(*simple_cols).show(3, truncate=True)
            print("   ✅ Simplified data query successful")
            
        except Exception as e2:
            print(f"   ❌ Simplified query also failed: {str(e2)[:100]}...")
            print("   ℹ️  Schema reading functionality still works normally")
    
    # Show count and statistics
    print("\n📈 Statistics:")
    try:
        record_count = milvus_df.count()
        print(f"   Total records: {record_count:,}")
        print("   ✅ Data count successful")
    except Exception as e:
        print(f"   ❌ Data count failed: {str(e)[:100]}...")
    
    # Show basic statistics for numeric columns
    try:
        print("\n📊 Numeric Statistics:")
        numeric_stats = milvus_df.describe()
        numeric_stats.show()
        print("   ✅ Data statistics successful")
    except Exception as e:
        print(f"   ❌ Data statistics failed: {str(e)[:100]}...")
    
    # Show distinct values for first few columns (if reasonable)
    try:
        print("\n🔢 Distinct Values:")
        for col in milvus_df.columns[:3]:  # Only check first 3 columns
            if col not in ['title_vector', '$meta']:  # Skip complex fields
                try:
                    distinct_count = milvus_df.select(col).distinct().count()
                    print(f"   Distinct values in '{col}': {distinct_count:,}")
                except Exception as e:
                    print(f"   Could not count distinct values for '{col}': {str(e)[:50]}...")
        print("   ✅ Distinct count statistics successful")
    except Exception as e:
        print(f"   ❌ Distinct count statistics failed: {str(e)[:100]}...")


def main():
    """
    Main function to demonstrate Milvus Spark Connector usage
    """
    
    print("🚀 PySpark Milvus Demo Starting...")
    print("="*50)
    
    # Validate configuration
    if not validate_config():
        print("\n❌ Configuration validation failed. Please check config.py")
        return
    
    with create_spark_session() as spark:
        try:
            # Read data from Milvus
            milvus_df = read_milvus_data(spark, MILVUS_CONFIG)
            
            # Analyze the data
            analyze_data(milvus_df)
            
            # Example: Save data to parquet (optional)
            # print("\n💾 Saving data to parquet...")
            # milvus_df.write.mode("overwrite").parquet("milvus_data.parquet")
            # print("   Data saved to milvus_data.parquet")
            
            print("\n✅ Demo completed successfully!")
            
        except Exception as e:
            print(f"\n❌ Error occurred: {str(e)}")
            print("\n🔧 Troubleshooting tips:")
            print("1. Verify your Milvus URI and token in config.py")
            print("2. Ensure the collection exists in your Milvus instance")
            print("3. Check S3 credentials and configuration")
            print("4. Verify network connectivity to Milvus and S3")
            print("5. Check if the Milvus Spark Connector JAR is accessible")
            
            # Print more detailed error for debugging
            import traceback
            print("\n🐛 Detailed error trace:")
            traceback.print_exc()
        print("Demo finished.")

if __name__ == "__main__":
    main()