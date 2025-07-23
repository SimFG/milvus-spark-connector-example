#!/usr/bin/env python3
"""
Simple Milvus Data Diff Tool
"""

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pathlib import Path

script_dir = Path(__file__).parent
env_file = script_dir / ".env"
load_dotenv(env_file)

def create_spark():
    home_dir = Path.home()
    ivy_cache = home_dir / ".ivy2" / "cache"
    m2_repo = home_dir / ".m2" / "repository"
    
    maven_coords = os.getenv("MAVEN_COORDS", "com.zilliz:spark-connector_2.13:0.1.11-SNAPSHOT")
    repositories = f"file://{m2_repo}/,https://repo1.maven.org/maven2/,https://central.sonatype.com/repository/maven-snapshots/"
    
    return SparkSession.builder \
        .appName("Data Diff") \
        .config("spark.jars.repositories", repositories) \
        .config("spark.jars.packages", maven_coords) \
        .config("spark.jars.excludes", "org.slf4j:slf4j-api,org.slf4j:slf4j-log4j12,org.slf4j:slf4j-simple") \
        .config("spark.jars.ivy", str(ivy_cache)) \
        .config("spark.driver.userClassPathFirst", "true") \
        .config("spark.executor.userClassPathFirst", "true") \
        .getOrCreate()

def read_data(spark, prefix):
    return spark.read.format("milvus") \
        .option("milvus.uri", os.getenv(f"{prefix}_MILVUS_URI")) \
        .option("milvus.token", os.getenv(f"{prefix}_MILVUS_TOKEN")) \
        .option("milvus.database.name", os.getenv(f"{prefix}_MILVUS_DATABASE_NAME")) \
        .option("milvus.collection.name", os.getenv(f"{prefix}_MILVUS_COLLECTION_NAME")) \
        .option("s3.fs", os.getenv(f"{prefix}_S3_FILESYSTEM_TYPE")) \
        .option("s3.endpoint", os.getenv(f"{prefix}_S3_ENDPOINT")) \
        .option("s3.user", os.getenv(f"{prefix}_S3_ACCESS_KEY")) \
        .option("s3.password", os.getenv(f"{prefix}_S3_SECRET_KEY")) \
        .option("s3.bucket", os.getenv(f"{prefix}_S3_BUCKET_NAME")) \
        .option("s3.rootPath", os.getenv(f"{prefix}_S3_ROOT_PATH")) \
        .option("s3.useSSL", os.getenv(f"{prefix}_S3_USE_SSL")) \
        .option("s3.pathStyleAccess", os.getenv(f"{prefix}_S3_PATH_STYLE_ACCESS")) \
        .option("reader.field.ids", "100") \
        .load()

def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("INFO")
    
    df_a = read_data(spark, "A")
    df_a.cache()
    df_b = read_data(spark, "B")
    df_b.cache()
    
    only_a = df_a.exceptAll(df_b)
    only_a.cache()
    only_b = df_b.exceptAll(df_a)
    only_b.cache()
    
    # 输出结果
    print("📊 Data Statistics:")
    print(f"   A Total: {df_a.count():,}")
    print(f"   B Total: {df_b.count():,}")
    print(f"   A Only: {only_a.count():,}")
    print(f"   B Only: {only_b.count():,}")
    
    print("\n🔴 A Only (Top 5):")
    only_a.show(5)
    
    print("\n🔵 B Only (Top 5):")
    only_b.show(5)
    
    spark.stop()

if __name__ == "__main__":
    main()