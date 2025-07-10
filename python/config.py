"""
Configuration file for Milvus PySpark Demo
==========================================

This file loads configuration from environment variables (.env file).
Please copy env.example to .env and update the values with your actual credentials.
"""

import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file
load_dotenv()

home_dir = Path.home()
ivy_cache = home_dir / ".ivy2" / "cache"
m2_repo   = home_dir / ".m2" / "repository"

# Helper function to get environment variable with fallback
def get_env(key, default=None):
    """Get environment variable with optional default value"""
    return os.getenv(key, default)

# Milvus Configuration
MILVUS_CONFIG = {
    # Milvus connection settings
    "uri": get_env("MILVUS_URI", "your_milvus_uri_here"),
    "token": get_env("MILVUS_TOKEN", "your_milvus_token_here"),
    "collection_name": get_env("MILVUS_COLLECTION_NAME", "your_collection_name"),
    "database_name": get_env("MILVUS_DATABASE_NAME", "default"),
    
    # S3 Configuration (for Milvus storage)
    "s3_filesystem_type": get_env("S3_FILESYSTEM_TYPE", "s3a://"),
    "s3_endpoint": get_env("S3_ENDPOINT", "xxx"),
    "s3_access_key": get_env("S3_ACCESS_KEY", "xxx"),
    "s3_secret_key": get_env("S3_SECRET_KEY", "xxx"),
    "s3_bucket_name": get_env("S3_BUCKET_NAME", "xxx"),
    "s3_root_path": get_env("S3_ROOT_PATH", "xxx"),
    "s3_use_ssl": get_env("S3_USE_SSL", "true"),
    "s3_path_style_access": get_env("S3_PATH_STYLE_ACCESS", "false")
}

# Spark Configuration
SPARK_CONFIG = {
    "app_name": get_env("SPARK_APP_NAME", "PySpark Milvus Demo"),
    "maven_coords": get_env("MAVEN_COORDS", "com.zilliz:spark-connector_2.13:0.1.6-SNAPSHOT"),
    "repositories": get_env("MAVEN_REPOSITORIES", 
                          "file://" + str(m2_repo) + "/,"
                          "https://repo1.maven.org/maven2/,"
                          "https://central.sonatype.com/repository/maven-snapshots/"),
    # Local Maven repository path
    "local_repository": get_env("LOCAL_MAVEN_REPOSITORY", str(m2_repo)),
    "snapshot_repo": get_env("SNAPSHOT_REPO", "https://central.sonatype.com/repository/maven-snapshots/"),
    "additional_configs": {
        "spark.jars.packages.defaultArtifactRepository": "https://repo1.maven.org/maven2/",
        "spark.jars.packages.localRepository": str(m2_repo),
    }
}

# Print loaded configuration for debugging (optional)
if __name__ == "__main__":
    print("🔧 Configuration loaded from environment variables:")
    print(f"   Maven Coords: {SPARK_CONFIG['maven_coords']}")
    print(f"   Repositories: {SPARK_CONFIG['repositories']}")
    print(f"   Local Repository: {SPARK_CONFIG['local_repository']}")
    print(f"   Snapshot Repo: {SPARK_CONFIG['snapshot_repo']}")
    print(f"   Milvus URI: {MILVUS_CONFIG['uri']}")
    print(f"   Collection Name: {MILVUS_CONFIG['collection_name']}")
    print(f"   S3 Endpoint: {MILVUS_CONFIG['s3_endpoint']}")
    print(f"   S3 Bucket: {MILVUS_CONFIG['s3_bucket_name']}") 