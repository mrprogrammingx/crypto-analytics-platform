try:
    from pyspark.sql import SparkSession
    _pyspark_available = True
except Exception:
    SparkSession = None  # type: ignore
    _pyspark_available = False


def create_session():
    if not _pyspark_available:
        raise RuntimeError("PySpark is not available in this environment")

    # Initialize the Spark Session
    spark = SparkSession.builder \
        .appName("InitialArchitectureSetup") \
        .master("local[*]") \
        .getOrCreate()
    
    return spark


if __name__ == "__main__":
    if not _pyspark_available:
        print("PySpark not installed; cannot create session")
    else:
        spark = create_session()
        # Print basic info to confirm it's running
        print(f"Spark Version: {spark.version}")
        print(f"Session successfully created: {spark}")
        # Stop the session
        spark.stop()