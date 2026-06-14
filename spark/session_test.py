from pyspark.sql import SparkSession

def create_session():
    # Initialize the Spark Session
    spark = SparkSession.builder \
        .appName("InitialArchitectureSetup") \
        .master("local[*]") \
        .getOrCreate()
    
    return spark

if __name__ == "__main__":
    spark = create_session()
    
    # Print basic info to confirm it's running
    print(f"Spark Version: {spark.version}")
    print(f"Session successfully created: {spark}")
    
    # Stop the session
    spark.stop()