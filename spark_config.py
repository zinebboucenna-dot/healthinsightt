from pyspark.sql import SparkSession
import os

def create_spark_session_streaming(app_name="HealthInsight-Streaming"):
    """Create Spark session for streaming (Q4)"""
    
    # Set environment variables programmatically
    os.environ['HADOOP_HOME'] = 'C:/Users/zeyne/hadoop'
    os.environ['PYSPARK_PYTHON'] = 'python'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def create_spark_session_batch(app_name="HealthInsight-Batch"):
    """Create Spark session for batch processing (Q5)"""
    
    os.environ['HADOOP_HOME'] = 'C:/Users/zeyne/hadoop'
    os.environ['PYSPARK_PYTHON'] = 'python'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark