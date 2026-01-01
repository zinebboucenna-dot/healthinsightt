from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

def create_spark_session():
    """Create Spark session with Kafka support"""
    return SparkSession.builder \
        .appName("HealthInsight-Anomaly-Detection-Q4") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
        .getOrCreate()

def detect_anomalies(spark):
    """Q4: Real-time abnormal reading detection"""
    
    print("\n" + "="*70)
    print("Q4: Real-Time Abnormal Reading Detection")
    print("="*70 + "\n")
    
    # Define schema for incoming Kafka messages
    schema = StructType([
        StructField("patient_id", StringType(), False),
        StructField("reading_timestamp", StringType(), False),
        StructField("metric_type", StringType(), False),
        StructField("value", DoubleType(), False),
        StructField("unit", StringType(), False),
        StructField("device_id", StringType(), False),
        StructField("is_abnormal", BooleanType(), False),
        StructField("threshold_min", DoubleType(), False),
        StructField("threshold_max", DoubleType(), False)
    ])
    
    print("✓ Connecting to Kafka topic: patient.monitoring.raw")
    
    # Read from Kafka
    readings_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "patient.monitoring.raw") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print("✓ Connected to Kafka")
    print("✓ Parsing JSON messages\n")
    
    # Parse JSON from Kafka value
    parsed_readings = readings_stream.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    # Filter only abnormal readings
    abnormal_readings = parsed_readings.filter(col("is_abnormal") == True)
    
    print("Starting real-time anomaly detection...")
    print("Monitoring for abnormal vital signs...\n")
    print("-" * 70)
    
    # Console output for monitoring
    query_console = abnormal_readings \
        .select(
            col("patient_id"),
            col("reading_timestamp"),
            col("metric_type"),
            col("value"),
            col("unit"),
            col("threshold_min"),
            col("threshold_max")
        ) \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='5 seconds') \
        .start()
    
    # Windowed aggregation: Count abnormalities per patient in 1-minute windows
    windowed_anomalies = abnormal_readings \
        .withWatermark("reading_timestamp", "1 minute") \
        .groupBy(
            window(col("reading_timestamp"), "1 minute"),
            col("patient_id"),
            col("metric_type")
        ) \
        .agg(
            count("*").alias("abnormal_count"),
            avg("value").alias("avg_value"),
            max("value").alias("max_value"),
            min("value").alias("min_value")
        ) \
        .filter(col("abnormal_count") >= 2)  # Alert if 2+ abnormalities in window
    
    # Write aggregated alerts to console
    query_aggregated = windowed_anomalies \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("patient_id"),
            col("metric_type"),
            col("abnormal_count"),
            col("avg_value"),
            col("max_value"),
            col("min_value")
        ) \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='10 seconds') \
        .start()
    
    print("\n✅ Q4 Active: Real-time anomaly detection running")
    print("   - Monitoring all incoming readings")
    print("   - Filtering abnormal vital signs")
    print("   - Aggregating in 1-minute windows")
    print("   - Alerting when 2+ abnormalities detected\n")
    print("Press Ctrl+C to stop...\n")
    
    # Wait for termination
    try:
        query_console.awaitTermination()
    except KeyboardInterrupt:
        print("\n\n✅ Q4 Complete: Anomaly detection stopped")
        print("="*70 + "\n")
        query_console.stop()
        query_aggregated.stop()

if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Reduce verbosity
    
    try:
        detect_anomalies(spark)
    finally:
        spark.stop()