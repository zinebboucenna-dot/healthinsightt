from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import os

def create_spark_session():
    """Create Spark session with Kafka support"""
    
    # Set environment variables
    os.environ['HADOOP_HOME'] = 'C:/Users/zeyne/hadoop'
    os.environ['PYSPARK_PYTHON'] = 'python'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'
    
    spark = SparkSession.builder \
        .appName("HealthInsight-Q4-Anomaly-Detection") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-q4-checkpoint") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def run_anomaly_detection(spark):
    """Q4: Real-time abnormal reading detection with Spark Streaming"""
    
    print("\n" + "="*80)
    print("Q4: Real-Time Abnormal Detection (Spark Streaming)")
    print("="*80 + "\n")
    
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
    
    print("✓ Connecting to Kafka (patient.monitoring.raw)...")
    
    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "patient.monitoring.raw") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print("✓ Connected to Kafka")
    print("✓ Parsing JSON messages\n")
    
    # Parse JSON from Kafka value
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    # Add processing timestamp
    readings_df = parsed_df.withColumn("processed_at", current_timestamp())
    
    # Filter only abnormal readings
    abnormal_df = readings_df.filter(col("is_abnormal") == True)
    
    print("="*80)
    print("STREAMING OUTPUT - Individual Abnormal Readings")
    print("="*80 + "\n")
    
    # Query 1: Display abnormal readings in real-time
    query1 = abnormal_df \
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
        .trigger(processingTime='10 seconds') \
        .start()
    
    print("\n" + "="*80)
    print("WINDOWED AGGREGATION - Multiple Abnormalities Alert")
    print("="*80 + "\n")
    
    # Convert string timestamp to proper timestamp type
    readings_with_ts = readings_df.withColumn(
        "timestamp",
        to_timestamp(col("reading_timestamp"))
    )
    
    # Query 2: Windowed aggregation - detect multiple abnormalities
    windowed_anomalies = readings_with_ts \
        .filter(col("is_abnormal") == True) \
        .withWatermark("timestamp", "2 minutes") \
        .groupBy(
            window(col("timestamp"), "1 minute"),
            col("patient_id"),
            col("metric_type")
        ) \
        .agg(
            count("*").alias("abnormal_count"),
            avg("value").alias("avg_value"),
            max("value").alias("max_value"),
            min("value").alias("min_value")
        ) \
        .filter(col("abnormal_count") >= 2) # Alert if 2+ abnormalities
    
    query2 = windowed_anomalies \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("patient_id"),
            col("metric_type"),
            col("abnormal_count"),
            round(col("avg_value"), 2).alias("avg_value"),
            round(col("max_value"), 2).alias("max_value"),
            round(col("min_value"), 2).alias("min_value")
        ) \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='15 seconds') \
        .start()
    
    # Query 3: Overall statistics
    overall_stats = readings_df \
        .withWatermark("processed_at", "2 minutes") \
        .groupBy(window(col("processed_at"), "1 minute")) \
        .agg(
            count("*").alias("total_readings"),
            sum(when(col("is_abnormal") == True, 1).otherwise(0)).alias("abnormal_readings"),
            countDistinct("patient_id").alias("unique_patients")
        )
    
    print("\n" + "="*80)
    print("OVERALL STATISTICS - 1 Minute Windows")
    print("="*80 + "\n")
    
    query3 = overall_stats \
        .select(
            col("window.start").alias("period_start"),
            col("window.end").alias("period_end"),
            col("total_readings"),
            col("abnormal_readings"),
            col("unique_patients"),
            round((col("abnormal_readings") / col("total_readings") * 100), 2).alias("abnormal_rate_pct")
        ) \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='20 seconds') \
        .start()
    
    print("\n✅ Q4 Spark Streaming Active:")
    print(" - Real-time abnormal reading detection")
    print(" - Windowed aggregation (1-minute windows)")
    print(" - Multiple abnormality alerts (≥2 per window)")
    print(" - Overall statistics tracking\n")
    print("Press Ctrl+C to stop...\n")
    
    try:
        # Wait for termination
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n\n" + "="*80)
        print("✅ Q4 COMPLETE: Spark Streaming stopped successfully")
        print("="*80 + "\n")
        query1.stop()
        query2.stop()
        query3.stop()

if __name__ == "__main__":
    print("\n🏥 HealthInsight - Q4: Real-Time Anomaly Detection (Spark Streaming)")
    print("="*80 + "\n")
    
    spark = create_spark_session()
    
    try:
        run_anomaly_detection(spark)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\nStopping Spark session...")
        spark.stop()