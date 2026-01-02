from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import os
import sys

def create_spark_session():
    """Create Spark session for batch processing"""
    
    os.environ['HADOOP_HOME'] = 'C:/Users/zeyne/hadoop'
    os.environ['PYSPARK_PYTHON'] = 'python'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'
    
    # Note: For Cassandra connector, you'd need:
    # .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0")
    # But we'll use a simpler approach - reading from CSV export
    
    spark = SparkSession.builder \
        .appName("HealthInsight-Q5-Daily-Statistics") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def export_cassandra_to_csv():
    """Export Cassandra data to CSV for Spark to read"""
    import subprocess
    
    print("📤 Exporting data from Cassandra...")
    
    # Export to CSV
    cmd = [
        'docker', 'exec', '-i', 'cassandra-1', 'cqlsh', '-e',
        "COPY healthinsight.patient_monitoring TO '/tmp/readings.csv' WITH HEADER=true;"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"❌ Export failed: {result.stderr}")
        return False
    
    # Copy from container to host
    subprocess.run(['docker', 'cp', 'cassandra-1:/tmp/readings.csv', 'data/readings.csv'])
    
    print("✓ Data exported to data/readings.csv\n")
    return True

def calculate_daily_statistics(spark):
    """Q5: Generate daily hospital-wide health statistics using Spark"""
    
    print("\n" + "="*80)
    print("Q5: Daily Hospital-Wide Health Statistics (Spark Batch Processing)")
    print("="*80 + "\n")
    
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Export data from Cassandra
    if not export_cassandra_to_csv():
        # If export fails, create sample data
        print("⚠️ Using sample data for demonstration\n")
        create_sample_data(spark)
        return
    
    print(f"📅 Report Date: {datetime.now().date()}")
    print(f"✓ Loading data from CSV...\n")
    
    # Define schema
    schema = StructType([
        StructField("patient_id", StringType()),
        StructField("reading_timestamp", TimestampType()),
        StructField("metric_type", StringType()),
        StructField("value", DoubleType()),
        StructField("unit", StringType()),
        StructField("device_id", StringType()),
        StructField("is_abnormal", BooleanType()),
        StructField("threshold_min", DoubleType()),
        StructField("threshold_max", DoubleType())
    ])
    
    # Read data
    try:
        readings_df = spark.read.csv("data/readings.csv", header=True, schema=schema)
    except:
        print("❌ Could not read data file")
        create_sample_data(spark)
        return
    
    total_count = readings_df.count()
    print(f"✓ Loaded {total_count:,} readings from Cassandra\n")
    print("-" * 80)
    
    if total_count == 0:
        print("\n⚠️ No data available")
        return
    
    # Cache for better performance
    readings_df.cache()
    
    # ========== STATISTIC 1: Overall Metrics Summary ==========
    print("\n📊 STATISTIC 1: Overall Health Metrics Summary")
    print("-" * 80)
    
    overall_stats = readings_df.groupBy("metric_type").agg(
        count("*").alias("total_readings"),
        round(avg("value"), 2).alias("avg_value"),
        round(stddev("value"), 2).alias("std_dev"),
        round(min("value"), 2).alias("min_value"),
        round(max("value"), 2).alias("max_value"),
        sum(when(col("is_abnormal") == True, 1).otherwise(0)).alias("abnormal_count")
    ).withColumn(
        "abnormal_percentage",
        round((col("abnormal_count") / col("total_readings") * 100), 2)
    ).orderBy("metric_type")
    
    overall_stats.show(truncate=False)
    
    # ========== STATISTIC 2: Patient-Level Analysis ==========
    print("\n👥 STATISTIC 2: Patient Health Summary")
    print("-" * 80)
    
    patient_stats = readings_df.groupBy("patient_id").agg(
        count("*").alias("total_readings"),
        sum(when(col("is_abnormal") == True, 1).otherwise(0)).alias("abnormal_readings"),
        countDistinct("metric_type").alias("metrics_measured")
    ).withColumn(
        "abnormal_percentage",
        round((col("abnormal_readings") / col("total_readings") * 100), 2)
    )
    
    total_patients = patient_stats.count()
    print(f"Total patients monitored: {total_patients}")
    
    print("\nTop 10 patients with highest abnormality rates:")
    patient_stats.orderBy(desc("abnormal_percentage")).show(10, truncate=False)
    
    # ========== STATISTIC 3: High-Risk Patients ==========
    print("\n🚨 STATISTIC 3: High-Risk Patients (>20% Abnormal Readings)")
    print("-" * 80)
    
    high_risk = patient_stats.filter(col("abnormal_percentage") > 20.0)
    high_risk_count = high_risk.count()
    
    print(f"High-risk patients identified: {high_risk_count}")
    if high_risk_count > 0:
        high_risk.orderBy(desc("abnormal_percentage")).show(20, truncate=False)
    else:
        print("✓ No high-risk patients detected")
    
    # ========== STATISTIC 4: Abnormality Rates ==========
    print("\n⚠️ STATISTIC 4: Abnormality Rates by Vital Sign")
    print("-" * 80)
    
    abnormality_rates = readings_df.groupBy("metric_type").agg(
        count("*").alias("total"),
        sum(when(col("is_abnormal") == True, 1).otherwise(0)).alias("abnormal"),
        round((sum(when(col("is_abnormal") == True, 1).otherwise(0)) / count("*") * 100), 2).alias("abnormal_rate")
    ).orderBy(desc("abnormal_rate"))
    
    abnormality_rates.show(truncate=False)
    
    # ========== STATISTIC 5: Device Performance ==========
    print("\n🔧 STATISTIC 5: Device Performance Analysis")
    print("-" * 80)
    
    device_stats = readings_df.groupBy("device_id").agg(
        count("*").alias("total_readings"),
        countDistinct("patient_id").alias("patients_monitored"),
        sum(when(col("is_abnormal") == True, 1).otherwise(0)).alias("abnormal_readings")
    ).withColumn(
        "abnormal_rate",
        round((col("abnormal_readings") / col("total_readings") * 100), 2)
    ).orderBy(desc("total_readings"))
    
    total_devices = device_stats.count()
    print(f"Total devices: {total_devices}")
    print("\nTop 10 most active devices:")
    device_stats.show(10, truncate=False)
    
    # ========== STATISTIC 6: Time-based Analysis ==========
    print("\n⏰ STATISTIC 6: Hourly Distribution")
    print("-" * 80)
    
    hourly_stats = readings_df.withColumn("hour", hour(col("reading_timestamp"))) \
        .groupBy("hour").agg(
            count("*").alias("readings_count"),
            sum(when(col("is_abnormal") == True, 1).otherwise(0)).alias("abnormal_count")
        ).orderBy("hour")
    
    print("Readings by hour:")
    hourly_stats.show(24, truncate=False)
    
    # ========== Save to MongoDB ==========
    print("\n💾 STATISTIC 7: Saving Results")
    print("-" * 80)
    
    # Convert to Python objects for MongoDB
    total_abnormal = readings_df.filter(col("is_abnormal") == True).count()
    
    summary_doc = {
        "report_date": datetime.now().date().isoformat(),
        "generated_at": datetime.now().isoformat(),
        "summary": {
            "total_readings": int(total_count),
            "total_patients": int(total_patients),
            "high_risk_patients": int(high_risk_count),
            "total_devices": int(total_devices),
            "total_abnormal": int(total_abnormal),
            "overall_abnormal_rate": round(float(total_abnormal / total_count * 100), 2) if total_count > 0 else 0
        }
    }
    
    print("Summary statistics:")
    for key, value in summary_doc["summary"].items():
        print(f" {key}: {value}")
    
    # Save to MongoDB (simplified)
    try:
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27018/?directConnection=true")
        db = client.healthinsight
        result = db.daily_statistics.insert_one(summary_doc)
        print(f"\n✓ Report saved to MongoDB (ID: {result.inserted_id})")
    except Exception as e:
        print(f"\n⚠️ Could not save to MongoDB: {e}")
    
    # ========== FINAL SUMMARY ==========
    print("\n" + "="*80)
    print("📊 FINAL SUMMARY")
    print("="*80)
    print(f"Report Date: {summary_doc['report_date']}")
    print(f"Total Readings: {summary_doc['summary']['total_readings']:,}")
    print(f"Total Patients: {summary_doc['summary']['total_patients']}")
    print(f"High-Risk Patients: {summary_doc['summary']['high_risk_patients']}")
    print(f"Total Devices: {summary_doc['summary']['total_devices']}")
    print(f"Overall Abnormality Rate: {summary_doc['summary']['overall_abnormal_rate']}%")
    print("="*80)
    print("\n✅ Q5 COMPLETE: Daily statistics generated with Spark Batch")
    print("="*80 + "\n")
    
    # Unpersist cached data
    readings_df.unpersist()

def create_sample_data(spark):
    """Create sample data for demonstration"""
    import random
    
    print("Creating sample data for demonstration...\n")
    
    sample_data = []
    patients = [f"P{i:06d}" for i in range(1, 51)]
    metrics = ["heart_rate", "blood_pressure_systolic", "temperature", "oxygen_saturation"]
    
    for _ in range(1000):
        sample_data.append((
            random.choice(patients),
            datetime.now(),
            random.choice(metrics),
            random.uniform(60, 140),
            "unit",
            f"DEV_{random.randint(1000, 9999)}",
            random.random() < 0.05, # 5% abnormal
            60.0,
            100.0
        ))
    
    schema = StructType([
        StructField("patient_id", StringType()),
        StructField("reading_timestamp", TimestampType()),
        StructField("metric_type", StringType()),
        StructField("value", DoubleType()),
        StructField("unit", StringType()),
        StructField("device_id", StringType()),
        StructField("is_abnormal", BooleanType()),
        StructField("threshold_min", DoubleType()),
        StructField("threshold_max", DoubleType())
    ])
    
    df = spark.createDataFrame(sample_data, schema)
    df.write.mode("overwrite").csv("data/readings.csv", header=True)
    print("✓ Sample data created\n")

if __name__ == "__main__":
    print("\n🏥 HealthInsight - Q5: Daily Statistics (Spark Batch Processing)")
    print("="*80 + "\n")
    
    spark = create_spark_session()
    
    try:
        calculate_daily_statistics(spark)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\nStopping Spark session...")
        spark.stop()