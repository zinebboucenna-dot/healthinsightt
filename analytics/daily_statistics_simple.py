import subprocess
import json
from datetime import datetime
from collections import defaultdict
from pymongo import MongoClient

def get_cassandra_data():
    """Read data from Cassandra using cqlsh"""
    print("📊 Reading data from Cassandra...")
    
    cmd = [
        'docker', 'exec', '-i', 'cassandra-1', 'cqlsh', '-e',
        "SELECT patient_id, reading_timestamp, metric_type, value, unit, is_abnormal FROM healthinsight.patient_monitoring;"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"❌ Error reading from Cassandra: {result.stderr}")
        return []
    
    # Parse output
    lines = result.stdout.strip().split('\n')
    readings = []
    
    for line in lines[3:-2]:  # Skip header and footer
        parts = line.split('|')
        if len(parts) >= 6:
            try:
                reading = {
                    'patient_id': parts[0].strip(),
                    'metric_type': parts[2].strip(),
                    'value': float(parts[3].strip()),
                    'unit': parts[4].strip(),
                    'is_abnormal': parts[5].strip().lower() == 'true'
                }
                readings.append(reading)
            except:
                continue
    
    return readings

def calculate_statistics(readings):
    """Q5: Calculate hospital-wide statistics"""
    
    print("\n" + "="*80)
    print("Q5: Daily Hospital-Wide Health Statistics (Batch Processing)")
    print("="*80 + "\n")
    
    if not readings:
        print("❌ No data found in Cassandra")
        print("   Run monitoring simulator and Kafka consumer first\n")
        return None
    
    total_readings = len(readings)
    print(f"✓ Loaded {total_readings:,} readings from Cassandra\n")
    print("-" * 80)
    
    # ========== STATISTIC 1: Overall Summary by Metric ==========
    print("\n📊 STATISTIC 1: Health Metrics Summary")
    print("-" * 80)
    
    metric_stats = defaultdict(lambda: {
        'count': 0,
        'values': [],
        'abnormal': 0
    })
    
    for r in readings:
        metric = r['metric_type']
        metric_stats[metric]['count'] += 1
        metric_stats[metric]['values'].append(r['value'])
        if r['is_abnormal']:
            metric_stats[metric]['abnormal'] += 1
    
    print(f"\n{'Metric':<30} {'Total':<10} {'Avg Value':<12} {'Abnormal':<10} {'Rate':<10}")
    print("-" * 80)
    
    for metric, stats in sorted(metric_stats.items()):
        avg_val = sum(stats['values']) / len(stats['values'])
        abnormal_rate = (stats['abnormal'] / stats['count'] * 100)
        print(f"{metric:<30} {stats['count']:<10} {avg_val:<12.2f} {stats['abnormal']:<10} {abnormal_rate:<10.2f}%")
    
    # ========== STATISTIC 2: Patient-Level Analysis ==========
    print("\n\n👥 STATISTIC 2: Patient Health Summary")
    print("-" * 80)
    
    patient_stats = defaultdict(lambda: {
        'total': 0,
        'abnormal': 0,
        'metrics': set()
    })
    
    for r in readings:
        pid = r['patient_id']
        patient_stats[pid]['total'] += 1
        patient_stats[pid]['metrics'].add(r['metric_type'])
        if r['is_abnormal']:
            patient_stats[pid]['abnormal'] += 1
    
    total_patients = len(patient_stats)
    print(f"Total patients monitored: {total_patients}\n")
    
    # Find high-risk patients
    high_risk = []
    for pid, stats in patient_stats.items():
        abnormal_pct = (stats['abnormal'] / stats['total'] * 100)
        if abnormal_pct > 20.0:
            high_risk.append((pid, stats['total'], stats['abnormal'], abnormal_pct))
    
    print(f"🚨 High-risk patients (>20% abnormal): {len(high_risk)}")
    
    if high_risk:
        print("\nTop 10 high-risk patients:")
        print(f"{'Patient ID':<15} {'Total':<10} {'Abnormal':<10} {'Rate':<10}")
        print("-" * 50)
        for pid, total, abnormal, rate in sorted(high_risk, key=lambda x: x[3], reverse=True)[:10]:
            print(f"{pid:<15} {total:<10} {abnormal:<10} {rate:<10.2f}%")
    
    # ========== STATISTIC 3: Abnormality Analysis ==========
    print("\n\n⚠️  STATISTIC 3: Abnormality Rates by Vital Sign")
    print("-" * 80)
    
    total_abnormal = sum(1 for r in readings if r['is_abnormal'])
    overall_rate = (total_abnormal / total_readings * 100)
    
    print(f"Total abnormal readings: {total_abnormal:,}")
    print(f"Overall abnormality rate: {overall_rate:.2f}%\n")
    
    print(f"{'Metric':<30} {'Abnormal Count':<15} {'Rate':<10}")
    print("-" * 60)
    
    abnormal_by_metric = []
    for metric, stats in sorted(metric_stats.items(), key=lambda x: x[1]['abnormal'], reverse=True):
        rate = (stats['abnormal'] / stats['count'] * 100)
        abnormal_by_metric.append({
            'metric': metric,
            'count': stats['abnormal'],
            'rate': rate
        })
        print(f"{metric:<30} {stats['abnormal']:<15} {rate:<10.2f}%")
    
    # ========== Create Summary Document ==========
    summary_doc = {
        'report_date': datetime.now().date().isoformat(),
        'generated_at': datetime.now().isoformat(),
        'summary': {
            'total_readings': total_readings,
            'total_patients': total_patients,
            'total_abnormal': total_abnormal,
            'overall_abnormal_rate': overall_rate,
            'high_risk_patients': len(high_risk)
        },
        'metric_statistics': [
            {
                'metric_type': metric,
                'total_readings': stats['count'],
                'avg_value': sum(stats['values']) / len(stats['values']),
                'abnormal_count': stats['abnormal'],
                'abnormal_rate': (stats['abnormal'] / stats['count'] * 100)
            }
            for metric, stats in metric_stats.items()
        ],
        'abnormality_rates': abnormal_by_metric,
        'high_risk_patients': [
            {
                'patient_id': pid,
                'total_readings': total,
                'abnormal_readings': abnormal,
                'abnormal_percentage': rate
            }
            for pid, total, abnormal, rate in high_risk[:20]
        ]
    }
    
    return summary_doc

def save_to_mongodb(summary_doc):
    """Save statistics to MongoDB"""
    print("\n\n💾 Saving Results to MongoDB")
    print("-" * 80)
    
    try:
        client = MongoClient("mongodb://localhost:27018/?directConnection=true")
        db = client.healthinsight
        
        # Save to daily_statistics collection
        result = db.daily_statistics.insert_one(summary_doc)
        
        print(f"✓ Report saved to MongoDB")
        print(f"  Database: healthinsight")
        print(f"  Collection: daily_statistics")
        print(f"  Document ID: {result.inserted_id}")
        
        return True
    except Exception as e:
        print(f"❌ Error saving to MongoDB: {e}")
        return False

if __name__ == "__main__":
    print("\n🏥 HealthInsight - Daily Statistics Generator (Q5)")
    print("="*80 + "\n")
    
    # Read data from Cassandra
    readings = get_cassandra_data()
    
    if not readings:
        print("\n⚠️  No data available for analysis")
        print("   Make sure you've run:")
        print("   1. python data_generator/monitoring_simulator.py")
        print("   2. python ingestion/kafka_to_cassandra_simple.py\n")
        exit(1)
    
    # Calculate statistics
    summary_doc = calculate_statistics(readings)
    
    if summary_doc:
        # Save to MongoDB
        if save_to_mongodb(summary_doc):
            print("\n" + "="*80)
            print("📊 FINAL SUMMARY")
            print("="*80)
            print(f"Report Date: {summary_doc['report_date']}")
            print(f"Total Readings: {summary_doc['summary']['total_readings']:,}")
            print(f"Total Patients: {summary_doc['summary']['total_patients']}")
            print(f"High-Risk Patients: {summary_doc['summary']['high_risk_patients']}")
            print(f"Overall Abnormality Rate: {summary_doc['summary']['overall_abnormal_rate']:.2f}%")
            print("="*80)
            print("\n✅ Q5 COMPLETE: Daily statistics generated and saved successfully")
            print("="*80 + "\n")
        else:
            print("\n❌ Q5 INCOMPLETE: Failed to save to MongoDB\n")
    else:
        print("\n❌ Q5 INCOMPLETE: Failed to calculate statistics\n")