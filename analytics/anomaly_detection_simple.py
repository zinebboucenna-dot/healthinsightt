import json
from kafka import KafkaConsumer
from datetime import datetime
from collections import defaultdict
import time

class SimpleAnomalyDetector:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'patient.monitoring.raw',
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='anomaly-detector-q4'
        )
        
        # Track abnormalities per patient in 1-minute windows
        self.abnormal_counts = defaultdict(lambda: defaultdict(int))
        self.window_start = time.time()
        self.total_in_window = 0
        self.abnormal_in_window = 0
        
    def run(self):
        print("\n" + "="*80)
        print("Q4: Real-Time Abnormal Detection (Streaming Analytics)")
        print("="*80 + "\n")
        print("✓ Connected to Kafka topic: patient.monitoring.raw")
        print("✓ Monitoring for abnormal vital signs...")
        print("✓ Detecting patients with multiple abnormalities")
        print("✓ Aggregating in 1-minute windows\n")
        print("-" * 80 + "\n")
        
        count = 0
        abnormal_count = 0
        
        try:
            for message in self.consumer:
                reading = message.value
                count += 1
                self.total_in_window += 1
                
                if reading['is_abnormal']:
                    abnormal_count += 1
                    self.abnormal_in_window += 1
                    
                    # Track by patient and metric
                    patient_id = reading['patient_id']
                    metric = reading['metric_type']
                    self.abnormal_counts[patient_id][metric] += 1
                    
                    # Display abnormal reading
                    print(f"⚠️  ABNORMAL DETECTED:")
                    print(f"   Patient: {patient_id}")
                    print(f"   Metric: {metric}")
                    print(f"   Value: {reading['value']} {reading['unit']}")
                    print(f"   Range: [{reading['threshold_min']}, {reading['threshold_max']}]")
                    print(f"   Time: {reading['reading_timestamp']}")
                    print()
                
                # Every 60 seconds, show window summary
                current_time = time.time()
                if current_time - self.window_start >= 60:
                    self.show_window_summary(current_time)
                    # Reset window counters
                    self.window_start = current_time
                    self.abnormal_counts.clear()
                    self.total_in_window = 0
                    self.abnormal_in_window = 0
                
                # Show progress
                if count % 20 == 0:
                    print(f"✓ Processed {count} readings ({abnormal_count} abnormal)", end='\r')
                    
        except KeyboardInterrupt:
            print("\n\n" + "="*80)
            print("📊 FINAL SUMMARY")
            print("="*80)
            print(f"Total readings processed: {count:,}")
            print(f"Abnormal readings detected: {abnormal_count:,}")
            if count > 0:
                print(f"Overall abnormality rate: {(abnormal_count/count*100):.2f}%")
            print("="*80)
            print("\n✅ Q4 COMPLETE: Real-time anomaly detection completed successfully")
            print("="*80 + "\n")
        finally:
            self.consumer.close()
    
    def show_window_summary(self, current_time):
        print("\n" + "="*80)
        print("📊 1-MINUTE WINDOW SUMMARY")
        print("="*80)
        print(f"Window end: {datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Readings in window: {self.total_in_window}")
        print(f"Abnormal readings: {self.abnormal_in_window}")
        
        if self.total_in_window > 0:
            rate = (self.abnormal_in_window / self.total_in_window * 100)
            print(f"Abnormality rate: {rate:.2f}%\n")
        
        # Show patients with multiple abnormalities (ALERT THRESHOLD)
        alerts_triggered = False
        if self.abnormal_counts:
            print("🚨 ALERTS - Patients with Multiple Abnormalities (≥2):")
            print("-" * 80)
            
            for patient_id, metrics in sorted(self.abnormal_counts.items()):
                total = sum(metrics.values())
                if total >= 2:  # Alert if patient has 2+ abnormal readings
                    alerts_triggered = True
                    print(f"\n  🔴 HIGH PRIORITY: Patient {patient_id}")
                    print(f"     Total abnormal readings: {total}")
                    for metric, count in sorted(metrics.items()):
                        print(f"     • {metric}: {count} abnormal reading(s)")
            
            if not alerts_triggered:
                print("  ✓ No high-priority alerts (all patients < 2 abnormalities)")
        else:
            print("  ✓ No abnormal readings in this window")
        
        print("\n" + "="*80 + "\n")

if __name__ == "__main__":
    print("\n🏥 HealthInsight - Real-Time Anomaly Detection System (Q4)")
    print("This demonstrates real-time streaming analytics on patient vital signs")
    print("Press Ctrl+C to stop\n")
    
    detector = SimpleAnomalyDetector()
    detector.run()