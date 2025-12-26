import json
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer
from pymongo import MongoClient

class MonitoringSimulator:
    """Simulates IoT medical device readings"""
    
    # Normal vital signs ranges
    VITALS_CONFIG = {
        'heart_rate': {
            'normal_range': (60, 100),
            'abnormal_low': 45,
            'abnormal_high': 120,
            'unit': 'bpm'
        },
        'blood_pressure_systolic': {
            'normal_range': (90, 120),
            'abnormal_low': 80,
            'abnormal_high': 140,
            'unit': 'mmHg'
        },
        'blood_pressure_diastolic': {
            'normal_range': (60, 80),
            'abnormal_low': 50,
            'abnormal_high': 90,
            'unit': 'mmHg'
        },
        'oxygen_saturation': {
            'normal_range': (95, 100),
            'abnormal_low': 90,
            'abnormal_high': 100,
            'unit': '%'
        },
        'temperature': {
            'normal_range': (36.1, 37.2),
            'abnormal_low': 35.0,
            'abnormal_high': 38.5,
            'unit': '°C'
        },
        'respiratory_rate': {
            'normal_range': (12, 20),
            'abnormal_low': 10,
            'abnormal_high': 25,
            'unit': 'breaths/min'
        }
    }
    
    def __init__(self, kafka_broker="localhost:9092", mongo_uri="mongodb://localhost:27017/"):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            compression_type='gzip'
        )
        
        # Get patient list from MongoDB
        mongo_client = MongoClient(mongo_uri)
        self.patients = list(mongo_client.healthinsight.patients.find({}, {"patient_id": 1}))
        
        if not self.patients:
            raise ValueError("No patients found in database. Run patient_generator.py first.")
        
        print(f"✓ Loaded {len(self.patients)} patients for simulation")
    
    def generate_reading(self, patient_id, metric_type, abnormal_chance=0.05):
        """Generate a single vital sign reading"""
        config = self.VITALS_CONFIG[metric_type]
        
        # Decide if this reading should be abnormal
        is_abnormal = random.random() < abnormal_chance
        
        if is_abnormal:
            # Generate abnormal value
            if random.random() < 0.5:
                value = random.uniform(config['abnormal_low'], config['normal_range'][0])
            else:
                value = random.uniform(config['normal_range'][1], config['abnormal_high'])
        else:
            # Generate normal value
            value = random.normalvariate(
                sum(config['normal_range']) / 2,
                (config['normal_range'][1] - config['normal_range'][0]) / 6
            )
        
        # Round to appropriate precision
        value = round(value, 1)
        
        reading = {
            'patient_id': patient_id,
            'reading_timestamp': datetime.utcnow().isoformat(),
            'metric_type': metric_type,
            'value': value,
            'unit': config['unit'],
            'device_id': f"DEV_{random.randint(1000, 9999)}",
            'is_abnormal': is_abnormal,
            'threshold_min': config['normal_range'][0],
            'threshold_max': config['normal_range'][1]
        }
        
        return reading
    
    def simulate_patient_cycle(self, patient_id):
        """Generate a complete set of vitals for one patient"""
        readings = []
        
        for metric_type in self.VITALS_CONFIG.keys():
            reading = self.generate_reading(patient_id, metric_type)
            readings.append(reading)
            
            # Send to Kafka
            self.producer.send('patient.monitoring.raw', value=reading)
            
            # Send abnormal readings to alert topic
            if reading['is_abnormal']:
                self.producer.send('patient.monitoring.abnormal', value=reading)
        
        return readings
    
    def run_simulation(self, duration_seconds=60, interval_seconds=5):
        """Run continuous simulation"""
        print(f"Starting simulation for {duration_seconds} seconds...")
        print(f"Generating readings every {interval_seconds} seconds")
        
        start_time = time.time()
        reading_count = 0
        
        try:
            while time.time() - start_time < duration_seconds:
                # Select random patients for this cycle
                num_patients = min(20, len(self.patients))
                selected_patients = random.sample(self.patients, num_patients)
                
                for patient in selected_patients:
                    readings = self.simulate_patient_cycle(patient['patient_id'])
                    reading_count += len(readings)
                
                # Flush to ensure delivery
                self.producer.flush()
                
                print(f"✓ Generated {reading_count} readings", end='\r')
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print("\n\nSimulation stopped by user")
        finally:
            self.producer.close()
            print(f"\n✓ Simulation complete: {reading_count} total readings")

# Usage
if __name__ == "__main__":
    simulator = MonitoringSimulator()
    simulator.run_simulation(duration_seconds=300, interval_seconds=5)