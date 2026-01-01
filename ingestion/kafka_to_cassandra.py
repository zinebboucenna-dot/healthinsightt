import json
import uuid
from datetime import datetime
from kafka import KafkaConsumer
import subprocess

def insert_to_cassandra(reading):
    """Insert one reading into Cassandra using cqlsh"""
    patient_id = reading['patient_id']
    timestamp = reading['reading_timestamp'].replace('Z', '+00:00')
    
    # Generate UUID from patient_id
    if '-' in patient_id:
        patient_uuid = patient_id
    else:
        patient_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, patient_id))
    
    cql = f"""
    INSERT INTO healthinsight.patient_monitoring 
    (patient_id, reading_timestamp, metric_type, value, unit, device_id, is_abnormal, threshold_min, threshold_max)
    VALUES ({patient_uuid}, '{timestamp}', '{reading['metric_type']}', {reading['value']}, 
            '{reading['unit']}', '{reading['device_id']}', {str(reading['is_abnormal']).lower()}, 
            {reading['threshold_min']}, {reading['threshold_max']});
    """
    
    # Execute via docker exec cqlsh
    cmd = ['docker', 'exec', '-i', 'cassandra-1', 'cqlsh', '-e', cql]
    subprocess.run(cmd, capture_output=True)

def main():
    consumer = KafkaConsumer(
        'patient.monitoring.raw',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='cassandra-writer-simple'
    )
    
    print("✓ Connected to Kafka")
    print("Processing messages... (Press Ctrl+C to stop)")
    
    count = 0
    try:
        for message in consumer:
            reading = message.value
            insert_to_cassandra(reading)
            count += 1
            
            if count % 10 == 0:
                print(f"✓ Processed {count} readings", end='\r')
            
            if count >= 100:  # Process first 100 for testing
                break
                
    except KeyboardInterrupt:
        print(f"\n\n✓ Stopped by user")
    finally:
        print(f"\n✓ Total readings written: {count}")
        consumer.close()

if __name__ == "__main__":
    main()